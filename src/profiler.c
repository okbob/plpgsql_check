/*-------------------------------------------------------------------------
 *
 * profiler.c
 *
 *			  profiler accessories code
 *
 * by Pavel Stehule 2013-2020
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#if PG_VERSION_NUM < 110000

#include "storage/spin.h"

#endif

#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

/*
 * Any instance of plpgsql function will have a own profile.
 * When function will be dropped, then related profile should
 * be removed from shared memory.
 *
 * The local profile is created when function is initialized,
 * and it is stored in plugin_info field. When function is finished,
 * data from local profile is merged to shared profile.
 */
typedef struct profiler_hashkey
{
	Oid			fn_oid;
	Oid			db_oid;
	TransactionId fn_xmin;
	ItemPointerData fn_tid;
	int16		chunk_num;
} profiler_hashkey;

/*
 * Attention - the commands that can contains nestested commands
 * has attached own time and nested statements time too.
 */
typedef struct profiler_stmt
{
	int		lineno;
	pc_queryid	queryid;
	uint64	us_max;
	uint64	us_total;
	uint64	rows;
	uint64	exec_count;
	instr_time	start_time;
	instr_time	total;
} profiler_stmt;

typedef struct profiler_stmt_reduced
{
	int		lineno;
	pc_queryid	queryid;
	uint64	us_max;
	uint64	us_total;
	uint64	rows;
	uint64	exec_count;
} profiler_stmt_reduced;

#define		STATEMENTS_PER_CHUNK		30

/*
 * The shared profile will be stored as set of chunks
 */
typedef struct profiler_stmt_chunk
{
	profiler_hashkey key;
	slock_t	mutex;			/* only first chunk require mutex */
	profiler_stmt_reduced stmts[STATEMENTS_PER_CHUNK];
} profiler_stmt_chunk;

typedef struct profiler_shared_state
{
	LWLock	   *lock;
} profiler_shared_state;

/*
 * should be enough for project of 300K PLpgSQL rows.
 * It should to take about 18MB of shared memory.
 */
#define		MAX_SHARED_CHUNKS		15000

/*
 * It is used for fast mapping plpgsql stmt -> stmtid
 */

#if PG_VERSION_NUM < 120000

typedef struct profiler_map_entry
{
	PLpgSQL_stmt *stmt;
	int			stmtid;
	struct profiler_map_entry *next;
} profiler_map_entry;

/*
 * holds profile data (counters) and metadata (maps)
 */
typedef struct profiler_profile
{
	profiler_hashkey key;
	int			nstatements;
	PLpgSQL_stmt *entry_stmt;
	int			stmts_map_max_lineno;
	profiler_map_entry *stmts_map;
} profiler_profile;

#else

typedef struct profiler_profile
{
	profiler_hashkey key;
	int			nstatements;
	PLpgSQL_stmt *entry_stmt;
	int		   *stmts_map;
} profiler_profile;

#endif

#define PI_MAGIC 2020080110

/*
 * This structure is used as plpgsql extension parameter
 */
typedef struct profiler_info
{
	int			pi_magic;
	profiler_profile *profile;
	profiler_stmt *stmts;
	instr_time	start_time;

	/* tracer part */
	bool		trace_info_is_initialized;
	int			frame_num;
	int			level;
	PLpgSQL_execstate *near_outer_estate;

	bool		disable_tracer;

#if PG_VERSION_NUM >= 120000

	instr_time *stmt_start_times;
	int		   *stmt_group_numbers;
	int		   *stmt_parent_group_numbers;
	bool	   *stmt_disabled_tracers;
	bool	   *pragma_disable_tracer_stack;

#endif

} profiler_info;

typedef struct profiler_iterator
{
	profiler_hashkey	key;
	plpgsql_check_result_info *ri;
	HTAB	   *chunks;
	profiler_stmt_chunk *current_chunk;
	int					 current_statement;
} profiler_iterator;

enum
{
	COVERAGE_STATEMENTS,
	COVERAGE_BRANCHES
};

static HTAB *profiler_HashTable = NULL;
static HTAB *shared_profiler_chunks_HashTable = NULL;
static HTAB *profiler_chunks_HashTable = NULL;

static profiler_shared_state *profiler_ss = NULL;
static MemoryContext profiler_mcxt = NULL;

bool plpgsql_check_profiler = true;
bool plpgsql_check_profiler_dynamic_queryid = false;

PG_FUNCTION_INFO_V1(plpgsql_profiler_reset_all);
PG_FUNCTION_INFO_V1(plpgsql_profiler_reset);
PG_FUNCTION_INFO_V1(plpgsql_coverage_statements);
PG_FUNCTION_INFO_V1(plpgsql_coverage_branches);
PG_FUNCTION_INFO_V1(plpgsql_coverage_statements_name);
PG_FUNCTION_INFO_V1(plpgsql_coverage_branches_name);

static void profiler_touch_stmt(profiler_info *pinfo, PLpgSQL_stmt *stmt, PLpgSQL_stmt *parent_stmt, const char *parent_note, int block_num, bool generate_map, bool finalize_profile, int64 *nested_us_total, int64 *nested_executed, profiler_iterator *pi, coverage_state *cs);
static void update_persistent_profile(profiler_info *pinfo, PLpgSQL_function *func);
static void profiler_update_map(profiler_profile *profile, PLpgSQL_stmt *stmt);
static int profiler_get_stmtid(profiler_profile *profile, PLpgSQL_stmt *stmt);
static void profiler_touch_stmts(profiler_info *pinfo, List *stmts, PLpgSQL_stmt *parent_stmt, const char *parent_note, bool generate_map, bool finalize_profile, int64 *nested_us_total, int64 *nested_executed, profiler_iterator *pi, coverage_state *cs);
static PLpgSQL_expr *profiler_get_expr(PLpgSQL_stmt *stmt, bool *dynamic);
static pc_queryid profiler_get_queryid(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

/*
 * Itereate over Error Context Stack and calculate deep of stack (used like frame number)
 * and most near outer PLpgSQL estate (detect call statement). This function should be
 * called from func_beg, where error_context_stack is correctly initialized.
 */
void
plpgsql_check_init_trace_info(PLpgSQL_execstate *estate)
{
	ErrorContextCallback *econtext;
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

#if PG_VERSION_NUM >= 120000

	PLpgSQL_stmt_block *stmt_block = estate->func->action;
	int		tgn;

#endif

	Assert(pinfo && pinfo->pi_magic == PI_MAGIC);

	for (econtext = error_context_stack->previous;
		 econtext != NULL;
		 econtext = econtext->previous)
	{
		pinfo->frame_num += 1;

		/*
		 * We detect PLpgSQL related estate by known error callback function.
		 * This is inspirated by PLDebugger.
		 */
		if (econtext->callback == (*plpgsql_check_plugin_var_ptr)->error_callback)
		{
			PLpgSQL_execstate *outer_estate = (PLpgSQL_execstate *) econtext->arg;

			if (!pinfo->near_outer_estate)
				pinfo->near_outer_estate = outer_estate;

			if (pinfo->level == 0 && outer_estate->plugin_info)
			{
				profiler_info *outer_pinfo = (profiler_info *) outer_estate->plugin_info;

				if (outer_pinfo->pi_magic == PI_MAGIC &&
					outer_pinfo->trace_info_is_initialized)
				{

#if PG_VERSION_NUM >= 120000

					PLpgSQL_stmt *outer_stmt = outer_estate->err_stmt;

					if (outer_stmt)
					{
						int		ogn;

						ogn = outer_pinfo->stmt_group_numbers[outer_stmt->stmtid];
						pinfo->disable_tracer = outer_pinfo->pragma_disable_tracer_stack[ogn];
					}

#endif

					pinfo->level = outer_pinfo->level + 1;
					pinfo->frame_num += outer_pinfo->frame_num;

					break;
				}
			}
		}
	}

	if (plpgsql_check_runtime_pragma_vector_changed)
		pinfo->disable_tracer = plpgsql_check_runtime_pragma_vector.disable_tracer;

#if PG_VERSION_NUM >= 120000

	/* set top current group number */
	tgn = pinfo->stmt_group_numbers[stmt_block->stmtid];
	pinfo->pragma_disable_tracer_stack[tgn] = pinfo->disable_tracer;

#endif

}

#if PG_VERSION_NUM >= 120000

bool *
plpgsql_check_get_disable_tracer_on_stack(PLpgSQL_execstate *estate,
										  PLpgSQL_stmt *stmt)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	Assert(pinfo && pinfo->pi_magic == PI_MAGIC);

	/* Allow tracing only when it is explicitly allowed */
	if (!plpgsql_check_enable_tracer)
		return false;

	if (pinfo->trace_info_is_initialized)
		return &pinfo->pragma_disable_tracer_stack[stmt->stmtid];

	return NULL;
}

#endif

/*
 * Outer profiler's code fields of profiler info are not available.
 * This routine reads tracer fields from profiler info.
 */
bool
plpgsql_check_get_trace_info(PLpgSQL_execstate *estate,
							 PLpgSQL_stmt *stmt,
							 PLpgSQL_execstate **outer_estate,
							 int *frame_num,
							 int *level,
							 instr_time *start_time)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	Assert(pinfo && pinfo->pi_magic == PI_MAGIC);

	(void) stmt;

	/* Allow tracing only when it is explicitly allowed */
	if (!plpgsql_check_enable_tracer)
		return false;

	if (pinfo->trace_info_is_initialized)
	{

#if PG_VERSION_NUM >= 120000

		if (stmt && pinfo->stmt_disabled_tracers[stmt->stmtid])
			return false;

		if (!stmt && pinfo->disable_tracer)
			return false;

#endif

		*outer_estate = pinfo->near_outer_estate;
		*frame_num = pinfo->frame_num;
		*level = pinfo->level;
		*start_time = pinfo->start_time;

		return true;
	}
	else
		return false;
}

#if PG_VERSION_NUM >= 120000

/*
 * Outer profiler's code fields of profiler info are not available.
 * This routine reads tracer statement fields from profiler info.
 */
void
plpgsql_check_get_trace_stmt_info(PLpgSQL_execstate *estate,
								  int stmt_id,
								  instr_time **start_time)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	Assert(pinfo && pinfo->pi_magic == PI_MAGIC);
	Assert(stmt_id > 0);

	/* Allow tracing only when it is explicitly allowed */
	if (!plpgsql_check_enable_tracer)
		return;

	if (pinfo->trace_info_is_initialized)
		*start_time = &pinfo->stmt_start_times[stmt_id - 1];
	else
		*start_time = NULL;

}

#endif

static profiler_stmt_reduced *
get_stmt_profile_next(profiler_iterator *pi)
{

	if (pi->current_chunk)
	{
		if (pi->current_statement >= STATEMENTS_PER_CHUNK)
		{
			bool		found;

			pi->key.chunk_num += 1;

			pi->current_chunk = (profiler_stmt_chunk *) hash_search(pi->chunks,
														 (void *) &pi->key,
														 HASH_FIND,
														 &found);

			if (!found)
				elog(ERROR, "broken consistency of plpgsql_check profiler chunks");

			pi->current_statement = 0;
		}

		return &pi->current_chunk->stmts[pi->current_statement++];
	}

	return NULL;
}

/*
 * Calculate required size of shared memory for chunks
 *
 */
Size
plpgsql_check_shmem_size(void)
{
	Size		num_bytes = 0;

	num_bytes = MAXALIGN(sizeof(profiler_shared_state));
	num_bytes = add_size(num_bytes,
						 hash_estimate_size(MAX_SHARED_CHUNKS,
											sizeof(profiler_stmt_chunk)));

	return num_bytes;
}

/*
 * Initialize shared memory used like permanent profile storage.
 * No other parts use shared memory, so this code is completly here.
 *
 */
void
plpgsql_check_profiler_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;

	shared_profiler_chunks_HashTable = NULL;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	profiler_ss = ShmemInitStruct("plpgsql_check profiler state",
						   sizeof(profiler_shared_state),
						   &found);

	if (!found)
	{

#if PG_VERSION_NUM > 90600

		profiler_ss->lock = &(GetNamedLWLockTranche("plpgsql_check profiler"))->lock;

#else

		profiler_ss->lock = LWLockAssign();

#endif

	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(profiler_hashkey);
	info.entrysize = sizeof(profiler_stmt_chunk);

	shared_profiler_chunks_HashTable = ShmemInitHash("plpgsql_check profiler chunks",
													MAX_SHARED_CHUNKS,
													MAX_SHARED_CHUNKS,
													&info,
													HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}

/*
 * Profiler implementation
 */

static void
profiler_init_hashkey(profiler_hashkey *hk, PLpgSQL_function *func)
{
	memset(hk, 0, sizeof(profiler_hashkey));

	hk->db_oid = MyDatabaseId;
	hk->fn_oid = func->fn_oid;
	hk->fn_xmin = func->fn_xmin;
	hk->fn_tid = func->fn_tid;
	hk->chunk_num = 1;
}

/*
 * Hash table for function profiling metadata.
 */
static void
profiler_localHashTableInit(void)
{
	HASHCTL		ctl;

	Assert(profiler_HashTable == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(profiler_hashkey);
	ctl.entrysize = sizeof(profiler_profile);
	ctl.hcxt = profiler_mcxt;

	profiler_HashTable = hash_create("plpgsql_check function profiler local cache",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Hash table for local function profiles. When shared memory is not available
 * because plpgsql_check was not loaded by shared_proload_libraries, then function
 * profiles is stored in local profile chunks. A format is same for shared profiles.
 */
static void
profiler_chunks_HashTableInit(void)
{
	HASHCTL		ctl;

	Assert(profiler_chunks_HashTable == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(profiler_hashkey);
	ctl.entrysize = sizeof(profiler_stmt_chunk);
	ctl.hcxt = profiler_mcxt;
	profiler_chunks_HashTable = hash_create("plpgsql_check function profiler local chunks",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void
plpgsql_check_profiler_init_hash_tables(void)
{
	if (profiler_mcxt)
	{
		MemoryContextReset(profiler_mcxt);

		profiler_HashTable = NULL;
		profiler_chunks_HashTable = NULL;
	}
	else
	{
		profiler_mcxt = AllocSetContextCreate(TopMemoryContext,
												"plpgsql_check - profiler context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);
	}

	profiler_localHashTableInit();
	profiler_chunks_HashTableInit();
}

/*
 * Increase a branch couter - used for branch coverage
 *
 */
static void
increment_branch_counter(coverage_state *cs, int64 executed)
{
	if (cs)
	{
		cs->branches += 1;
		cs->executed_branches += executed > 0 ? 1 : 0;
	}
}

/*
 * profiler_touch_stmt - iterator over plpgsql statements.
 *
 * This function is designed for two different purposes:
 *
 *   a) assign unique id to every plpgsql statement and
 *      create statement -> id mapping
 *   b) iterate over all commends and finalize total time
 *      as measured total time substract child total time.
 *   c) iterate over all commands and prepare result for
 *      plpgsql_profiler_function_statements_tb function.
 *
 */
static void
profiler_touch_stmt(profiler_info *pinfo,
					PLpgSQL_stmt *stmt,
					PLpgSQL_stmt *parent_stmt,
					const char *parent_note,
					int block_num,
					bool generate_map,
					bool finalize_profile,
					int64 *nested_us_total,
					int64 *nested_executed,
					profiler_iterator *pi,
					coverage_state *cs)
{
	int64		us_total = 0;
	int64		_nested_executed = 0;
	profiler_profile *profile = pinfo->profile;
	profiler_stmt *pstmt = NULL;

	Assert(profile);

	if (pi)
	{
		int		stmtid = profiler_get_stmtid(profile, stmt);
		int		parent_stmtid = parent_stmt ? profiler_get_stmtid(profile, parent_stmt) : -1;
		profiler_stmt_reduced *pstmt;

		Assert(pi->current_statement == stmtid);

		pstmt = get_stmt_profile_next(pi);

		if (pi->ri)
			plpgsql_check_put_profile_statement(pi->ri,
												pstmt ? pstmt->queryid : NOQUERYID,
												stmtid,
												parent_stmtid,
												parent_note,
												block_num,
												stmt->lineno,
												pstmt ? pstmt->exec_count : 0,
												pstmt ? pstmt->us_total : 0.0,
												pstmt ? pstmt->us_max : 0.0,
												pstmt ? pstmt->rows : 0,
												(char *) plpgsql_check__stmt_typename_p(stmt));

		if (cs)
		{
			_nested_executed = pstmt ? pstmt->exec_count : 0;

			/* ignore invisible BLOCK */
			if (stmt->lineno != -1)
			{
				cs->statements += 1;
				cs->executed_statements += _nested_executed > 0 ? 1 : 0;
			}
		}

		if (nested_executed)
			*nested_executed = _nested_executed;

		parent_note = NULL;
	}
	else if (generate_map)
	{
		profiler_update_map(profile, stmt);
	}
	else if (finalize_profile)
	{
		int stmtid = profiler_get_stmtid(profile, stmt);

		*nested_us_total = 0;

		pstmt = &pinfo->stmts[stmtid];
		pstmt->lineno = stmt->lineno;
	}

	switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			{
				PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) stmt;

				profiler_touch_stmts(pinfo,
									 stmt_block->body,
									 stmt,
									 "body",
									 generate_map,
									 finalize_profile,
									 &us_total,
									 &_nested_executed,
									 pi,
									 cs);

				if (finalize_profile)
					*nested_us_total += us_total;

				if (stmt_block->exceptions)
				{
					ListCell *lc;
					char	buffer[100];
					int		n = 0;

					foreach(lc, stmt_block->exceptions->exc_list)
					{
						sprintf(buffer, "exception %d", ++n);

						profiler_touch_stmts(pinfo,
											 ((PLpgSQL_exception *) lfirst(lc))->action,
											 stmt,
											 (const char *) buffer,
											 generate_map,
											 finalize_profile,
											 &us_total,
											 &_nested_executed,
											 pi,
											 cs);

						if (finalize_profile)
							*nested_us_total += us_total;
					}
				}

				if (finalize_profile)
				{
					pstmt->us_total -= *nested_us_total;

					/*
					 * the max time can be calculated only when this node
					 * was executed once!
					 */
					if (pstmt->exec_count == 1)
						pstmt->us_max = pstmt->us_total;
					else
						pstmt->us_max = 0;

					*nested_us_total += pstmt->us_total;
				}
			}
			break;

		case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;
				int64	_nested_executed2;
				int64	_nested_executed2_total = 0;
				ListCell *lc;

				profiler_touch_stmts(pinfo,
									 stmt_if->then_body,
									 stmt,
									 "then body",
									 generate_map,
									 finalize_profile,
									 &us_total,
									 &_nested_executed2,
									 pi,
									 cs);

				increment_branch_counter(cs, _nested_executed2);
				_nested_executed2_total += _nested_executed2;

				if (finalize_profile)
					*nested_us_total += us_total;

				foreach(lc, stmt_if->elsif_list)
				{
					int		n = 0;
					char buffer[100];

					sprintf(buffer, "elsif %d", ++n);
					profiler_touch_stmts(pinfo,
										 ((PLpgSQL_if_elsif *) lfirst(lc))->stmts,
										 stmt,
										 (const char *) buffer,
										 generate_map,
										 finalize_profile,
										 &us_total,
										 &_nested_executed2,
										 pi,
										 cs);

					if (finalize_profile)
						*nested_us_total += us_total;

					increment_branch_counter(cs, _nested_executed2);
					_nested_executed2_total += _nested_executed2;
				}

				profiler_touch_stmts(pinfo,
									 stmt_if->else_body,
									 stmt,
									 "else body",
									 generate_map,
									 finalize_profile,
									 &us_total,
									 &_nested_executed2,
									 pi,
									 cs);

				if (finalize_profile)
					*nested_us_total += us_total;

				if (stmt_if->else_body)
				{
					increment_branch_counter(cs, _nested_executed2);
				}
				else
				{
					/*
					 * When we have not else branch, then we can increase branch counter,
					 */
					increment_branch_counter(cs, _nested_executed - _nested_executed2_total);
				}

				if (finalize_profile)
				{
					pstmt->us_total -= *nested_us_total;

					/*
					 * the max time can be calculated only when this node
					 * was executed once!
					 */
					if (pstmt->exec_count == 1)
						pstmt->us_max = pstmt->us_total;
					else
						pstmt->us_max = 0;

					*nested_us_total += pstmt->us_total;
				}
			}
			break;

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;
				ListCell *lc;

				foreach(lc, stmt_case->case_when_list)
				{
					char	buffer[100];
					int		n = 0;

					sprintf(buffer, "case when %d", ++n);
					profiler_touch_stmts(pinfo,
										 ((PLpgSQL_case_when *) lfirst(lc))->stmts,
										 stmt,
										 (const char *) buffer,
										 generate_map,
										 finalize_profile,
										 &us_total,
										 &_nested_executed,
										 pi,
										 cs);

					if (finalize_profile)
						*nested_us_total += us_total;

					increment_branch_counter(cs, _nested_executed);
				}

				profiler_touch_stmts(pinfo,
									 stmt_case->else_stmts,
									 stmt,
									 "case else",
									 generate_map,
									 finalize_profile,
									 &us_total,
									 &_nested_executed,
									 pi,
									 cs);

				if (finalize_profile)
					*nested_us_total += us_total;

				if (stmt_case->else_stmts)
					increment_branch_counter(cs, _nested_executed);

				if (finalize_profile)
				{
					pstmt->us_total -= *nested_us_total;

					/*
					 * the max time can be calculated only when this node
					 * was executed once!
					 */
					if (pstmt->exec_count == 1)
						pstmt->us_max = pstmt->us_total;
					else
						pstmt->us_max = 0;

					*nested_us_total += pstmt->us_total;
				}
			}
			break;

		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_FORS:
		case PLPGSQL_STMT_FORC:
		case PLPGSQL_STMT_DYNFORS:
		case PLPGSQL_STMT_FOREACH_A:
		case PLPGSQL_STMT_WHILE:
			{
				List   *stmts;

				switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
				{
					case PLPGSQL_STMT_WHILE:
						stmts = ((PLpgSQL_stmt_while *) stmt)->body;
						break;
					case PLPGSQL_STMT_LOOP:
						stmts = ((PLpgSQL_stmt_loop *) stmt)->body;
						break;
					case PLPGSQL_STMT_FORI:
						stmts = ((PLpgSQL_stmt_fori *) stmt)->body;
						break;
					case PLPGSQL_STMT_FORS:
						stmts = ((PLpgSQL_stmt_fors *) stmt)->body;
						break;
					case PLPGSQL_STMT_FORC:
						stmts = ((PLpgSQL_stmt_forc *) stmt)->body;
						break;
					case PLPGSQL_STMT_DYNFORS:
						stmts = ((PLpgSQL_stmt_dynfors *) stmt)->body;
						break;
					case PLPGSQL_STMT_FOREACH_A:
						stmts = ((PLpgSQL_stmt_foreach_a *) stmt)->body;
						break;
					default:
						stmts = NIL;
						break;
				}

				profiler_touch_stmts(pinfo,
								 stmts,
								 stmt,
								 "loop body",
								 generate_map,
								 finalize_profile,
								 &us_total,
								 &_nested_executed,
								 pi,
								 cs);

				increment_branch_counter(cs, _nested_executed);

				if (finalize_profile)
					*nested_us_total += us_total;

				if (finalize_profile)
				{
					pstmt->us_total -= *nested_us_total;

					/*
					 * the max time can be calculated only when this node
					 * was executed once!
					 */
					if (pstmt->exec_count == 1)
						pstmt->us_max = pstmt->us_total;
					else
						pstmt->us_max = 0;

					*nested_us_total += pstmt->us_total;
				}
			}
			break;

		default:
			if (finalize_profile)
				*nested_us_total = pstmt->us_total;
			break;
	}
}

/*
 * clean all chunks used by profiler
 */
Datum
plpgsql_profiler_reset_all(PG_FUNCTION_ARGS)
{
	/*be compiler quite */
	(void) fcinfo;

	if (shared_profiler_chunks_HashTable)
	{
		HASH_SEQ_STATUS			hash_seq;
		profiler_stmt_chunk    *chunk;

		LWLockAcquire(profiler_ss->lock, LW_EXCLUSIVE);

		hash_seq_init(&hash_seq, shared_profiler_chunks_HashTable);

		while ((chunk = hash_seq_search(&hash_seq)) != NULL)
		{
			hash_search(shared_profiler_chunks_HashTable, &(chunk->key), HASH_REMOVE, NULL);
		}

		LWLockRelease(profiler_ss->lock);
	}
	else
		plpgsql_check_profiler_init_hash_tables();

	PG_RETURN_VOID();
}

/*
 * Clean chunks related to some function
 */
Datum
plpgsql_profiler_reset(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	profiler_hashkey hk;
	HTAB	   *chunks;
	HeapTuple	procTuple;
	bool		found;
	bool		shared_chunks;

	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);

	/* ensure correct complete content of hash key */
	memset(&hk, 0, sizeof(profiler_hashkey));
	hk.fn_oid = funcoid;
	hk.db_oid = MyDatabaseId;
	hk.fn_xmin = HeapTupleHeaderGetRawXmin(procTuple->t_data);
	hk.fn_tid =  procTuple->t_self;
	hk.chunk_num = 1;

	ReleaseSysCache(procTuple);

	if (shared_profiler_chunks_HashTable)
	{
		LWLockAcquire(profiler_ss->lock, LW_EXCLUSIVE);
		chunks = shared_profiler_chunks_HashTable;
		shared_chunks = true;
	}
	else
	{
		chunks = profiler_chunks_HashTable;
		shared_chunks = false;
	}

	for(;;)
	{
		hash_search(chunks, (void *) &hk, HASH_REMOVE, &found);
		if (!found)
			break;
		hk.chunk_num += 1;
	}

	if (shared_chunks)
		LWLockRelease(profiler_ss->lock);

	PG_RETURN_VOID();
}

/*
 * Prepare environment for reading profile and calculation of coverage metric
 */
static double
coverage_internal(Oid fnoid, int coverage_type)
{
	plpgsql_check_info		cinfo;
	coverage_state			cs;

	memset(&cs, 0, sizeof(cs));

	plpgsql_check_info_init(&cinfo, fnoid);
	cinfo.show_profile = true;

	cinfo.proctuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(cinfo.fn_oid));
	if (!HeapTupleIsValid(cinfo.proctuple))
		elog(ERROR, "cache lookup failed for function %u", cinfo.fn_oid);

	plpgsql_check_get_function_info(cinfo.proctuple,
									&cinfo.rettype,
									&cinfo.volatility,
									&cinfo.trigtype,
									&cinfo.is_procedure);

	plpgsql_check_precheck_conditions(&cinfo);

	plpgsql_check_profiler_show_profile_statements(NULL, &cinfo, &cs);

	ReleaseSysCache(cinfo.proctuple);

	if (coverage_type == COVERAGE_STATEMENTS)
	{
		if (cs.statements > 0)
			return (double) cs.executed_statements / (double) cs.statements;
		else
			return (double) 1.0;
	}
	else
	{
		if (cs.branches > 0)
			return (double) cs.executed_branches / (double) cs.branches;
		else
			return (double) 1.0;
	}
}

Datum
plpgsql_coverage_statements_name(PG_FUNCTION_ARGS)
{
	Oid		fnoid;
	char   *name_or_signature;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	name_or_signature = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fnoid = plpgsql_check_parse_name_or_signature(name_or_signature);

	PG_RETURN_FLOAT8(coverage_internal(fnoid, COVERAGE_STATEMENTS));
}

Datum
plpgsql_coverage_branches_name(PG_FUNCTION_ARGS)
{
	Oid		fnoid;
	char   *name_or_signature;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	name_or_signature = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fnoid = plpgsql_check_parse_name_or_signature(name_or_signature);

	PG_RETURN_FLOAT8(coverage_internal(fnoid, COVERAGE_BRANCHES));
}

Datum
plpgsql_coverage_statements(PG_FUNCTION_ARGS)
{
	Oid		fnoid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	fnoid = PG_GETARG_OID(0);

	PG_RETURN_FLOAT8(coverage_internal(fnoid, COVERAGE_STATEMENTS));
}

Datum
plpgsql_coverage_branches(PG_FUNCTION_ARGS)
{
	Oid		fnoid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	fnoid = PG_GETARG_OID(0);

	PG_RETURN_FLOAT8(coverage_internal(fnoid, COVERAGE_BRANCHES));
}

static void
update_persistent_profile(profiler_info *pinfo, PLpgSQL_function *func)
{
	profiler_profile *profile = pinfo->profile;
	profiler_hashkey hk;
	profiler_stmt_chunk *chunk = NULL;
	bool		found;
	HTAB	   *chunks;
	bool		shared_chunks;
	volatile profiler_stmt_chunk *chunk_with_mutex = NULL;

	if (shared_profiler_chunks_HashTable)
	{
		chunks = shared_profiler_chunks_HashTable;
		LWLockAcquire(profiler_ss->lock, LW_SHARED);
		shared_chunks = true;
	}
	else
	{
		chunks = profiler_chunks_HashTable;
		shared_chunks = false;
	}

	profiler_init_hashkey(&hk, func) ;

	/* don't need too strong lock for shared memory */
	chunk = (profiler_stmt_chunk *) hash_search(chunks,
											 (void *) &hk,
											 HASH_FIND,
											 &found);

	/* We need exclusive lock */
	if (!found && shared_chunks)
	{
		LWLockRelease(profiler_ss->lock);
		LWLockAcquire(profiler_ss->lock, LW_EXCLUSIVE);

		/* repeat searching under exclusive lock */
		chunk = (profiler_stmt_chunk *) hash_search(chunks,
												 (void *) &hk,
												 HASH_FIND,
												 &found);
	}

	if (!found)
	{
		int		stmt_counter = 0;
		int		i;

		/* aftre increment first chunk will be created with chunk number 1 */
		hk.chunk_num = 0;

		/* we should to enter empty chunks first */
		for (i = 0; i < profile->nstatements; i++)
		{
			volatile profiler_stmt_reduced *prstmt;
			profiler_stmt *pstmt = &pinfo->stmts[i];

			if (hk.chunk_num == 0 || stmt_counter >= STATEMENTS_PER_CHUNK)
			{
				hk.chunk_num += 1;

				chunk = (profiler_stmt_chunk *) hash_search(chunks,
															 (void *) &hk,
															 HASH_ENTER,
															 &found);

				if (found)
					elog(ERROR, "broken consistency of plpgsql_check profiler chunks");

				if (hk.chunk_num == 1 && shared_chunks)
					SpinLockInit(&chunk->mutex);

				stmt_counter = 0;
			}

			prstmt = &chunk->stmts[stmt_counter++];

			prstmt->lineno = pstmt->lineno;
			prstmt->queryid = pstmt->queryid;
			prstmt->us_max = pstmt->us_max;
			prstmt->us_total = pstmt->us_total;
			prstmt->rows = pstmt->rows;
			prstmt->exec_count = pstmt->exec_count;
		}

		/* clean unused stmts in chunk */
		while (stmt_counter < STATEMENTS_PER_CHUNK)
			chunk->stmts[stmt_counter++].lineno = -1;

		if (shared_chunks)
			LWLockRelease(profiler_ss->lock);

		return;
	}

	/*
	 * Now we know, so there is already profile, and we have all necessary locks.
	 * Teoreticaly, we can reuse existing chunk, but inside PG_TRY block is better
	 * to take this value again to fix warning - "might be clobbered by 'longjmp"
	 */
	PG_TRY();
	{
		profiler_stmt_chunk *_chunk = NULL;
		HTAB   *_chunks;
		int		stmt_counter = 0;
		int		i = 0;

		_chunks = shared_chunks ? shared_profiler_chunks_HashTable : profiler_chunks_HashTable;
		profiler_init_hashkey(&hk, func) ;

		/* search chunk again */
		_chunk = (profiler_stmt_chunk *) hash_search(_chunks,
												    (void *) &hk,
													HASH_FIND,
													&found);

		if (shared_chunks)
		{
			chunk_with_mutex = _chunk;
			SpinLockAcquire(&chunk_with_mutex->mutex);
		}
		else
			chunk_with_mutex = NULL;

		hk.chunk_num = 1;
		stmt_counter = 0;

		/* there is a profiler chunk already */
		for (i = 0; i < profile->nstatements; i++)
		{
			profiler_stmt_reduced *prstmt;
			profiler_stmt *pstmt = &pinfo->stmts[i];

			if (stmt_counter >= STATEMENTS_PER_CHUNK)
			{
				hk.chunk_num += 1;

				_chunk = (profiler_stmt_chunk *) hash_search(_chunks,
															 (void *) &hk,
															 HASH_FIND,
															 &found);

				if (!found)
					elog(ERROR, "broken consistency of plpgsql_check profiler chunks");

				stmt_counter = 0;
			}

			prstmt = &_chunk->stmts[stmt_counter++];

			if (prstmt->lineno != pstmt->lineno)
				elog(ERROR, "broken consistency of plpgsql_check profiler chunks %d %d", prstmt->lineno, pstmt->lineno);

			if (prstmt->us_max < pstmt->us_max)
				prstmt->us_max = pstmt->us_max;

			prstmt->us_total += pstmt->us_total;
			prstmt->rows += pstmt->rows;
			prstmt->exec_count += pstmt->exec_count;
		}
	}
	PG_CATCH();
	{
		if (chunk_with_mutex)
			SpinLockRelease(&chunk_with_mutex->mutex);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (chunk_with_mutex)
		SpinLockRelease(&chunk_with_mutex->mutex);

	if (shared_chunks)
		LWLockRelease(profiler_ss->lock);
}

/*
 * PLpgSQL statements has not unique id. We can assign some unique id
 * that can be used for statements counters. Fast access to this id
 * is implemented via map structure. It is a array of lists structure.
 *
 * From PostgreSQL 12 we can use stmtid, but still we need map table,
 * because native stmtid has different order against lineno. But with
 * native stmtid, a creating map and searching in map is much faster.
 */
static void
profiler_update_map(profiler_profile *profile, PLpgSQL_stmt *stmt)
{
#if PG_VERSION_NUM < 120000

	int		lineno = stmt->lineno;
	profiler_map_entry *pme;

	if (lineno > profile->stmts_map_max_lineno)
	{
		int		lines;
		int		i;

		/* calculate new size of map */
		for (lines = profile->stmts_map_max_lineno; lineno > lines;)
			if (lines < 10000)
				lines *= 2;
			else
				lines += 10000;

		profile->stmts_map = repalloc(profile->stmts_map,
									 (lines + 1) * sizeof(profiler_map_entry));

		for (i = profile->stmts_map_max_lineno + 1; i <= lines; i++)
			memset(&profile->stmts_map[i], 0, sizeof(profiler_map_entry));

		profile->stmts_map_max_lineno = lines;
	}

	pme = &profile->stmts_map[lineno];

	if (!pme->stmt)
	{
		pme->stmt = stmt;
		pme->stmtid = profile->nstatements++;
	}
	else
	{
		profiler_map_entry *new_pme = palloc(sizeof(profiler_map_entry));

		new_pme->stmt = stmt;
		new_pme->stmtid = profile->nstatements++;
		new_pme->next = NULL;

		while (pme->next)
			pme = pme->next;

		pme->next = new_pme;
	}

#else

	profile->stmts_map[stmt->stmtid - 1] = profile->nstatements++;

#endif

}

/*
 * Returns statement id assigned to plpgsql statement. Should be
 * fast, because lineno is usually unique.
 */
static int
profiler_get_stmtid(profiler_profile *profile, PLpgSQL_stmt *stmt)
{
#if PG_VERSION_NUM < 120000

	int		lineno = stmt->lineno;
	profiler_map_entry *pme;

	if (lineno > profile->stmts_map_max_lineno)
		elog(ERROR, "broken statement map - too high lineno");

	pme = &profile->stmts_map[lineno];

	/* pme->stmt should not be null */
	if (!pme->stmt)
		elog(ERROR, "broken statement map - broken format on line: %d", lineno);

	while (pme && pme->stmt != stmt)
		pme = pme->next;

	/* we should to find statement */
	if (!pme)
		elog(ERROR, "broken statement map - cannot to find statement on line: %d", lineno);

	return pme->stmtid;

#else

	return profile->stmts_map[stmt->stmtid - 1];

#endif
}

static void
profiler_touch_stmts(profiler_info *pinfo,
					 List *stmts,
					 PLpgSQL_stmt *parent_stmt,
					 const char *parent_note,
					 bool generate_map,
					 bool finalize_profile,
					 int64 *nested_us_total,
					 int64 *nested_executed,
					 profiler_iterator *pi,
					 coverage_state *cs)
{
	ListCell   *lc;
	int			block_num = 1;
	int64		_nested_executed;
	bool		is_first = true;

	*nested_us_total = 0;
	block_num = 1;

	if (nested_executed)
		*nested_executed = false;

	foreach(lc, stmts)
	{
		int64		us_total = 0;

		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *) lfirst(lc);

		profiler_touch_stmt(pinfo,
							stmt,
							parent_stmt,
							parent_note,
							block_num++,
							generate_map,
							finalize_profile,
							&us_total,
							&_nested_executed,
							pi,
							cs);

		if (finalize_profile)
			*nested_us_total += us_total;

		/* take first */
		if (nested_executed && is_first)
		{
			*nested_executed = _nested_executed;
			is_first = false;
		}
	}
}

/*
 * Given a PLpgSQL_stmt, return the underlying PLpgSQL_expr that may contain a
 * queryidμ
 */
static PLpgSQL_expr  *
profiler_get_expr(PLpgSQL_stmt *stmt, bool *dynamic)
{
	PLpgSQL_expr *expr = NULL;

	*dynamic = false;

	switch(stmt->cmd_type)
	{
		case PLPGSQL_STMT_ASSIGN:
			expr = ((PLpgSQL_stmt_assign *) stmt)->expr;
			break;
		case PLPGSQL_STMT_PERFORM:
			expr = ((PLpgSQL_stmt_perform *) stmt)->expr;
			break;
#if PG_VERSION_NUM >= 110000
		case PLPGSQL_STMT_CALL:
			expr = ((PLpgSQL_stmt_call *) stmt)->expr;
			break;
		case PLPGSQL_STMT_SET:
			expr = ((PLpgSQL_stmt_set *) stmt)->expr;
			break;
#endif
		case PLPGSQL_STMT_IF:
			expr = ((PLpgSQL_stmt_if *) stmt)->cond;
			break;
		case 	PLPGSQL_STMT_CASE:
			expr = ((PLpgSQL_stmt_case *) stmt)->t_expr;
			break;
		case PLPGSQL_STMT_WHILE:
			expr = ((PLpgSQL_stmt_while *) stmt)->cond;
			break;
		case PLPGSQL_STMT_FORC:
			expr = ((PLpgSQL_stmt_forc *) stmt)->argquery;
			break;
		case PLPGSQL_STMT_DYNFORS:
			expr = ((PLpgSQL_stmt_dynfors *) stmt)->query;
			break;
		case PLPGSQL_STMT_FOREACH_A:
			expr = ((PLpgSQL_stmt_foreach_a *) stmt)->expr;
			break;
		case PLPGSQL_STMT_FETCH:
			expr = ((PLpgSQL_stmt_fetch *) stmt)->expr;
			break;
		case PLPGSQL_STMT_EXIT:
			expr = ((PLpgSQL_stmt_exit *) stmt)->cond;
			break;
		case PLPGSQL_STMT_RETURN:
			expr = ((PLpgSQL_stmt_return *) stmt)->expr;
			break;
		case PLPGSQL_STMT_RETURN_NEXT:
			expr = ((PLpgSQL_stmt_return_next *) stmt)->expr;
			break;
		case PLPGSQL_STMT_RETURN_QUERY:
			{
				PLpgSQL_stmt_return_query *q;

				q = (PLpgSQL_stmt_return_query *) stmt;
				if (q->query)
					expr = q->query;
				else
				{
					expr = q->dynquery;
					*dynamic = true;
				}
			}
			break;
		case PLPGSQL_STMT_ASSERT:
			expr = ((PLpgSQL_stmt_assert *) stmt)->cond;
			break;
		case PLPGSQL_STMT_EXECSQL:
			expr = ((PLpgSQL_stmt_execsql *) stmt)->sqlstmt;
			break;
		case PLPGSQL_STMT_DYNEXECUTE:
			expr = ((PLpgSQL_stmt_dynexecute *) stmt)->query;
			*dynamic = true;
			break;
		case PLPGSQL_STMT_OPEN:
			{
				PLpgSQL_stmt_open *o;

				o = (PLpgSQL_stmt_open *) stmt;
				if (o->query)
					expr = o->query;
				else if (o->dynquery)
				{
					expr = o->dynquery;
					*dynamic = true;
				}
				else
					expr = o->argquery;
			}
		case PLPGSQL_STMT_BLOCK:
#if PG_VERSION_NUM >= 100000
		case PLPGSQL_STMT_FORS:
#endif
#if PG_VERSION_NUM >= 110000
		case PLPGSQL_STMT_COMMIT:
		case PLPGSQL_STMT_ROLLBACK:
#endif
		case PLPGSQL_STMT_GETDIAG:
		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_RAISE:
		case PLPGSQL_STMT_CLOSE:
			break;
	}

	return expr;
}

static pc_queryid
profiler_get_dyn_queryid(PLpgSQL_execstate *estate, PLpgSQL_expr *expr)
{
	Query	   *query;
#if PG_VERSION_NUM >= 100000
	RawStmt    *parsetree;
#else
	Node	   *parsetree;
#endif
	bool		snapshot_set;
	List	   *parsetree_list;
	PLpgSQL_var result;
	PLpgSQL_type typ;
	char	   *query_string = NULL;

	memset(&result, 0, sizeof(result));
	memset(&typ, 0, sizeof(typ));

	result.dtype = PLPGSQL_DTYPE_VAR;
	result.refname = "*auxstorage*";
	result.datatype = &typ;

	typ.typoid = TEXTOID;
	typ.ttype = PLPGSQL_TTYPE_SCALAR;
	typ.typlen = -1;
	typ.typbyval = false;
	typ.typtype = 'b';

	(*plpgsql_check_plugin_var_ptr)->assign_expr(estate,
			(PLpgSQL_datum *) &result, expr);

	query_string = TextDatumGetCString(result.value);

	/*
	 * Do basic parsing of the query or queries (this should be safe even if
	 * we are in aborted transaction state!)
	 */
	parsetree_list = pg_parse_query(query_string);

	/*
	 * There should not be more than one query, silently ignore rather than
	 * error out in that case.
	 */
	if (list_length(parsetree_list) > 1)
		return NOQUERYID;

	/* Run through the raw parsetree and process it. */
#if PG_VERSION_NUM >= 100000
	parsetree = (RawStmt *) linitial(parsetree_list);
#else
	parsetree = (Node *) linitial(parsetree_list);
#endif
	snapshot_set = false;

	/*
	 * Set up a snapshot if parse analysis/planning will need one.
	 */
	if (analyze_requires_snapshot(parsetree))
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		snapshot_set = true;
	}

	query = parse_analyze(parsetree, query_string, NULL, 0
#if PG_VERSION_NUM >= 100000
			, NULL
#endif
			);

	if (snapshot_set)
		PopActiveSnapshot();

	return query->queryId;
}

/* Return the first queryid found in the given PLpgSQL_stmt, if any. */
static pc_queryid
profiler_get_queryid(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	bool dynamic;
	PLpgSQL_expr *expr = profiler_get_expr(stmt, &dynamic);

	if (dynamic)
	{
		Assert(expr);

		if (!plpgsql_check_profiler_dynamic_queryid)
			return NOQUERYID;

		return profiler_get_dyn_queryid(estate, expr);
	}

	if (expr)
	{
		SPIPlanPtr ptr  = expr->plan;
		List *sources = SPI_plan_get_plan_sources(ptr);

		if (sources)
		{
			CachedPlanSource *source;

			source = (CachedPlanSource *) linitial(sources);
			if (source->query_list)
			{
				Query *q = linitial_node(Query, source->query_list);

				return q->queryId;
			}
		}
	}

	return NOQUERYID;
}

/*
 * Prepare tuplestore with function profile
 *
 */
void
plpgsql_check_profiler_show_profile_statements(plpgsql_check_result_info *ri,
									plpgsql_check_info *cinfo,
									coverage_state *cs)
{
	PLpgSQL_function *function = NULL;

#if PG_VERSION_NUM >= 120000

	LOCAL_FCINFO(fake_fcinfo, 0);

#else

	FunctionCallInfoData fake_fcinfo_data;
	FunctionCallInfo fake_fcinfo = &fake_fcinfo_data;

#endif

	FmgrInfo	flinfo;
	TriggerData trigdata;
	EventTriggerData etrigdata;
	Trigger tg_trigger;
	ReturnSetInfo rsinfo;
	bool		fake_rtd;
	profiler_profile *profile;
	profiler_hashkey hk_function;
	profiler_info pinfo;
	profiler_stmt_chunk *first_chunk = NULL;
	profiler_iterator		pi;
	volatile bool		unlock_mutex = false;
	bool		found_profile = false;
	bool		shared_chunks;

	memset(&pi, 0, sizeof(profiler_iterator));
	pi.key.fn_oid = cinfo->fn_oid;
	pi.key.db_oid = MyDatabaseId;
	pi.key.fn_xmin = HeapTupleHeaderGetRawXmin(cinfo->proctuple->t_data);
	pi.key.fn_tid =  cinfo->proctuple->t_self;
	pi.key.chunk_num = 1;
	pi.ri = ri;

	/* try to find first chunk in shared (or local) memory */
	if (shared_profiler_chunks_HashTable)
	{
		LWLockAcquire(profiler_ss->lock, LW_SHARED);
		pi.chunks = shared_profiler_chunks_HashTable;
		shared_chunks = true;
	}
	else
	{
		pi.chunks = profiler_chunks_HashTable;
		shared_chunks = false;
	}

	pi.current_chunk = first_chunk = (profiler_stmt_chunk *) hash_search(pi.chunks,
																		 (void *) &pi.key,
																		 HASH_FIND,
																		 NULL);

	PG_TRY();
	{
		if (shared_chunks && first_chunk)
		{
			SpinLockAcquire(&first_chunk->mutex);
			unlock_mutex = true;
		}

		plpgsql_check_setup_fcinfo(cinfo,
								   &flinfo,
								   fake_fcinfo,
								   &rsinfo,
								   &trigdata,
								   &etrigdata,
								   &tg_trigger,
								   &fake_rtd);

		/* Get a compiled function */
		function = plpgsql_check__compile_p(fake_fcinfo, false);

		profiler_init_hashkey(&hk_function, function);
		profile = (profiler_profile *) hash_search(profiler_HashTable,
												 (void *) &hk_function,
												 HASH_ENTER,
												 &found_profile);

		pinfo.profile = profile;

		if (!found_profile)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(profiler_mcxt);

			profile->nstatements = 0;

#if PG_VERSION_NUM < 120000

			profile->stmts_map_max_lineno = 200;

			profile->stmts_map = palloc0((profile->stmts_map_max_lineno + 1) * sizeof(profiler_map_entry));

#else

			profile->stmts_map = palloc0(function->nstatements * sizeof(int));

#endif

			profile->entry_stmt = (PLpgSQL_stmt *) function->action;
			profiler_touch_stmt(&pinfo, (PLpgSQL_stmt *) function->action, NULL, NULL, 1, true, false, NULL, NULL, NULL, NULL);

			MemoryContextSwitchTo(oldcxt);
		}

		profiler_touch_stmt(&pinfo, (PLpgSQL_stmt *) function->action, NULL, NULL, 1, false, false, NULL, NULL, &pi, cs);

	}
	PG_CATCH();
	{
		if (unlock_mutex)
			SpinLockRelease(&first_chunk->mutex);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (unlock_mutex)
		SpinLockRelease(&first_chunk->mutex);

	if (shared_chunks)
		LWLockRelease(profiler_ss->lock);
}

/*
 * Prepare tuplestore with function profile
 *
 */
void
plpgsql_check_profiler_show_profile(plpgsql_check_result_info *ri,
									plpgsql_check_info *cinfo)
{
	profiler_hashkey hk;
	bool found;
	HTAB	   *chunks;
	bool		shared_chunks;
	volatile profiler_stmt_chunk *first_chunk = NULL;
	volatile bool		unlock_mutex = false;

	/* ensure correct complete content of hash key */
	memset(&hk, 0, sizeof(profiler_hashkey));
	hk.fn_oid = cinfo->fn_oid;
	hk.db_oid = MyDatabaseId;
	hk.fn_xmin = HeapTupleHeaderGetRawXmin(cinfo->proctuple->t_data);
	hk.fn_tid =  cinfo->proctuple->t_self;
	hk.chunk_num = 1;

	/* try to find first chunk in shared (or local) memory */
	if (shared_profiler_chunks_HashTable)
	{
		LWLockAcquire(profiler_ss->lock, LW_SHARED);
		chunks = shared_profiler_chunks_HashTable;
		shared_chunks = true;
	}
	else
	{
		chunks = profiler_chunks_HashTable;
		shared_chunks = false;
	}

	PG_TRY();
	{
		char	   *prosrc = cinfo->src;
		profiler_stmt_chunk *chunk = NULL;
		int			lineno = 1;
		int			current_statement = 0;

		chunk = (profiler_stmt_chunk *) hash_search(chunks,
											 (void *) &hk,
											 HASH_FIND,
											 &found);

		if (shared_chunks && chunk)
		{
			first_chunk = chunk;
			SpinLockAcquire(&first_chunk->mutex);
			unlock_mutex = true;
		}

		/* iterate over source code rows */
		while (*prosrc)
		{
			char	   *lineend = NULL;
			char	   *linebeg = NULL;

			int			stmt_lineno = -1;
			int64		us_total = 0;
			int64		exec_count = 0;
			Datum		queryids_array = (Datum) 0;
			Datum		max_time_array = (Datum) 0;
			Datum		processed_rows_array = (Datum) 0;
			int			cmds_on_row = 0;

			lineend = prosrc;
			linebeg = prosrc;

			/* find lineend */
			while (*lineend != '\0' && *lineend != '\n')
				lineend += 1;

			if (*lineend == '\n')
			{
				*lineend = '\0';
				prosrc = lineend + 1;
			}
			else
				prosrc = lineend;

			if (chunk)
			{
				ArrayBuildState *queryids_abs = NULL;
				ArrayBuildState *max_time_abs = NULL;
				ArrayBuildState *processed_rows_abs = NULL;

#if PG_VERSION_NUM >= 90500

				queryids_abs = initArrayResult(INT8OID, CurrentMemoryContext, true);
				max_time_abs = initArrayResult(FLOAT8OID, CurrentMemoryContext, true);
				processed_rows_abs = initArrayResult(INT8OID, CurrentMemoryContext, true);

#endif

				/* process all statements on this line */
				for(;;)
				{
					/* ensure so  access to chunks is correct */
					if (current_statement >= STATEMENTS_PER_CHUNK)
					{
						hk.chunk_num += 1;

						chunk = (profiler_stmt_chunk *) hash_search(chunks,
														 (void *) &hk,
														 HASH_FIND,
														 &found);

						if (!found)
						{
							chunk = NULL;
							break;
						}

						current_statement = 0;
					}

					Assert(chunk != NULL);

					/* skip invisible statements if any */
					if (chunk->stmts[current_statement].lineno < lineno)
					{
						current_statement += 1;
						continue;
					}
					else if (chunk->stmts[current_statement].lineno == lineno)
					{
						profiler_stmt_reduced *prstmt = &chunk->stmts[current_statement];

						us_total += prstmt->us_total;
						exec_count += prstmt->exec_count;

						stmt_lineno = lineno;

						queryids_abs = accumArrayResult(queryids_abs,
														Int64GetDatum((int64) prstmt->queryid),
														prstmt->queryid == NOQUERYID,
														INT8OID,
														CurrentMemoryContext);

						max_time_abs = accumArrayResult(max_time_abs,
														Float8GetDatum(prstmt->us_max / 1000.0), false,
														FLOAT8OID,
														CurrentMemoryContext);

						processed_rows_abs = accumArrayResult(processed_rows_abs,
															 Int64GetDatum(prstmt->rows), false,
															 INT8OID,
															 CurrentMemoryContext);
						cmds_on_row += 1;


						current_statement += 1;
						continue;
					}
					else
						break;
				}

				if (cmds_on_row > 0)
				{
					queryids_array = makeArrayResult(queryids_abs, CurrentMemoryContext);
					max_time_array = makeArrayResult(max_time_abs, CurrentMemoryContext);
					processed_rows_array = makeArrayResult(processed_rows_abs, CurrentMemoryContext);
				}
			}

			plpgsql_check_put_profile(ri,
								   queryids_array,
								   lineno,
								   stmt_lineno,
								   cmds_on_row,
								   exec_count,
								   us_total,
								   max_time_array,
								   processed_rows_array,
								   (char *) linebeg);

			lineno += 1;
		}
	}
	PG_CATCH();
	{
		if (unlock_mutex)
			SpinLockRelease(&first_chunk->mutex);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (unlock_mutex)
		SpinLockRelease(&first_chunk->mutex);

	if (shared_chunks)
		LWLockRelease(profiler_ss->lock);
}

/*
 * plpgsql plugin related functions
 */

/*
 * Try to search profile pattern for function. Creates profile pattern when
 * it doesn't exists.
 */
void
plpgsql_check_profiler_func_init(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	profiler_info *pinfo;

	if (plpgsql_check_tracer)
	{

#if PG_VERSION_NUM >= 120000

		int		group_number_counter = 0;

#endif

		pinfo = palloc0(sizeof(profiler_info));
		pinfo->pi_magic = PI_MAGIC;

		INSTR_TIME_SET_CURRENT(pinfo->start_time);
		pinfo->trace_info_is_initialized = true;

#if PG_VERSION_NUM >= 120000

		pinfo->stmt_start_times = palloc0(sizeof(instr_time) * func->nstatements);
		pinfo->stmt_group_numbers = palloc(sizeof(int) * (func->nstatements + 1));
		pinfo->stmt_parent_group_numbers = palloc(sizeof(int) * (func->nstatements + 1));
		pinfo->stmt_disabled_tracers = palloc0(sizeof(int) * (func->nstatements + 1));

		plpgsql_check_set_stmt_group_number((PLpgSQL_stmt *) func->action,
											pinfo->stmt_group_numbers,
											pinfo->stmt_parent_group_numbers,
											0,
											&group_number_counter,
											-1);

		pinfo->pragma_disable_tracer_stack = palloc(sizeof(bool) * (group_number_counter + 1));

		/* now we have not access to outer estate */
		pinfo->disable_tracer = false;

		plpgsql_check_runtime_pragma_vector_changed = false;

#endif

		estate->plugin_info = pinfo;
	}

	if (plpgsql_check_profiler && func->fn_oid != InvalidOid)
	{
		profiler_info *pinfo;
		profiler_profile *profile;
		profiler_hashkey hk;
		bool		found;

		profiler_init_hashkey(&hk, func);
		profile = (profiler_profile *) hash_search(profiler_HashTable,
											 (void *) &hk,
											 HASH_ENTER,
											 &found);

		pinfo = estate->plugin_info;
		if (!pinfo)
		{
			pinfo = palloc0(sizeof(profiler_info));
			pinfo->pi_magic = PI_MAGIC;

			INSTR_TIME_SET_CURRENT(pinfo->start_time);

			estate->plugin_info = pinfo;
		}

		pinfo->profile = profile;

		if (!found)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(profiler_mcxt);

			profile->nstatements = 0;

#if PG_VERSION_NUM < 120000

			profile->stmts_map_max_lineno = 200;

			profile->stmts_map = palloc0((profile->stmts_map_max_lineno + 1) * sizeof(profiler_map_entry));

#else

			profile->stmts_map = palloc0(func->nstatements * sizeof(int));

#endif

			profile->entry_stmt = (PLpgSQL_stmt *) func->action;
			profiler_touch_stmt(pinfo, (PLpgSQL_stmt *) func->action, NULL, NULL, 1, true, false, NULL, NULL, NULL, NULL);

			/* entry statements is not visible for plugin functions */

			MemoryContextSwitchTo(oldcxt);
		}

		pinfo->stmts = palloc0(profile->nstatements * sizeof(profiler_stmt));

		INSTR_TIME_SET_CURRENT(pinfo->start_time);

		estate->plugin_info = pinfo;
	}
}

void
plpgsql_check_profiler_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	profiler_info *pinfo = estate->plugin_info;

	if (plpgsql_check_tracer && pinfo )
	{
		plpgsql_check_tracer_on_func_end(estate, func);

#if PG_VERSION_NUM >= 120000

		pfree(pinfo->stmt_start_times);
		pfree(pinfo->stmt_group_numbers);
		pfree(pinfo->stmt_parent_group_numbers);
		pfree(pinfo->stmt_disabled_tracers);
		pfree(pinfo->pragma_disable_tracer_stack);

#endif

	}

	if (plpgsql_check_profiler &&
		pinfo && pinfo->profile &&
		func->fn_oid != InvalidOid)
	{
		profiler_info *pinfo = (profiler_info *) estate->plugin_info;
		profiler_profile *profile = pinfo->profile;
		int		entry_stmtid = profiler_get_stmtid(profile, profile->entry_stmt);
		int64			nested_us_total;
		instr_time		end_time;
		uint64			elapsed;

		INSTR_TIME_SET_CURRENT(end_time);
		INSTR_TIME_SUBTRACT(end_time, pinfo->start_time);

		elapsed = INSTR_TIME_GET_MICROSEC(end_time);

		if (pinfo->stmts[entry_stmtid].exec_count == 0)
		{
			pinfo->stmts[entry_stmtid].exec_count = 1;
			pinfo->stmts[entry_stmtid].us_total = elapsed;
			pinfo->stmts[entry_stmtid].us_max = elapsed;
		}

		/* finalize profile - get result profile */
		profiler_touch_stmt(pinfo,
						   profile->entry_stmt,
						   NULL,
						   NULL,
						   1,
						   false,
						   true,
						   &nested_us_total,
						   NULL,
						   NULL,
						   NULL);

		update_persistent_profile(pinfo, func);

		pfree(pinfo->stmts);
	}

	if ((plpgsql_check_tracer || plpgsql_check_profiler) && pinfo)
		pfree(pinfo);
}

void
plpgsql_check_profiler_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	if (plpgsql_check_tracer && pinfo)
	{

#if PG_VERSION_NUM >= 120000

		int		stmtid = stmt->stmtid;
		int		sgn = pinfo->stmt_group_numbers[stmtid];
		int		pgn = pinfo->stmt_parent_group_numbers[stmtid];

		plpgsql_check_runtime_pragma_vector_changed = false;

		/*
		 * First statement in group has valid parent group number.
		 * We use this number for copy setting from outer group
		 * to nested group.
		 */
		if (pgn != -1)
		{
			pinfo->pragma_disable_tracer_stack[sgn] =
				pinfo->pragma_disable_tracer_stack[pgn];
		}

		pinfo->stmt_disabled_tracers[stmtid] =
				pinfo->pragma_disable_tracer_stack[sgn];

#endif

		plpgsql_check_tracer_on_stmt_beg(estate, stmt);
	}

	if (stmt->cmd_type == PLPGSQL_STMT_ASSERT &&
			plpgsql_check_enable_tracer &&
			plpgsql_check_trace_assert)
		plpgsql_check_trace_assert_on_stmt_beg(estate, stmt);

	if (plpgsql_check_profiler &&
		pinfo && pinfo->profile &&
		estate->func->fn_oid != InvalidOid)
	{
		profiler_profile *profile = pinfo->profile;
		int stmtid = profiler_get_stmtid(profile, stmt);
		profiler_stmt *pstmt = &pinfo->stmts[stmtid];

		Assert(pinfo->pi_magic == PI_MAGIC);

		INSTR_TIME_SET_CURRENT(pstmt->start_time);
	}
}

void
plpgsql_check_profiler_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	if (plpgsql_check_tracer && pinfo)
	{

#if PG_VERSION_NUM >= 120000

		int		stmtid = stmt->stmtid;

		if (plpgsql_check_runtime_pragma_vector_changed)
		{
			int		sgn;

			sgn = pinfo->stmt_group_numbers[stmtid];

			pinfo->pragma_disable_tracer_stack[sgn] =
				plpgsql_check_runtime_pragma_vector.disable_tracer;
		}

#endif

		plpgsql_check_tracer_on_stmt_end(estate, stmt);
	}

	if (plpgsql_check_profiler && 
		pinfo && pinfo->profile &&
		estate->func->fn_oid != InvalidOid)
	{
		profiler_profile *profile  = pinfo->profile;
		int stmtid = profiler_get_stmtid(profile, stmt);
		profiler_stmt *pstmt = &pinfo->stmts[stmtid];
		instr_time		end_time;
		uint64			elapsed;
		instr_time		end_time2;

		Assert(pinfo->pi_magic == PI_MAGIC);

		if (pstmt->queryid == NOQUERYID)
			pstmt->queryid = profiler_get_queryid(estate, stmt);

		INSTR_TIME_SET_CURRENT(end_time);
		end_time2 = end_time;
		INSTR_TIME_ACCUM_DIFF(pstmt->total, end_time, pstmt->start_time);

		INSTR_TIME_SUBTRACT(end_time2, pstmt->start_time);
		elapsed = INSTR_TIME_GET_MICROSEC(end_time2);

		if (elapsed > pstmt->us_max)
			pstmt->us_max = elapsed;

		pstmt->us_total = INSTR_TIME_GET_MICROSEC(pstmt->total);
		pstmt->rows += estate->eval_processed;
		pstmt->exec_count++;
	}
}
