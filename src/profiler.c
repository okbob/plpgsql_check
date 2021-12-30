/*-------------------------------------------------------------------------
 *
 * profiler.c
 *
 *			  profiler accessories code
 *
 * by Pavel Stehule 2013-2021
 *
 *-------------------------------------------------------------------------
 */
#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
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

#if PG_VERSION_NUM >= 120000

#include "utils/float.h"

#endif

#include <math.h>

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
 * We want to collect data about execution on function level. The
 * possible issue can be more different profiles. On this level
 * the version of function is not important, so the hash key is
 * composed only from fn_oid and db_oid
 */
typedef struct fstats_hashkey
{
	Oid			fn_oid;
	Oid			db_oid;
} fstats_hashkey;

typedef struct fstats
{
	fstats_hashkey key;
	slock_t		mutex;
	uint64		exec_count;
	uint64		exec_count_err;
	uint64		total_time;
	double		total_time_xx;
	uint64		min_time;
	uint64		max_time;
} fstats;

/*
 * This is used as cache for types of expressions of USING clause
 * (EXECUTE like statements).
 */
typedef struct
{
	int			nparams;
	Oid			paramtypes[FLEXIBLE_ARRAY_MEMBER];
} query_params;

/*
 * Attention - the commands that can contains nestested commands
 * has attached own time and nested statements time too.
 */
typedef struct profiler_stmt
{
	int		lineno;
	pc_queryid	queryid;
	uint64		us_max;
	uint64		us_total;
	uint64		rows;
	uint64		exec_count;
	uint64		exec_count_err;
	instr_time	start_time;
	instr_time	total;
	bool		has_queryid;
	query_params *qparams;
} profiler_stmt;

typedef struct profiler_stmt_reduced
{
	int		lineno;
	pc_queryid	queryid;
	uint64		us_max;
	uint64		us_total;
	uint64		rows;
	uint64		exec_count;
	uint64		exec_count_err;
	bool		has_queryid;
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
	LWLock	   *fstats_lock;
} profiler_shared_state;

/*
 * It is used for fast mapping plpgsql stmt -> stmtid
 */

#if PG_VERSION_NUM < 120000

typedef struct profiler_map_entry
{
	PLpgSQL_function *function;
	PLpgSQL_stmt *stmt;
	int			stmtid;
	struct profiler_map_entry *next;
} profiler_map_entry;

/*
 * holds profiland metadata (maps)
 */
typedef struct profiler_profile
{
	profiler_hashkey key;
	int			nstatements;
	PLpgSQL_function **mapped_functions;
	int			max_mapped_functions;
	int			n_mapped_functions;
	int			stmts_map_max_lineno;
	profiler_map_entry *stmts_map;
} profiler_profile;

#else

/*
 * In current releases we know unique statement id, and we don't
 * need statement map. Unfortunately this id (stmtid) is not in
 * order of statement execution. The order depends on order of
 * processing inside parser. So some deeper statememts has less
 * number than less deeper statements. When we generate result,
 * then we need to work with statements in natural order. This
 * mapping is provided by stmtid_reorder_map.
 */
typedef struct profiler_profile
{
	profiler_hashkey key;
	int	   *stmtid_reorder_map;
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
	PLpgSQL_function *func;
	instr_time	start_time;

	/* tracer part */
	bool		trace_info_is_initialized;
	int			frame_num;
	int			level;
	PLpgSQL_execstate *near_outer_estate;
	PLpgSQL_execstate *estate;

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

typedef struct
{
	int		stmtid;
	int64 nested_us_time;
	int64 nested_exec_count;
	profiler_iterator *pi;
	coverage_state *cs;
} profiler_stmt_walker_options;

enum
{
	COVERAGE_STATEMENTS,
	COVERAGE_BRANCHES
};

typedef struct fmgr_hook_private
{
	bool		use_plpgsql;
	Datum		next_private;
} fmgr_hook_private;

#define		NESTED_STMTS_STACK_SIZE		64

typedef struct profiler_stack
{
	profiler_info *pinfo;
	struct profiler_stack *prev_pinfo;
	PLpgSQL_stmt *nested_stmts[NESTED_STMTS_STACK_SIZE];

	/*
	 * The commands executed under same protected block shares
	 * eval_context. The eval_context is created for every
	 * protected block, but exception handler uses parent
	 * eval_context. The change of eval_context can signalize
	 * an entry/leave to protected section. The nonempty
	 * current error can signalize so we are inside an exception
	 * handler.
	 */
	ExprContext *eval_econtext[NESTED_STMTS_STACK_SIZE];
	int			nested_stmts_count;
} profiler_stack;

static HTAB *profiler_HashTable = NULL;
static HTAB *shared_profiler_chunks_HashTable = NULL;
static HTAB *profiler_chunks_HashTable = NULL;
static HTAB *fstats_HashTable = NULL;
static HTAB *shared_fstats_HashTable = NULL;

static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

static profiler_shared_state *profiler_ss = NULL;
static MemoryContext profiler_mcxt = NULL;
static MemoryContext profiler_queryid_mcxt = NULL;

static profiler_stack *top_pinfo = NULL;
static ExprContext *curr_eval_econtext = NULL;

bool plpgsql_check_profiler = true;

needs_fmgr_hook_type	plpgsql_check_next_needs_fmgr_hook = NULL;
fmgr_hook_type			plpgsql_check_next_fmgr_hook = NULL;

/*
 * should be enough for project of 300K PLpgSQL rows.
 * It should to take about 24.4MB of shared memory.
 */
int plpgsql_check_profiler_max_shared_chunks = 15000;

PG_FUNCTION_INFO_V1(plpgsql_profiler_reset_all);
PG_FUNCTION_INFO_V1(plpgsql_profiler_reset);
PG_FUNCTION_INFO_V1(plpgsql_coverage_statements);
PG_FUNCTION_INFO_V1(plpgsql_coverage_branches);
PG_FUNCTION_INFO_V1(plpgsql_coverage_statements_name);
PG_FUNCTION_INFO_V1(plpgsql_coverage_branches_name);
PG_FUNCTION_INFO_V1(plpgsql_profiler_install_fake_queryid_hook);
PG_FUNCTION_INFO_V1(plpgsql_profiler_remove_fake_queryid_hook);

static void update_persistent_profile(profiler_info *pinfo, PLpgSQL_function *func);
static PLpgSQL_expr *profiler_get_expr(PLpgSQL_stmt *stmt, bool *dynamic, List **params);
static pc_queryid profiler_get_queryid(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, bool *has_queryid, query_params **qparams);

#if PG_VERSION_NUM >= 140000

static void profiler_fake_queryid_hook(ParseState *pstate, Query *query, JumbleState *jstate);

#else

static void profiler_fake_queryid_hook(ParseState *pstate, Query *query);

#endif

static void profile_register_stmt(profiler_info *pinfo, profiler_stmt_walker_options *opts, PLpgSQL_stmt *stmt);
static void stmts_walker(profiler_info *pinfo, profiler_stmt_walker_mode, List *stmts, PLpgSQL_stmt *parent_stmt,
	 const char *description, profiler_stmt_walker_options *opts);
static void profiler_stmt_walker(profiler_info *pinfo, profiler_stmt_walker_mode mode, PLpgSQL_stmt *stmt,
	 PLpgSQL_stmt *parent_stmt, const char *description, int stmt_block_num, profiler_stmt_walker_options *opts);

#if PG_VERSION_NUM < 120000

static void profiler_update_map(profiler_profile *profile, profiler_stmt_walker_options *opts,
	 PLpgSQL_function *function, PLpgSQL_stmt *stmt);
static int profiler_get_stmtid(profiler_profile *profile, PLpgSQL_function *function, PLpgSQL_stmt *stmt);

#endif


/*
 * Use the Youngs-Cramer algorithm to incorporate the new value into the
 * transition values.
 */
static void
eval_stddev_accum(uint64 *_N, uint64 *_Sx, float8 *_Sxx, uint64 newval)
{
	uint64		N = *_N;
	uint64		Sx = *_Sx;
	float8		Sxx = *_Sxx;
	float8		tmp;

	/*
	 * Use the Youngs-Cramer algorithm to incorporate the new value into the
	 * transition values.
	 */
	N += 1;
	Sx += newval;

	if (N > 1)
	{
		tmp = ((float8 ) newval) * ((float8) N) - ((float8) Sx);

		Sxx += tmp * tmp / (N * (N - 1));

		if (isinf(Sxx))
			Sxx = get_float8_nan();
	}
	else
		Sxx = 0.0;

	*_N = N;
	*_Sx = Sx;
	*_Sxx = Sxx;
}

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

						ogn = outer_pinfo->stmt_group_numbers[outer_stmt->stmtid - 1];
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
	tgn = pinfo->stmt_group_numbers[stmt_block->stmtid - 1];
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
		return &pinfo->pragma_disable_tracer_stack[stmt->stmtid - 1];

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

		if (stmt && pinfo->stmt_disabled_tracers[stmt->stmtid - 1])
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

	/* Allow tracing only when it is explicitly allowed */
	if (!plpgsql_check_enable_tracer)
		return;

	if (pinfo->trace_info_is_initialized)
		*start_time = &pinfo->stmt_start_times[stmt_id];
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
						 hash_estimate_size(plpgsql_check_profiler_max_shared_chunks,
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
	shared_fstats_HashTable = NULL;

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
		profiler_ss->lock = &(GetNamedLWLockTranche("plpgsql_check profiler"))->lock;
		profiler_ss->fstats_lock = &(GetNamedLWLockTranche("plpgsql_check fstats"))->lock;
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(profiler_hashkey);
	info.entrysize = sizeof(profiler_stmt_chunk);

	shared_profiler_chunks_HashTable = ShmemInitHash("plpgsql_check profiler chunks",
													plpgsql_check_profiler_max_shared_chunks,
													plpgsql_check_profiler_max_shared_chunks,
													&info,
													HASH_ELEM | HASH_BLOBS);

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(fstats_hashkey);
	info.entrysize = sizeof(fstats);

	shared_fstats_HashTable = ShmemInitHash("plpgsql_check fstats",
													500,
													1000,
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

static void
fstats_init_hashkey(fstats_hashkey *fhk, Oid fn_oid)
{
	memset(fhk, 0, sizeof(fstats_hashkey));

	fhk->db_oid = MyDatabaseId;
	fhk->fn_oid = fn_oid;
}

static void
fstats_HashTableInit(void)
{
	HASHCTL		ctl;

	Assert(fstats_HashTable == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(fstats_hashkey);
	ctl.entrysize = sizeof(fstats);
	ctl.hcxt = profiler_mcxt;
	fstats_HashTable = hash_create("plpgsql_check function execution statistics",
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
		fstats_HashTable = NULL;
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
	fstats_HashTableInit();
}

/*
 * Increase a branch couter - used for branch coverage
 */
static void
increment_branch_counter(coverage_state *cs, int64 executed)
{
	Assert(cs);

	cs->branches += 1;
	cs->executed_branches += executed > 0 ? 1 : 0;
}

/*
 * Aux routine to reduce differencies between pg releases with and without
 * statemet id support.
 */
#if PG_VERSION_NUM < 120000

#define STMTID(stmt)		(profiler_get_stmtid(pinfo->profile, pinfo->func, ((PLpgSQL_stmt *) stmt)))
#define FUNC_NSTATEMENTS(pinfo)	(pinfo->profile->nstatements)
#define NATURAL_STMTID(pinfo, id) (id)

#else

#define STMTID(stmt)		(((PLpgSQL_stmt *) stmt)->stmtid - 1)
#define FUNC_NSTATEMENTS(pinfo) ((int) pinfo->func->nstatements)

static int
get_natural_stmtid(profiler_info *pinfo, int id)
{
	int		i;

	for (i = 0; i < FUNC_NSTATEMENTS(pinfo); i++)
		if (pinfo->profile->stmtid_reorder_map[i] == id)
			return i;

	return -1;
}

#define NATURAL_STMTID(pinfo, id)		get_natural_stmtid(pinfo, id)

#endif

#define IS_PLPGSQL_STMT(stmt, typ)		(PLPGSQL_STMT_TYPES stmt->cmd_type == typ)

static bool
is_cycle(PLpgSQL_stmt *stmt)
{
	switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
	{
		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_FORS:
		case PLPGSQL_STMT_FORC:
		case PLPGSQL_STMT_DYNFORS:
		case PLPGSQL_STMT_FOREACH_A:
		case PLPGSQL_STMT_WHILE:
			return true;
		default:
			return false;
	}
}

/*
 * Returns statements assigned to cycle's body
 */
static List *
get_cycle_body(PLpgSQL_stmt *stmt)
{
	List *stmts;

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

	return stmts;
}

/*
 * profiler_stmt_walker - iterator over plpgsql statements.
 *
 * This function is designed for two different purposes:
 *
 *   a) assign unique id to every plpgsql statement and
 *      create statement -> id mapping
 *   b) iterate over all commends and finalize total time
 *      as measured total time substract child total time.
 *   c) iterate over all commands and prepare result for
 *      plpgsql_profiler_function_statements_tb function.
 *   d) iterate over all commands to collect code coverage
 *      metrics
 */
static void
profiler_stmt_walker(profiler_info *pinfo,
					profiler_stmt_walker_mode mode,
					PLpgSQL_stmt *stmt,
					PLpgSQL_stmt *parent_stmt,
					const char *description,
					int stmt_block_num,
					profiler_stmt_walker_options *opts)
{
	profiler_stmt *pstmt = NULL;
	profiler_profile *profile = pinfo->profile;

	bool	prepare_profile_mode  = mode == PLPGSQL_CHECK_STMT_WALKER_PREPARE_PROFILE;
	bool	count_exec_time_mode  = mode == PLPGSQL_CHECK_STMT_WALKER_COUNT_EXEC_TIME;
	bool	prepare_result_mode	  = mode == PLPGSQL_CHECK_STMT_WALKER_PREPARE_RESULT;
	bool	collect_coverage_mode = mode == PLPGSQL_CHECK_STMT_WALKER_COLLECT_COVERAGE;

	int64	total_us_time = 0;
	int64	nested_us_time = 0;
	int64	exec_count = 0;

	int		stmtid = -1;

	char	strbuf[100];
	int		n = 0;
	List   *stmts;
	ListCell   *lc;

	Assert(profile);

	if (prepare_profile_mode)
	{
		profile_register_stmt(pinfo, opts, stmt);
	}
	else
	{
		stmtid = STMTID(stmt);

		if (count_exec_time_mode)
		{
			/*
			 * Get statement info from function execution context
			 * by statement id.
			 */
			pstmt = &pinfo->stmts[stmtid];
			pstmt->lineno = stmt->lineno;

			total_us_time = pstmt->us_total;
			opts->nested_us_time = 0;
		}
		else
		{
			profiler_stmt_reduced *ppstmt = NULL;

			Assert(opts->pi);

			/*
			 * When iterator is used, then id of iterator's current statement
			 * have to be same like stmtid of stmt. When function was not executed
			 * in active profile mode, then we have not any stored chunk, and
			 * iterator returns 0 stmtid.
			 */
#if PG_VERSION_NUM < 120000

			Assert(!opts->pi->current_chunk ||
				   opts->pi->current_statement == stmtid);

#else

			Assert(!opts->pi->current_chunk ||
				   profile->stmtid_reorder_map[opts->pi->current_statement] == stmtid);

#endif

			/*
			 * Get persistent statement info stored in shared memory
			 * or in session memory by iterator.
			 */
			ppstmt = get_stmt_profile_next(opts->pi);

			if (prepare_result_mode)
			{
				int parent_stmtid = parent_stmt ? ((int) STMTID(parent_stmt)) : -1;

				if (opts->pi->ri)
				{
					plpgsql_check_put_profile_statement(opts->pi->ri,
														ppstmt ? ppstmt->queryid : NOQUERYID,
														NATURAL_STMTID(pinfo, stmtid),
														NATURAL_STMTID(pinfo, parent_stmtid),
														description,
														stmt_block_num,
														stmt->lineno,
														ppstmt ? ppstmt->exec_count : 0,
														ppstmt ? ppstmt->exec_count_err : 0,
														ppstmt ? ppstmt->us_total : 0.0,
														ppstmt ? ppstmt->us_max : 0.0,
														ppstmt ? ppstmt->rows : 0,
														(char *) plpgsql_check__stmt_typename_p(stmt));
				}
			}
			else if (collect_coverage_mode)
			{
				/* save statement exec count */
				exec_count = ppstmt ? ppstmt->exec_count : 0;

				/* ignore invisible BLOCK */
				if (stmt->lineno != -1)
				{
					opts->cs->statements += 1;
					opts->cs->executed_statements += exec_count > 0 ? 1 : 0;
				}
			}
		}
	}

	if (is_cycle(stmt))
	{
		stmts = get_cycle_body(stmt);

		stmts_walker(pinfo, mode,
					 stmts, stmt, "loop body",
					 opts);

		if (collect_coverage_mode)
			increment_branch_counter(opts->cs,
									 opts->nested_exec_count);
	}
	else if (IS_PLPGSQL_STMT(stmt, PLPGSQL_STMT_IF))
	{
		PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;
		int64		all_nested_branches_exec_count = 0;

		/*
		 * Note: when if statement has not else path, then
		 * we have to calculate deduce number of execution
		 * of implicit else path manually. We know number
		 * of execution of IF statement and we can subtract
		 * an number of execution of nested paths.
		 */

		stmts_walker(pinfo, mode,
					 stmt_if->then_body, stmt, "then body",
					 opts);

		if (count_exec_time_mode)
		{
			nested_us_time = opts->nested_us_time;
		}
		else if (collect_coverage_mode)
		{
			increment_branch_counter(opts->cs,
									 opts->nested_exec_count);

			all_nested_branches_exec_count += opts->nested_exec_count;
		}

		foreach(lc, stmt_if->elsif_list)
		{
			stmts = ((PLpgSQL_if_elsif *) lfirst(lc))->stmts;

			sprintf(strbuf, "elsif %d", ++n);

			stmts_walker(pinfo, mode,
						 stmts, stmt, strbuf,
						 opts);

			if (count_exec_time_mode)
				nested_us_time += opts->nested_us_time;

			else if (collect_coverage_mode)
			{
				increment_branch_counter(opts->cs,
										 opts->nested_exec_count);

				all_nested_branches_exec_count += opts->nested_exec_count;
			}
		}

		if (stmt_if->else_body)
		{
			stmts_walker(pinfo, mode,
						 stmt_if->else_body, stmt, "else body",
						 opts);

			if (count_exec_time_mode)
				nested_us_time += opts->nested_us_time;

			else if (collect_coverage_mode)
				increment_branch_counter(opts->cs,
										 opts->nested_exec_count);
		}
		else
		{
			/* calculate exec_count for implicit else path */
			if (collect_coverage_mode)
			{
				int64 else_exec_count = exec_count - all_nested_branches_exec_count;

				increment_branch_counter(opts->cs,
										 else_exec_count);
			}
		}
	}
	else if (IS_PLPGSQL_STMT(stmt, PLPGSQL_STMT_CASE))
	{
		PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;

		foreach(lc, stmt_case->case_when_list)
		{
			stmts = ((PLpgSQL_case_when *) lfirst(lc))->stmts;

			sprintf(strbuf, "case when %d", ++n);

			stmts_walker(pinfo, mode,
						 stmts, stmt, strbuf,
						 opts);

			if (count_exec_time_mode)
				nested_us_time = opts->nested_us_time;

			else if (collect_coverage_mode)
				increment_branch_counter(opts->cs,
										 opts->nested_exec_count);
		}

		stmts_walker(pinfo, mode,
					 stmt_case->else_stmts, stmt, "case else",
					 opts);

		if (count_exec_time_mode)
			nested_us_time = opts->nested_us_time;

		else if (collect_coverage_mode)
			increment_branch_counter(opts->cs,
									 opts->nested_exec_count);
	}
	else if (IS_PLPGSQL_STMT(stmt, PLPGSQL_STMT_BLOCK))
	{
		PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) stmt;

		stmts_walker(pinfo, mode,
					 stmt_block->body, stmt, "body",
					 opts);

		if (count_exec_time_mode)
			nested_us_time = opts->nested_us_time;

		if (stmt_block->exceptions)
		{
			foreach(lc, stmt_block->exceptions->exc_list)
			{
				stmts = ((PLpgSQL_exception *) lfirst(lc))->action;

				sprintf(strbuf, "exception %d", ++n);

				stmts_walker(pinfo, mode,
							 stmts, stmt, strbuf,
							 opts);

				if (count_exec_time_mode)
					nested_us_time += opts->nested_us_time;
			}
		}
	}

	if (count_exec_time_mode)
	{
		Assert (pstmt);

		pstmt->us_total -= opts->nested_us_time;
		opts->nested_us_time = total_us_time;

		/*
		 * When max time is unknown, but statement was executed only
		 * only once, we can se max time.
		 */
		if (pstmt->exec_count == 1 && pstmt->us_max == 1)
			pstmt->us_max = pstmt->us_total;
	}
	else if (collect_coverage_mode)
	{
		opts->nested_exec_count = exec_count;
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
		HASH_SEQ_STATUS hash_seq;
		profiler_stmt_chunk *chunk;
		fstats	   *fstats_entry;

		LWLockAcquire(profiler_ss->lock, LW_EXCLUSIVE);

		hash_seq_init(&hash_seq, shared_profiler_chunks_HashTable);

		while ((chunk = hash_seq_search(&hash_seq)) != NULL)
		{
			hash_search(shared_profiler_chunks_HashTable,
						&(chunk->key),
						HASH_REMOVE,
						NULL);
		}

		LWLockRelease(profiler_ss->lock);

		Assert(shared_fstats_HashTable);

		LWLockAcquire(profiler_ss->fstats_lock, LW_EXCLUSIVE);

		hash_seq_init(&hash_seq, shared_fstats_HashTable);

		while ((fstats_entry = hash_seq_search(&hash_seq)) != NULL)
		{
			hash_search(shared_fstats_HashTable,
						&(fstats_entry->key),
						HASH_REMOVE,
						NULL);
		}

		LWLockRelease(profiler_ss->fstats_lock);
	}

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
	fstats_hashkey fhk;
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

	fstats_init_hashkey(&fhk, funcoid);

	if (shared_fstats_HashTable)
	{
		LWLockAcquire(profiler_ss->fstats_lock, LW_EXCLUSIVE);

		hash_search(shared_fstats_HashTable, (void *) &fhk, HASH_REMOVE, NULL);

		LWLockRelease(profiler_ss->fstats_lock);
	}
	else
		hash_search(fstats_HashTable, (void *) &fhk, HASH_REMOVE, NULL);

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

	plpgsql_check_iterate_over_profile(&cinfo,
									   PLPGSQL_CHECK_STMT_WALKER_COLLECT_COVERAGE,
									   NULL, &cs);

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

Datum
plpgsql_profiler_install_fake_queryid_hook(PG_FUNCTION_ARGS)
{
	(void) fcinfo;

	if (post_parse_analyze_hook == profiler_fake_queryid_hook)
		PG_RETURN_VOID();

	if (post_parse_analyze_hook == NULL)
		prev_post_parse_analyze_hook = post_parse_analyze_hook;

	post_parse_analyze_hook = profiler_fake_queryid_hook;

	PG_RETURN_VOID();
}

Datum
plpgsql_profiler_remove_fake_queryid_hook(PG_FUNCTION_ARGS)
{
	(void) fcinfo;

	if (post_parse_analyze_hook == profiler_fake_queryid_hook)
	{
		post_parse_analyze_hook = prev_post_parse_analyze_hook;
		prev_post_parse_analyze_hook = NULL;
	}

	PG_RETURN_VOID();
}

static void
update_persistent_fstats(PLpgSQL_function *func,
						 uint64 elapsed)
{
	HTAB	   *fstats_ht;
	bool		htab_is_shared;
	fstats_hashkey fhk;
	fstats	   *fstats_item;
	bool		found;
	bool		use_spinlock = false;

	fstats_init_hashkey(&fhk, func->fn_oid);

	/* try to find first chunk in shared (or local) memory */
	if (shared_fstats_HashTable)
	{
		LWLockAcquire(profiler_ss->fstats_lock, LW_SHARED);
		fstats_ht = shared_fstats_HashTable;
		htab_is_shared = true;
	}
	else
	{
		fstats_ht = fstats_HashTable;
		htab_is_shared = false;
	}

	fstats_item = (fstats *) hash_search(fstats_ht,
										 (void *) &fhk,
										 HASH_FIND,
										 &found);

	if (!found)
	{
		if (htab_is_shared)
		{
			LWLockRelease(profiler_ss->fstats_lock);
			LWLockAcquire(profiler_ss->fstats_lock, LW_EXCLUSIVE);
		}

		fstats_item = (fstats *) hash_search(fstats_ht,
											 (void *) &fhk,
											 HASH_ENTER,
											 &found);
	}

	if (!fstats_item)
		elog(ERROR,
			"cannot to insert new entry to profiler's function statistics");

	if (htab_is_shared)
	{
		if (found)
		{
			SpinLockAcquire(&fstats_item->mutex);
			use_spinlock = true;
		}
		else
			SpinLockInit(&fstats_item->mutex);
	}

	if (!found)
	{
		fstats_item->exec_count = 0;
		fstats_item->exec_count_err = 0;
		fstats_item->total_time = 0;
		fstats_item->total_time_xx = 0.0;
		fstats_item->min_time = elapsed;
		fstats_item->max_time = elapsed;
	}
	else
	{
		fstats_item->min_time = fstats_item->min_time < elapsed ? fstats_item->min_time : elapsed;
		fstats_item->max_time = fstats_item->max_time > elapsed ? fstats_item->max_time : elapsed;
	}

	eval_stddev_accum(&fstats_item->exec_count,
					  &fstats_item->total_time,
					  &fstats_item->total_time_xx,
					  elapsed);

	if (use_spinlock)
		SpinLockRelease(&fstats_item->mutex);

	if (htab_is_shared)
		LWLockRelease(profiler_ss->fstats_lock);
}

static void
update_persistent_profile(profiler_info *pinfo,
						  PLpgSQL_function *func)
{
#if PG_VERSION_NUM >= 120000

	profiler_profile *profile = pinfo->profile;

#endif

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

	/* don't need too strong lock for reading shared memory */
	chunk = (profiler_stmt_chunk *) hash_search(chunks,
											 (void *) &hk,
											 HASH_FIND,
											 &found);

	/* We need exclusive lock, when we want to add new chunk */
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

		for (i = 0; i < FUNC_NSTATEMENTS(pinfo); i++)
		{
			volatile profiler_stmt_reduced *prstmt;
			profiler_stmt *pstmt;

#if PG_VERSION_NUM >= 120000

			/*
			 * We need to store statement statistics to chunks in natural order
			 * next statistics should be related to statement on same or higher
			 * line. Unfortunately buildin stmtid has inverse order based on
			 * bison parser processing 
			 * statement should to be on same or higher line)
			 *
			 */
			int		n = profile->stmtid_reorder_map[i];

			/* Skip gaps in reorder map */
			if (n == -1)
				continue;

			pstmt = &pinfo->stmts[n];

#else

			pstmt = &pinfo->stmts[i];

#endif 

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
			prstmt->has_queryid = pstmt->has_queryid;
			prstmt->us_max = pstmt->us_max;
			prstmt->us_total = pstmt->us_total;
			prstmt->rows = pstmt->rows;
			prstmt->exec_count = pstmt->exec_count;
			prstmt->exec_count_err = pstmt->exec_count_err;
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
		for (i = 0; i < FUNC_NSTATEMENTS(pinfo); i++)
		{
			volatile profiler_stmt_reduced *prstmt;
			profiler_stmt *pstmt;

#if PG_VERSION_NUM >= 120000

			/*
			 * We need to store statement statistics to chunks in natural order
			 * (next statistics should be related to statement on same or higher
			 * line). Unfortunately buildin stmtid has inverse order based on
			 * bison parser processing statement.
			 */
			int		n = profile->stmtid_reorder_map[i];

			/* Skip gaps in reorder map */
			if (n == -1)
				continue;

			pstmt = &pinfo->stmts[n];

#else

			pstmt = &pinfo->stmts[i];

#endif

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
			prstmt->exec_count_err += pstmt->exec_count_err;
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

#if PG_VERSION_NUM < 120000

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
profiler_update_map(profiler_profile *profile,
					profiler_stmt_walker_options *opts,
					PLpgSQL_function *function,
					PLpgSQL_stmt *stmt)
{
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
		pme->function = function;
		pme->stmt = stmt;
		pme->stmtid = opts->stmtid++;
		pme->next = NULL;
	}
	else
	{
		MemoryContext oldcxt;
		profiler_map_entry *new_pme;

		oldcxt = MemoryContextSwitchTo(profiler_mcxt);

		new_pme = palloc0(sizeof(profiler_map_entry));

		new_pme->function = function;
		new_pme->stmt = stmt;
		new_pme->stmtid = opts->stmtid++;
		new_pme->next = NULL;

		while (pme->next)
			pme = pme->next;

		pme->next = new_pme;

		MemoryContextSwitchTo(oldcxt);
	}
}

#endif

static void
profile_register_stmt(profiler_info *pinfo,
					  profiler_stmt_walker_options *opts,
					  PLpgSQL_stmt *stmt)
{
#if PG_VERSION_NUM < 120000

		profiler_update_map(pinfo->profile, opts, pinfo->func, stmt);

#else

		pinfo->profile->stmtid_reorder_map[opts->stmtid++] = stmt->stmtid - 1;

#endif
}

/*
 * Returns statement id assigned to plpgsql statement. Should be
 * fast, because lineno is usually unique.
 */
#if PG_VERSION_NUM < 120000

static int
profiler_get_stmtid(profiler_profile *profile, PLpgSQL_function *function, PLpgSQL_stmt *stmt)
{

	int		lineno = stmt->lineno;
	profiler_map_entry *pme;
	int			i;
	bool		found = false;

	for (i = 0; i < profile->n_mapped_functions; i++)
	{
		if (profile->mapped_functions[i] == function)
		{
			found = true;
			break;
		}
	}

	if (!found)
		elog(ERROR, "Internal error - this compiled function has not created statement map");

	if (lineno > profile->stmts_map_max_lineno)
		elog(ERROR, "broken statement map - too high lineno");

	pme = &profile->stmts_map[lineno];

	/* pme->stmt should not be null */
	if (!pme->stmt)
		elog(ERROR, "broken statement map - broken format on line: %d", lineno);

	while (pme && !
			(pme->stmt == stmt && pme->function == function))
		pme = pme->next;

	/* we should to find statement */
	if (!pme)
		elog(ERROR, "broken statement map - cannot to find statement on line: %d", lineno);

	return pme->stmtid;
}

#endif

/*
 * Iterate over list of statements
 */
static void
stmts_walker(profiler_info *pinfo,
			 profiler_stmt_walker_mode mode,
			 List *stmts,
			 PLpgSQL_stmt *parent_stmt,
			 const char *description,
			 profiler_stmt_walker_options *opts)
{
	bool	count_exec_time = mode == PLPGSQL_CHECK_STMT_WALKER_COUNT_EXEC_TIME;
	bool	collect_coverage = mode == PLPGSQL_CHECK_STMT_WALKER_COLLECT_COVERAGE;

	int64 nested_us_time = 0;
	int64 nested_exec_count = 0;

	int			stmt_block_num = 1;

	ListCell   *lc;

	foreach(lc, stmts)
	{
		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *) lfirst(lc);

		if (count_exec_time)
			opts->nested_us_time = 0;

		if (collect_coverage)
			opts->nested_exec_count = 0;

		profiler_stmt_walker(pinfo, mode,
							 stmt, parent_stmt, description,
							 stmt_block_num,
							 opts);

		 /* add stmt execution time to total execution time */
		if (count_exec_time)
			nested_us_time += opts->nested_us_time;

		/*
		 * For calculation of coverage we need a numbers of nested statements
		 * execution. Usually or statements in list has same number of execution.
		 * But it should not be true for some reasons (after RETURN or some exception).
		 * I am not sure if following simplification is accurate, but maybe. I use
		 * number of execution of first statement in block like number of execution
		 * all statements in list.
		 */
		if (collect_coverage && stmt_block_num == 1)
			nested_exec_count = opts->nested_exec_count;

		stmt_block_num += 1;
	}

	if (count_exec_time)
		opts->nested_us_time = nested_us_time;

	if (collect_coverage)
		opts->nested_exec_count = nested_exec_count;
}

/*
 * Given a PLpgSQL_stmt, return the underlying PLpgSQL_expr that may contain a
 * queryid.
 */
static PLpgSQL_expr  *
profiler_get_expr(PLpgSQL_stmt *stmt, bool *dynamic, List **params)
{
	PLpgSQL_expr *expr = NULL;

	*params = NIL;
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
#if PG_VERSION_NUM < 140000
		case PLPGSQL_STMT_SET:
			expr = ((PLpgSQL_stmt_set *) stmt)->expr;
			break;
#endif			/* PG_VERSION_NUM < 140000 */

#endif			/* PG_VERSION_NUM >= 110000 */

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
			*params = ((PLpgSQL_stmt_dynfors *) stmt)->params;
			*dynamic = true;
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
					*params = q->params;
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
			*params = ((PLpgSQL_stmt_dynexecute *) stmt)->params;
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
		case PLPGSQL_STMT_FORS:

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
profiler_get_dyn_queryid(PLpgSQL_execstate *estate, PLpgSQL_expr *expr, query_params *qparams)
{
	MemoryContext oldcxt;
	Query	   *query;
	RawStmt    *parsetree;
	bool		snapshot_set;
	List	   *parsetree_list;
	PLpgSQL_var result;
	PLpgSQL_type typ;
	char	   *query_string = NULL;
	Oid		   *paramtypes = NULL;
	int			nparams = 0;

	if (qparams)
	{
		paramtypes = qparams->paramtypes;
		nparams = qparams->nparams;
	}

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

	if (profiler_queryid_mcxt == NULL)
		profiler_queryid_mcxt = AllocSetContextCreate(TopMemoryContext,
				"plpgsql_check - profiler queryid context",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	oldcxt = MemoryContextSwitchTo(profiler_queryid_mcxt);
	MemoryContextSwitchTo(oldcxt);

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
	{
		MemoryContextSwitchTo(oldcxt);
		MemoryContextReset(profiler_queryid_mcxt);
		return NOQUERYID;
	}

	/* Run through the raw parsetree and process it. */
	parsetree = (RawStmt *) linitial(parsetree_list);
	snapshot_set = false;

	/*
	 * Set up a snapshot if parse analysis/planning will need one.
	 */
	if (analyze_requires_snapshot(parsetree))
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		snapshot_set = true;
	}

	query = parse_analyze(parsetree, query_string, paramtypes, nparams, NULL);

	if (snapshot_set)
		PopActiveSnapshot();

	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(profiler_queryid_mcxt);

	return query->queryId;
}

/*
 * Returns result type of already executed (has assigned plan) expression
 */
static bool
get_expr_type(PLpgSQL_expr *expr, Oid *result_type)
{
	if (expr)
	{
		SPIPlanPtr ptr  = expr->plan;

		if (ptr)
		{
			List	   *plan_sources = SPI_plan_get_plan_sources(ptr);

			if (plan_sources && list_length(plan_sources) == 1)
			{
				CachedPlanSource *plan_source;
				TupleDesc	tupdesc;

				plan_source = (CachedPlanSource *) linitial(plan_sources);
				tupdesc = plan_source->resultDesc;

				if (tupdesc->natts == 1)
				{
					*result_type = TupleDescAttr(tupdesc, 0)->atttypid;

					return true;
				}
			}
		}
	}

	return false;
}


/* Return the first queryid found in the given PLpgSQL_stmt, if any. */
static pc_queryid
profiler_get_queryid(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt,
					 bool *has_queryid, query_params **qparams)
{
	PLpgSQL_expr *expr;
	bool		dynamic;
	List	   *params;
	List	   *plan_sources;

	expr = profiler_get_expr(stmt, &dynamic, &params);
	*has_queryid = (expr != NULL);

	/* fast leaving, when expression has not assigned plan */
	if (!expr || !expr->plan)
		return NOQUERYID;

	if (dynamic)
	{
		Assert(expr);

		if (params && !*qparams)
		{
			query_params *qps = NULL;
			int		nparams = list_length(params);
			int		paramno = 0;
			MemoryContext oldcxt;
			ListCell *lc;

			/* build array of Oid used like dynamic query parameters */
			oldcxt = MemoryContextSwitchTo(profiler_mcxt);
			qps = (query_params *) palloc(sizeof(Oid) * nparams + sizeof(int));
			MemoryContextSwitchTo(oldcxt);

			foreach(lc, params)
			{
				PLpgSQL_expr *param_expr = (PLpgSQL_expr *) lfirst(lc);

				if (!get_expr_type(param_expr, &qps->paramtypes[paramno++]))
				{
					free(qps);
					return NOQUERYID;
				}
			}

			qps->nparams = nparams;
			*qparams = qps;
		}

		return profiler_get_dyn_queryid(estate, expr, *qparams);
	}

	plan_sources = SPI_plan_get_plan_sources(expr->plan);

	if (plan_sources)
	{
		CachedPlanSource *plan_source = (CachedPlanSource *) linitial(plan_sources);

		if (plan_source->query_list)
		{
			Query *q = linitial_node(Query, plan_source->query_list);

			return q->queryId;
		}
	}

	return NOQUERYID;
}

/*
 * Generate simple queryid  for testing purpose.
 * DO NOT USE IN PRODUCTION.
 */
#if PG_VERSION_NUM >= 140000

static void
profiler_fake_queryid_hook(ParseState *pstate, Query *query, JumbleState *jstate)
{
	(void) jstate;

#else

static void
profiler_fake_queryid_hook(ParseState *pstate, Query *query)
{

#endif

	(void) pstate;

	Assert(query->queryId == NOQUERYID);

	query->queryId = query->commandType;
}

/*
 * This routine does an update or creating new profile. Function's profile
 * holds statements metadata (statement map).  The statement map is necessary
 * for pre pg 12 releases, where we have not statement id. Instead we have
 * a structure, that allows mapping statement's pointer to assigned id.
 * The work is complicated by fact so we can have more different (independent)
 * tries of statements for one real function. Then we need two fields
 * (function_ptr, stmt_ptr) for workable searching statement id for all
 * compiled variant of one function (identified by oid).
 *
 * PostgreSQL 12 has assigned statement id, so there is not necessity to
 * prepare and maintain statement map. On second hand these releases needs
 * reorder map for reordering statement id to natural order of execution.
 */
static void
prepare_profile(profiler_info *pinfo,
				profiler_profile *profile,
				bool init)
{
	profiler_stmt_walker_options opts;
	PLpgSQL_function *func;
	int		i;

#if PG_VERSION_NUM < 120000

	bool	found = false;

#endif

	Assert(pinfo && pinfo->func);

	memset(&opts, 0, sizeof(profiler_stmt_walker_options));

	func = pinfo->func;
	pinfo->profile = profile;

	if (init)
	{
		MemoryContext oldcxt;

#if PG_VERSION_NUM < 120000

		oldcxt = MemoryContextSwitchTo(profiler_mcxt);

		profile->nstatements = 0;
		profile->n_mapped_functions = 0;

		profile->stmts_map_max_lineno = 200;
		profile->max_mapped_functions = 10;

		profile->stmts_map = palloc0((profile->stmts_map_max_lineno + 1) * sizeof(profiler_map_entry));
		profile->mapped_functions = palloc0(profile->max_mapped_functions * sizeof(PLpgSQL_function *));

		MemoryContextSwitchTo(oldcxt);

#else

		oldcxt = MemoryContextSwitchTo(profiler_mcxt);

		profile->stmtid_reorder_map = palloc0(sizeof(int) * func->nstatements);

		/*
		 * I found my bug in PLpgSQL runtime - when function statement counter
		 * is incremental 2x for every FOR cycle. Until fix this bug, some entries
		 * of reorder map can be uniinitialized, and we have to detect these entries.
		 */
		for (i = 0; i < ((int) func->nstatements); i++)
			profile->stmtid_reorder_map[i] = -1;

		MemoryContextSwitchTo(oldcxt);

		opts.stmtid = 0;
		profiler_stmt_walker(pinfo, PLPGSQL_CHECK_STMT_WALKER_PREPARE_PROFILE,
							 (PLpgSQL_stmt *) func->action, NULL, NULL, 1,
							 &opts);

#endif

	}

#if PG_VERSION_NUM < 120000

	/*
	 * Every touched incarnation should to have statement map.
	 */
	for (i = 0; i < profile->n_mapped_functions; i++)
	{
		if (profile->mapped_functions[i] == func)
		{
			found = true;
			break;
		}
	}

	if (!found)
	{
		/*
		 * Ensure correct size of array of pointers to function incarnations with
		 * prepared statement map.
		 */
		if (profile->n_mapped_functions == profile->max_mapped_functions)
		{
			int		new_max_mapped_functions = profile->max_mapped_functions * 2;

			if (new_max_mapped_functions > 200)
				elog(ERROR, "too much different incarnations of one function (please, close session)");

			profile->mapped_functions = repalloc(profile->mapped_functions,
												 new_max_mapped_functions * sizeof(PLpgSQL_function *));

			profile->max_mapped_functions = new_max_mapped_functions;
		}

		profile->mapped_functions[profile->n_mapped_functions++] = func;

		opts.stmtid = 0;
		profiler_stmt_walker(pinfo, PLPGSQL_CHECK_STMT_WALKER_PREPARE_PROFILE,
							 (PLpgSQL_stmt *) func->action, NULL, NULL, 1,
							 &opts);

		if (profile->nstatements > 0 && profile->nstatements != opts.stmtid)
			elog(ERROR,
					"internal error - unexpected number of statements in different function incarnations (%d <> %d)",
					opts.stmtid, profile->nstatements);

		profile->nstatements = opts.stmtid;
	}

#endif

}

/*
 * Prepare tuplestore with function profile
 *
 */
void
plpgsql_check_iterate_over_profile(plpgsql_check_info *cinfo,
								   profiler_stmt_walker_mode mode,
								   plpgsql_check_result_info *ri,
								   coverage_state *cs)
{
#if PG_VERSION_NUM >= 120000

	LOCAL_FCINFO(fake_fcinfo, 0);

#else

	FunctionCallInfoData fake_fcinfo_data;
	FunctionCallInfo fake_fcinfo = &fake_fcinfo_data;

#endif

	profiler_profile *profile;
	profiler_hashkey hk_function;
	bool		found_profile = false;

	FmgrInfo	flinfo;
	TriggerData trigdata;
	EventTriggerData etrigdata;
	Trigger tg_trigger;
	ReturnSetInfo rsinfo;
	bool		fake_rtd;
	profiler_info pinfo;
	profiler_stmt_chunk *first_chunk = NULL;
	profiler_iterator		pi;
	volatile bool		unlock_mutex = false;
	bool		shared_chunks;
	profiler_stmt_walker_options opts;

	memset(&opts, 0, sizeof(profiler_stmt_walker_options));

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
		PLpgSQL_stmt *stmt;

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

		pinfo.func = plpgsql_check__compile_p(fake_fcinfo, false);

		profiler_init_hashkey(&hk_function, pinfo.func);
		profile = (profiler_profile *) hash_search(profiler_HashTable,
												 (void *) &hk_function,
												 HASH_ENTER,
												 &found_profile);

		prepare_profile(&pinfo, profile, !found_profile);

		opts.pi =  &pi;
		opts.cs = cs;

		stmt = (PLpgSQL_stmt *) pinfo.func->action;

		profiler_stmt_walker(&pinfo, mode, stmt, NULL, NULL, 1, &opts);
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
			int64		exec_count_err = 0;
			Datum		queryids_array = (Datum) 0;
			Datum		max_time_array = (Datum) 0;
			Datum		processed_rows_array = (Datum) 0;
			int			cmds_on_row = 0;
			int			queryids_on_row = 0;

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

				queryids_abs = initArrayResult(INT8OID, CurrentMemoryContext, true);
				max_time_abs = initArrayResult(FLOAT8OID, CurrentMemoryContext, true);
				processed_rows_abs = initArrayResult(INT8OID, CurrentMemoryContext, true);

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
					if (0 && chunk->stmts[current_statement].lineno < lineno)
					{
						current_statement += 1;
						continue;
					}
					else if (chunk->stmts[current_statement].lineno == lineno)
					{
						profiler_stmt_reduced *prstmt = &chunk->stmts[current_statement];

						us_total += prstmt->us_total;
						exec_count += prstmt->exec_count;
						exec_count_err += prstmt->exec_count_err;

						stmt_lineno = lineno;

						if (prstmt->has_queryid)
						{
							if (prstmt->queryid != NOQUERYID)
							{
								queryids_abs = accumArrayResult(queryids_abs,
																Int64GetDatum((int64) prstmt->queryid),
																prstmt->queryid == NOQUERYID,
																INT8OID,
																CurrentMemoryContext);
								queryids_on_row += 1;
							}
						}

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

				if (queryids_on_row > 0)
					queryids_array = makeArrayResult(queryids_abs, CurrentMemoryContext);

				if (cmds_on_row > 0)
				{
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
								   exec_count_err,
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
static profiler_info *
init_profiler_info(profiler_info *pinfo, PLpgSQL_function *func)
{
	if (!pinfo)
	{
		pinfo = palloc0(sizeof(profiler_info));
		pinfo->pi_magic = PI_MAGIC;
		pinfo->func = func;
	}

	return pinfo;
}

/*
 * Try to search profile pattern for function. Creates profile pattern when
 * it doesn't exists.
 */
void
plpgsql_check_profiler_func_init(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	profiler_info *pinfo = NULL;

	if (plpgsql_check_tracer)
	{

#if PG_VERSION_NUM >= 120000

		int		group_number_counter = 0;

#endif

		pinfo = init_profiler_info(pinfo, func);
		pinfo->trace_info_is_initialized = true;

#if PG_VERSION_NUM >= 120000

		pinfo->stmt_start_times = palloc0(sizeof(instr_time) * func->nstatements);
		pinfo->stmt_group_numbers = palloc(sizeof(int) * func->nstatements);
		pinfo->stmt_parent_group_numbers = palloc(sizeof(int) * func->nstatements);
		pinfo->stmt_disabled_tracers = palloc0(sizeof(int) * func->nstatements);

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

	}

	if (plpgsql_check_profiler && func->fn_oid != InvalidOid)
	{
		profiler_profile *profile;
		profiler_hashkey hk;
		bool		found;

		profiler_init_hashkey(&hk, func);
		profile = (profiler_profile *) hash_search(profiler_HashTable,
												   (void *) &hk,
												   HASH_ENTER,
												   &found);

		pinfo = init_profiler_info(pinfo, func);
		prepare_profile(pinfo, profile, !found);

		pinfo->stmts = palloc0(FUNC_NSTATEMENTS(pinfo) * sizeof(profiler_stmt));
	}

	if (pinfo)
	{
		INSTR_TIME_SET_CURRENT(pinfo->start_time);

		pinfo->estate = estate;
	}

	estate->plugin_info = pinfo;

	if (top_pinfo)
	{
		top_pinfo->pinfo = pinfo;
		curr_eval_econtext = estate->eval_econtext;
	}
}

void
plpgsql_check_profiler_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	profiler_info *pinfo = NULL;

	if (estate)
		pinfo = (profiler_info *) estate->plugin_info;
	else if (top_pinfo)
		pinfo = top_pinfo->pinfo;

	if (plpgsql_check_tracer && pinfo )
	{
		if (estate)
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
		int		entry_stmtid = STMTID(pinfo->func->action);

		instr_time		end_time;
		uint64			elapsed;
		profiler_stmt_walker_options opts;

		memset(&opts, 0, sizeof(profiler_stmt_walker_options));

		INSTR_TIME_SET_CURRENT(end_time);
		INSTR_TIME_SUBTRACT(end_time, pinfo->start_time);

		elapsed = INSTR_TIME_GET_MICROSEC(end_time);

		if (pinfo->stmts[entry_stmtid].exec_count == 0)
		{
			pinfo->stmts[entry_stmtid].exec_count = 1;
			pinfo->stmts[entry_stmtid].exec_count_err = 0;
			pinfo->stmts[entry_stmtid].us_total = elapsed;
			pinfo->stmts[entry_stmtid].us_max = elapsed;
		}

		/* finalize profile - get result profile */

		profiler_stmt_walker(pinfo, PLPGSQL_CHECK_STMT_WALKER_COUNT_EXEC_TIME,
							 (PLpgSQL_stmt *) pinfo->func->action, NULL, NULL, 1,
							 &opts);

		update_persistent_profile(pinfo, func);
		update_persistent_fstats(func, elapsed);

		pfree(pinfo->stmts);
	}

	if ((plpgsql_check_tracer || plpgsql_check_profiler) && pinfo)
		pfree(pinfo);
}

void
plpgsql_check_profiler_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	if (top_pinfo && top_pinfo->pinfo)
	{
		if (estate->eval_econtext != curr_eval_econtext)
		{
			if (estate->cur_error)
			{
				int		i;
				bool	found = false;

				/*
				 * detected exception handler. We have to close
				 * all statements from stack, until we find common
				 * eval_context. The reduction is possible, only when
				 * we found common eval_context. We can lost if if this
				 * shared context is over then NESTED_STMTS_STACK_SIZE.
				 */
				for (i = top_pinfo->nested_stmts_count - 1; i >= 0; i--)
				{
					if (i < NESTED_STMTS_STACK_SIZE)
					{
						if (top_pinfo->eval_econtext[i] == estate->eval_econtext)
						{
							found = true;
							break;
						}
					}
				}

				if (found)
				{
					for (i = top_pinfo->nested_stmts_count - 1; i >= 0; i--)
					{
						if (i < NESTED_STMTS_STACK_SIZE)
						{
							if (top_pinfo->eval_econtext[i] == estate->eval_econtext)
							{
								top_pinfo->nested_stmts_count = i + 1;
								break;
							}
							else
								plpgsql_check_profiler_stmt_end(NULL, top_pinfo->nested_stmts[i]);
						}
					}
				}
			}

			curr_eval_econtext = estate->eval_econtext;
		}

		if (top_pinfo->nested_stmts_count < NESTED_STMTS_STACK_SIZE)
		{
			top_pinfo->nested_stmts[top_pinfo->nested_stmts_count] = stmt;
			top_pinfo->eval_econtext[top_pinfo->nested_stmts_count] = estate->eval_econtext;
		}

		top_pinfo->nested_stmts_count++;
	}


	if (plpgsql_check_tracer && pinfo)
	{

#if PG_VERSION_NUM >= 120000

		int		stmtid = stmt->stmtid - 1;
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
		int stmtid = STMTID(stmt);
		profiler_stmt *pstmt = &pinfo->stmts[stmtid];

		Assert(pinfo->pi_magic == PI_MAGIC);

		INSTR_TIME_SET_CURRENT(pstmt->start_time);
	}
}

/*
 * Cleaning mode is used for closing unfinished statements after an exception.
 */
void
plpgsql_check_profiler_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	profiler_info *pinfo;
	bool	cleaning_mode = false;
	bool	is_error_stmt = false;

	if (!estate)
	{
		Assert(top_pinfo && top_pinfo->pinfo);

		pinfo = top_pinfo->pinfo;

		estate = pinfo->estate;
		is_error_stmt = estate->err_stmt == stmt;
		cleaning_mode = true;
	}
	else
		pinfo = (profiler_info *) estate->plugin_info;

	if (top_pinfo && top_pinfo->pinfo && !cleaning_mode)
	{
		int		i;
		bool	found = false;

		top_pinfo->nested_stmts_count--;

		/*
		 * try to synchronize counter, we can lost synchronization when
		 * there was handled exception and deeper statement's stack than
		 * NESTED_STMTS_STACK_SIZE.
		 */
		for (i = top_pinfo->nested_stmts_count; i >= 0; i--)
		{
			if (i < NESTED_STMTS_STACK_SIZE)
			{
				if (top_pinfo->nested_stmts[i] == stmt)
				{
					found = true;
					break;
				}
			}
		}

		if (found)
		{
			for (i = top_pinfo->nested_stmts_count; i >= 0; i--)
			{
				if (i < NESTED_STMTS_STACK_SIZE)
				{
					if (top_pinfo->nested_stmts[i] == stmt)
					{
						top_pinfo->nested_stmts_count = i;
						break;
					}
					else
						plpgsql_check_profiler_stmt_end(NULL, top_pinfo->nested_stmts[i]);
				}
			}
		}
	}

	if (plpgsql_check_tracer && pinfo)
	{

#if PG_VERSION_NUM >= 120000

		int		stmtid = stmt->stmtid - 1;

		if (plpgsql_check_runtime_pragma_vector_changed)
		{
			int		sgn;

			sgn = pinfo->stmt_group_numbers[stmtid];

			pinfo->pragma_disable_tracer_stack[sgn] =
				plpgsql_check_runtime_pragma_vector.disable_tracer;
		}

#endif

		/* These nodes was not executed */
		if (!cleaning_mode)
			plpgsql_check_tracer_on_stmt_end(estate, stmt);
	}

	if (plpgsql_check_profiler &&
		pinfo && pinfo->profile &&
		pinfo->func->fn_oid != InvalidOid)
	{
		int stmtid = STMTID(stmt);
		profiler_stmt *pstmt = &pinfo->stmts[stmtid];
		instr_time		end_time;
		uint64			elapsed;
		instr_time		end_time2;

		Assert(pinfo->pi_magic == PI_MAGIC);

		if (pstmt->queryid == NOQUERYID && estate)
			pstmt->queryid = profiler_get_queryid(estate, stmt,
												  &pstmt->has_queryid,
												  &pstmt->qparams);

		INSTR_TIME_SET_CURRENT(end_time);
		end_time2 = end_time;
		INSTR_TIME_ACCUM_DIFF(pstmt->total, end_time, pstmt->start_time);

		INSTR_TIME_SUBTRACT(end_time2, pstmt->start_time);
		elapsed = INSTR_TIME_GET_MICROSEC(end_time2);

		if (elapsed > pstmt->us_max)
			pstmt->us_max = elapsed;

		pstmt->us_total = INSTR_TIME_GET_MICROSEC(pstmt->total);

		if (!cleaning_mode)
			pstmt->rows += estate->eval_processed;

		pstmt->exec_count++;

		if (is_error_stmt)
			pstmt->exec_count_err++;
	}
}

void
plpgsql_check_profiler_iterate_over_all_profiles(plpgsql_check_result_info *ri)
{
	HASH_SEQ_STATUS seqstatus;
	fstats		*fstats_item;

	HTAB	   *fstats_ht;
	bool		htab_is_shared;

	/* try to find first chunk in shared (or local) memory */
	if (shared_fstats_HashTable)
	{
		LWLockAcquire(profiler_ss->fstats_lock, LW_SHARED);
		fstats_ht = shared_fstats_HashTable;
		htab_is_shared = true;
	}
	else
	{
		fstats_ht = fstats_HashTable;
		htab_is_shared = false;
	}

	hash_seq_init(&seqstatus, fstats_ht);

	while ((fstats_item = (fstats *) hash_seq_search(&seqstatus)) != NULL)
	{
		Oid		fn_oid,
				db_oid;
		uint64	exec_count,
				exec_count_err,
				total_time,
				min_time,
				max_time;

		float8	total_time_xx;
		HeapTuple	tp;

		if (htab_is_shared)
			SpinLockAcquire(&fstats_item->mutex);

		fn_oid = fstats_item->key.fn_oid;
		db_oid = fstats_item->key.db_oid;
		exec_count = fstats_item->exec_count;
		exec_count_err = fstats_item->exec_count_err;
		total_time = fstats_item->total_time;
		total_time_xx = fstats_item->total_time_xx;
		min_time = fstats_item->min_time;
		max_time = fstats_item->max_time;

		if (htab_is_shared)
			SpinLockRelease(&fstats_item->mutex);

		/*
		 * only function's statistics for current database can be displayed here,
		 * Oid of functions from other databases has unassigned oids to current
		 * system catalogue.
		 */
		if (db_oid != MyDatabaseId)
			continue;

		/* check if function has name */
		tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(fn_oid));
		if (!HeapTupleIsValid(tp))
			continue;

		ReleaseSysCache(tp);

		plpgsql_check_put_profiler_functions_all_tb(ri,
													fn_oid,
													exec_count,
													exec_count_err,
													(double) total_time,
													ceil(total_time / ((double) exec_count)),
													ceil(sqrt(total_time_xx / exec_count)),
													(double) min_time,
													(double) max_time);
	}

	if (htab_is_shared)
		LWLockRelease(profiler_ss->fstats_lock);
}

/*
 * Used as needs_fmgr_hook. All plpgsql functions
 * needs this hook when profiler is active.
 */
bool
plpgsql_check_needs_fmgr_hook(Oid fn_oid)
{
	if (plpgsql_check_next_needs_fmgr_hook &&
		(*plpgsql_check_next_needs_fmgr_hook)(fn_oid))
		return true;

	if (!plpgsql_check_profiler)
		return false;

	return plpgsql_check_is_plpgsql_function(fn_oid);
}


void
plpgsql_check_fmgr_hook(FmgrHookEventType event,
						FmgrInfo *flinfo, Datum *private)
{
	fmgr_hook_private *stack;

	switch (event)
	{
		case FHET_START:
			stack = (fmgr_hook_private *) DatumGetPointer(*private);
			if (!stack)
			{
				MemoryContext oldcxt;

				oldcxt = MemoryContextSwitchTo(flinfo->fn_mcxt);
				stack = palloc(sizeof(fmgr_hook_private));
				stack->use_plpgsql = plpgsql_check_is_plpgsql_function(flinfo->fn_oid);
				stack->next_private = 0;

				MemoryContextSwitchTo(oldcxt);

				*private = PointerGetDatum(stack);
			}

			if (stack->use_plpgsql)
			{
				profiler_stack *pstack = palloc0(sizeof(profiler_stack));

				pstack->prev_pinfo = top_pinfo;
				top_pinfo = pstack;
			}

			if (plpgsql_check_next_fmgr_hook)
				(*plpgsql_check_next_fmgr_hook) (event, flinfo, &stack->next_private);

			break;

		case FHET_END:
		case FHET_ABORT:
			stack = (fmgr_hook_private *) DatumGetPointer(*private);

			if (stack->use_plpgsql)
			{
				profiler_stack *pstack = top_pinfo->prev_pinfo;

				if (event == FHET_ABORT)
				{
					profiler_info *pinfo = top_pinfo->pinfo;
					int		i;

					if (pinfo)
					{
						for (i = top_pinfo->nested_stmts_count - 1; i >= 0; i--)
						{
							if (i < NESTED_STMTS_STACK_SIZE)
								plpgsql_check_profiler_stmt_end(NULL, top_pinfo->nested_stmts[i]);
						}

						plpgsql_check_profiler_func_end(NULL, pinfo->func);
					}
				}

				pfree(top_pinfo);
				top_pinfo = pstack;
			}

			if (plpgsql_check_next_fmgr_hook)
				(*plpgsql_check_next_fmgr_hook) (event, flinfo, &stack->next_private);

			break;
	}
}
