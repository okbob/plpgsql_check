/*-------------------------------------------------------------------------
 *
 * profiler.c
 *
 *			  profiler accessories code
 *
 * by Pavel Stehule 2013-2026
 *
 *-------------------------------------------------------------------------
 */
#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/float.h"

#include <math.h>

/*
 * It is unique for function with same oid. For function statistic
 * We don't want to have multiple stats per every update of any
 * function.
 */
typedef struct func_hashkey
{
	Oid			fn_oid;
	Oid			db_oid;
} func_hashkey;

typedef struct FuncStats
{
	func_hashkey key;
	slock_t		mutex;
	uint64		exec_count;
	uint64		exec_count_err;
	uint64		total_time;
	double		total_time_xx;
	uint64		min_time;
	uint64		max_time;
} FuncStats;

/*
 * This is used as cache for types of expressions of USING clause
 * (EXECUTE like statements).
 */
typedef struct QParams
{
	int			nparams;
	Oid			paramtypes[FLEXIBLE_ARRAY_MEMBER];
} QParams;

/*
 * PLpgSQL statement instrumentation
 * Attention - the commands that can contains nestested commands
 * has attached own time and nested statements time too.
 */
typedef struct StmtInstr
{
	bool		has_queryid;
	pc_queryid	queryid;
	uint64		us_max;
	uint64		us_total;
	uint64		rows;
	uint64		exec_count;
	uint64		exec_count_err;
	instr_time	start_time;
	instr_time	total;
	QParams *qparams;
} StmtInstr;

typedef struct StmtStats
{
	bool		has_queryid;
	pc_queryid	queryid;
	uint64		us_max;
	uint64		us_total;
	uint64		rows;
	uint64		exec_count;
	uint64		exec_count_err;
} StmtStats;

#define		FUNC_STATS_COUNT				20000

/*
 * It is unique for any version of function with same oid.
 * It ensure strong relation between runtime data and source code
 * (pg_proc tuple).
 */
typedef struct func_identity_hashkey
{
	Oid			fn_oid;
	Oid			db_oid;
	TransactionId fn_xmin;
	ItemPointerData fn_tid;
} func_identity_hashkey;

typedef struct FuncStmtsStats
{
	func_identity_hashkey key;
	int			nstatements;
	slock_t		mutex;
	StmtStats  *sstats;
	int			shared_sstats_offset;
} FuncStmtsStats;

typedef struct ProfilerSharedState
{
	LWLock	   *func_stats_lock;
	LWLock	   *func_stmts_stats_lock;
	int			stmt_stats_count;
	int			used_stmt_stats_count;
} ProfilerSharedState;

/*
 * This structure is used as plpgsql extension parameter
 */
typedef struct profiler_info
{
	StmtInstr *sinstrs;
	StmtStats *sstats;
	int			nstatements;
	instr_time	start_time;
	PLpgSQL_function *func;
} profiler_info;

enum
{
	COVERAGE_STATEMENTS,
	COVERAGE_BRANCHES
};

PG_FUNCTION_INFO_V1(plpgsql_check_profiler_ctrl);
PG_FUNCTION_INFO_V1(plpgsql_profiler_reset_all);
PG_FUNCTION_INFO_V1(plpgsql_profiler_reset);
PG_FUNCTION_INFO_V1(plpgsql_coverage_statements);
PG_FUNCTION_INFO_V1(plpgsql_coverage_branches);
PG_FUNCTION_INFO_V1(plpgsql_coverage_statements_name);
PG_FUNCTION_INFO_V1(plpgsql_coverage_branches_name);
PG_FUNCTION_INFO_V1(plpgsql_profiler_install_fake_queryid_hook);
PG_FUNCTION_INFO_V1(plpgsql_profiler_remove_fake_queryid_hook);

static void update_persistent_stmts_stats(profiler_info *pinfo);
static pc_queryid profiler_get_queryid(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, bool *has_queryid, QParams **qparams);

#if PG_VERSION_NUM >= 190000

static void profiler_fake_queryid_hook(ParseState *pstate, Query *query, const JumbleState *jstate);

#else

static void profiler_fake_queryid_hook(ParseState *pstate, Query *query, JumbleState *jstate);

#endif

static bool profiler_is_active(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void profiler_func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func, plch_fextra *fextra);
static void profiler_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func, plch_fextra *fextra);
static void profiler_func_abort(PLpgSQL_execstate *estate, PLpgSQL_function *func, plch_fextra *fextra);
static void profiler_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, plch_fextra *fextra);
static void profiler_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, plch_fextra *fextra);
static void profiler_stmt_abort(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, plch_fextra *fextra);

static plch_plugin profiler_plugin =
{
	.is_active = profiler_is_active,
	.func_setup = profiler_func_setup,
	.func_beg = NULL,
	.func_end = profiler_func_end,
	.func_abort = profiler_func_abort,
	.stmt_beg = profiler_stmt_beg,
	.stmt_end = profiler_stmt_end,
	.stmt_abort = profiler_stmt_abort
};

static void init_func_hashkey(func_hashkey *hk, Oid fn_oid);
static void plfunction_init_function_identity_hashkey(func_identity_hashkey *hk, PLpgSQL_function *func);
static void proctuple_init_function_identity_hashkey(func_identity_hashkey *hk, HeapTuple procTuple);

static pc_queryid profiler_get_dyn_queryid(PLpgSQL_execstate *estate, PLpgSQL_expr *expr, QParams *qparams);
static pc_queryid profiler_get_queryid(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt,
									   bool *has_queryid, QParams **qparams);

static void eval_stddev_accum(uint64 *_N, uint64 *_Sx, float8 *_Sxx, uint64 newval);
static PLpgSQL_function *cinfo_get_function(plpgsql_check_info *cinfo);
static bool get_plpgsql_expr_type(PLpgSQL_expr *expr, Oid *result_type);
static char *cutline(char *str, char **line);
static HTAB *assign_shared_htab(const char *tabname, Size keysize, Size Entrysize, int nelems);

static HTAB *func_stats_ht = NULL;
static HTAB *shared_func_stats_ht = NULL;
static HTAB *func_stmts_stats_ht = NULL;
static HTAB *shared_func_stmts_stats_ht = NULL;

static MemoryContext profiler_mcxt = NULL;
static MemoryContext profiler_queryid_mcxt = NULL;

static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

bool		plpgsql_check_profiler = false;
bool		plch_use_shared_stats_when_it_possible = true;
int			plch_max_stat_size = 20480;

static int used_stmt_stats_count = 0;
static int estimated_stmt_stats_count = 0;

static ProfilerSharedState *profiler_ss = NULL;
static StmtStats *SharedStmtStatsArray = NULL;

#define		USE_SHARED_FUNC_STATS			(shared_func_stats_ht && plch_use_shared_stats_when_it_possible)
#define		USE_SHARED_FUNC_STMTS_STATS		(shared_func_stmts_stats_ht && plch_use_shared_stats_when_it_possible)


/***************************************
 *
 * Profiler initialization
 *
 ***************************************
 */

/*
 * Calculate required size of shared memory for chunks
 *
 */
Size
plpgsql_check_shmem_size(void)
{
	Size		num_bytes = 0;

	num_bytes = MAXALIGN(sizeof(ProfilerSharedState));

	estimated_stmt_stats_count = plch_max_stat_size / sizeof(StmtStats);

	num_bytes = add_size(num_bytes, mul_size(estimated_stmt_stats_count, sizeof(StmtStats)));
	num_bytes = add_size(num_bytes, hash_estimate_size(20000, sizeof(FuncStmtsStats)));
	num_bytes = add_size(num_bytes, hash_estimate_size(20000, sizeof(FuncStats)));

	return num_bytes;
}

/*
 * Request additional shared memory resources.
 *
 * If you change code here, don't forget to also report the modifications in
 * _PG_init() for pg14 and below.
 */
#if PG_VERSION_NUM >= 150000

void
plpgsql_check_profiler_shmem_request(void)
{
	if (plpgsql_check_prev_shmem_request_hook)
		plpgsql_check_prev_shmem_request_hook();

	RequestAddinShmemSpace(plpgsql_check_shmem_size());

	RequestNamedLWLockTranche("plpgsql_check profiler funcs stats", 1);
	RequestNamedLWLockTranche("plpgsql_check profiler func stmts stats", 1);
}

#endif


/*
 * Initialize shared memory used like permanent profile storage.
 * No other parts use shared memory, so this code is completly here.
 *
 */
void
plpgsql_check_profiler_shmem_startup(void)
{
	bool		found;

	shared_func_stats_ht = NULL;

	if (plpgsql_check_prev_shmem_startup_hook)
		plpgsql_check_prev_shmem_startup_hook();

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	profiler_ss = ShmemInitStruct("plpgsql_check profiler state",
								  sizeof(ProfilerSharedState),
								  &found);

	if (!found)
	{
		profiler_ss->func_stats_lock = &(GetNamedLWLockTranche("plpgsql_check profiler funcs stats"))->lock;
		profiler_ss->func_stmts_stats_lock = &(GetNamedLWLockTranche("plpgsql_check profiler func stmts stats"))->lock;
		profiler_ss->stmt_stats_count = estimated_stmt_stats_count;
		profiler_ss->used_stmt_stats_count = 0;
	}

	SharedStmtStatsArray = ShmemInitStruct("plpgsql_check profiler shared statements stats",
										  mul_size(sizeof(StmtStats), estimated_stmt_stats_count),
										  &found);

	shared_func_stats_ht = assign_shared_htab("plpgsql_check profiler func stats",
											  sizeof(func_hashkey), sizeof(FuncStats),
											  FUNC_STATS_COUNT);

	shared_func_stmts_stats_ht = assign_shared_htab("plpgsql_check profiler func stmts stats",
													sizeof(func_identity_hashkey), sizeof(FuncStmtsStats),
													FUNC_STATS_COUNT);

	LWLockRelease(AddinShmemInitLock);
}

/*
 * Register plpgsql plugin2 for profiler
 */
void
plpgsql_check_profiler_init(void)
{
	plch_register_plugin(&profiler_plugin);
}


/***************************************
 *
 * Profiler controlling
 *
 ***************************************
 */

/*
 * Enable, disable, show state profiler
 */
Datum
plpgsql_check_profiler_ctrl(PG_FUNCTION_ARGS)
{
	char	   *optstr;
	bool		result;

#define OPTNAME		"plpgsql_check.profiler"

	if (!PG_ARGISNULL(0))
	{
		bool		optval = PG_GETARG_BOOL(0);

		(void) set_config_option(OPTNAME, optval ? "on" : "off",
								 (superuser() ? PGC_SUSET : PGC_USERSET),
								 PGC_S_SESSION, GUC_ACTION_SET,
								 true, 0, false);
	}

	optstr = GetConfigOptionByName(OPTNAME, NULL, false);

	if (strcmp(optstr, "on") == 0)
	{
		elog(NOTICE, "profiler is active");
		result = true;
	}
	else
	{
		elog(NOTICE, "profiler is not active");
		result = false;
	};

	if (PG_ARGISNULL(0))
	{
		if (result)
		{
			if (shared_func_stats_ht)
			{
				if (plch_use_shared_stats_when_it_possible)
					elog(NOTICE, "shared memory is preallocated and used now");
				else
					elog(NOTICE, "shared memory is preallocated and not used now");
			}
			else
				elog(NOTICE, "shared memory is not preallocated");
		}
		else
		{
			if (shared_func_stats_ht)
				elog(NOTICE, "shared memory is preallocated");
			else
				elog(NOTICE, "shared memory is not preallocated");
		}
	}

	PG_RETURN_BOOL(result);
}

/*
 * clean all chunks used by profiler
 */
Datum
plpgsql_profiler_reset_all(PG_FUNCTION_ARGS)
{
	/* be compiler quite */
	(void) fcinfo;

	if (shared_func_stats_ht)
	{
		HASH_SEQ_STATUS hash_seq;
		FuncStats *fs;

		LWLockAcquire(profiler_ss->func_stats_lock, LW_EXCLUSIVE);

		hash_seq_init(&hash_seq, shared_func_stats_ht);

		while ((fs = hash_seq_search(&hash_seq)) != NULL)
		{
			hash_search(shared_func_stats_ht,
						&(fs->key),
						HASH_REMOVE, NULL);
		}

		LWLockRelease(profiler_ss->func_stats_lock);
	}

	if (shared_func_stmts_stats_ht)
	{
		HASH_SEQ_STATUS hash_seq;
		FuncStmtsStats *fss;

		LWLockAcquire(profiler_ss->func_stmts_stats_lock, LW_EXCLUSIVE);

		hash_seq_init(&hash_seq, shared_func_stmts_stats_ht);

		while ((fss = hash_seq_search(&hash_seq)) != NULL)
		{
			hash_search(shared_func_stmts_stats_ht,
						&(fss->key),
						HASH_REMOVE, NULL);
		}

		profiler_ss->used_stmt_stats_count = 0;

		LWLockRelease(profiler_ss->func_stmts_stats_lock);
	}

	plch_profiler_init_local_hash_tables();

	PG_RETURN_VOID();
}

/*
 * Clean chunks related to some function
 */
Datum
plpgsql_profiler_reset(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	func_hashkey fhk;
	func_identity_hashkey hk;
	HeapTuple	procTuple;
	FuncStmtsStats *fss;
	bool		found;

	init_func_hashkey(&fhk, funcoid);
	hash_search(func_stats_ht, (void *) &fhk, HASH_REMOVE, NULL);

	if (shared_func_stats_ht)
	{
		LWLockAcquire(profiler_ss->func_stats_lock, LW_EXCLUSIVE);
		hash_search(shared_func_stats_ht, (void *) &fhk, HASH_REMOVE, NULL);
		LWLockRelease(profiler_ss->func_stats_lock);
	}

	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);

	proctuple_init_function_identity_hashkey(&hk, procTuple);

	fss = (FuncStmtsStats *) hash_search(func_stmts_stats_ht,
										 (void *) &hk,
										 HASH_FIND,
										 &found);

	if (found)
	{
		used_stmt_stats_count -= fss->nstatements;
		pfree(fss->sstats);
		hash_search(func_stmts_stats_ht, (void *) &hk, HASH_REMOVE, NULL);
	}

	if (shared_func_stmts_stats_ht)
	{
		/*
		 * In this case we doesn't release memory from SharedStmtStatsArray.
		 * Probably it is possible if we use DSA. But user friendly API for
		 * DSA is in Ph 18+. So DSA is not supported now.
		 */
		LWLockAcquire(profiler_ss->func_stmts_stats_lock, LW_EXCLUSIVE);
		hash_search(shared_func_stmts_stats_ht, (void *) &hk, HASH_REMOVE, NULL);
		LWLockRelease(profiler_ss->func_stmts_stats_lock);
	}

	ReleaseSysCache(procTuple);

	PG_RETURN_VOID();
}

Datum
plpgsql_profiler_install_fake_queryid_hook(PG_FUNCTION_ARGS)
{
	(void) fcinfo;

	if (post_parse_analyze_hook == profiler_fake_queryid_hook)
		PG_RETURN_VOID();

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

/*
 * Generate simple queryid  for testing purpose.
 * DO NOT USE IN PRODUCTION.
 */
#if PG_VERSION_NUM >= 190000

static void
profiler_fake_queryid_hook(ParseState *pstate, Query *query, const JumbleState *jstate)

#else

static void
profiler_fake_queryid_hook(ParseState *pstate, Query *query, JumbleState *jstate)

#endif

{
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/*
	 * force fake queryid
	 *
	 * profiler_fake_queryid_hook is active only for one plpgsql_check
	 * regress test. For this case, we can force fake queryid, althought
	 * is possible, so real queryid (computed by pg_stat_statements) is
	 * already computed.
	 *
	 * Because this fake queryid hook is designed only for one test,
	 * I don't check this possibility, and I don't raise any warning (because
	 * it breaks stability of plpgsql_check regress tests - in dependency
	 * on active (or non active) pg_stat_statements).
	 */
	query->queryId = query->commandType;
}


/***************************************
 *
 * Profiler hash tables
 *
 ***************************************
 */
static void
init_func_hashkey(func_hashkey *hk, Oid fn_oid)
{
	memset(hk, 0, sizeof(func_hashkey));

	hk->db_oid = MyDatabaseId;
	hk->fn_oid = fn_oid;
}

static void
func_stats_ht_init(void)
{
	HASHCTL		ctl;

	Assert(func_stats_ht == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(func_hashkey);
	ctl.entrysize = sizeof(FuncStats);
	ctl.hcxt = profiler_mcxt;
	func_stats_ht = hash_create("plpgsql_check function execution statistics",
								   FUNCS_PER_USER,
								   &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
plfunction_init_function_identity_hashkey(func_identity_hashkey *hk, PLpgSQL_function *func)
{
	memset(hk, 0, sizeof(func_identity_hashkey));

	hk->db_oid = MyDatabaseId;
	hk->fn_oid = func->fn_oid;

#if PG_VERSION_NUM >= 180000

	hk->fn_xmin = func->cfunc.fn_xmin;
	hk->fn_tid = func->cfunc.fn_tid;

#else

	hk->fn_xmin = func->fn_xmin;
	hk->fn_tid = func->fn_tid;

#endif

}

static void
proctuple_init_function_identity_hashkey(func_identity_hashkey *hk, HeapTuple procTuple)
{
	Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(procTuple);

	memset(hk, 0, sizeof(func_identity_hashkey));

	hk->db_oid = MyDatabaseId;
	hk->fn_oid = proc->oid;

	hk->fn_xmin = HeapTupleHeaderGetRawXmin(procTuple->t_data);
	hk->fn_tid = procTuple->t_self;

}

static void
func_stmts_stats_ht_init(void)
{
	HASHCTL		ctl;

	Assert(func_stmts_stats_ht == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(func_identity_hashkey);
	ctl.entrysize = sizeof(FuncStmtsStats);
	ctl.hcxt = profiler_mcxt;
	func_stmts_stats_ht = hash_create("plpgsql_check function execution statistics",
											 FUNCS_PER_USER,
											 &ctl,
											 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

}

void
plch_profiler_init_local_hash_tables(void)
{
	if (profiler_mcxt)
	{
		MemoryContextReset(profiler_mcxt);

		func_stats_ht = NULL;
		func_stmts_stats_ht = NULL;
	}
	else
	{
		profiler_mcxt = AllocSetContextCreate(TopMemoryContext,
											  "plpgsql_check - profiler context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);
	}

	func_stats_ht_init();
	func_stmts_stats_ht_init();

	used_stmt_stats_count = 0;
}


/***************************************
 *
 * Merging statistics to persistent memory
 *
 ***************************************
 */
static void
update_local_persistent_fstats(func_hashkey *hk,
							   uint64 elapsed,
							   bool aborted)
{
	FuncStats	   *fs;
	bool		found;

	fs = (FuncStats *) hash_search(func_stats_ht,
								   (void *) hk,
								   HASH_ENTER,
								   &found);

	if (!found)
	{
		fs = (FuncStats *) hash_search(func_stats_ht,
									   (void *) hk,
									   HASH_ENTER,
									   &found);

		if (!fs)
			elog(ERROR,
				 "cannot to insert new entry to profiler's function statistics");

		fs->exec_count = 0;
		fs->exec_count_err = 0;
		fs->total_time = 0;
		fs->total_time_xx = 0.0;
		fs->min_time = elapsed;
		fs->max_time = elapsed;
	}
	else
	{
		fs->min_time = fs->min_time < elapsed ? fs->min_time : elapsed;
		fs->max_time = fs->max_time > elapsed ? fs->max_time : elapsed;
	}

	eval_stddev_accum(&fs->exec_count,
					  &fs->total_time,
					  &fs->total_time_xx,
					  elapsed);

	if (aborted)
		fs->exec_count_err += 1;
}

static void
update_shared_persistent_fstats(func_hashkey *hk,
							   uint64 elapsed,
							   bool aborted)
{
	FuncStats	   *fs;
	bool		found;
	bool		unlock_mutex = false;

	LWLockAcquire(profiler_ss->func_stats_lock, LW_SHARED);

	fs = (FuncStats *) hash_search(shared_func_stats_ht,
								   (void *) hk,
								   HASH_FIND,
								   &found);

	if (!found)
	{
		LWLockRelease(profiler_ss->func_stats_lock);
		LWLockAcquire(profiler_ss->func_stats_lock, LW_EXCLUSIVE);

		fs = (FuncStats *) hash_search(shared_func_stats_ht,
									   (void *) hk,
									   HASH_ENTER,
									   &found);

		if (!fs)
			elog(ERROR,
				 "cannot to insert new entry to profiler's function statistics");
	}

	if (found)
	{
		SpinLockAcquire(&fs->mutex);
		unlock_mutex = true;

		fs->min_time = fs->min_time < elapsed ? fs->min_time : elapsed;
		fs->max_time = fs->max_time > elapsed ? fs->max_time : elapsed;
	}
	else
	{
		SpinLockInit(&fs->mutex);

		fs->exec_count = 0;
		fs->exec_count_err = 0;
		fs->total_time = 0;
		fs->total_time_xx = 0.0;
		fs->min_time = elapsed;
		fs->max_time = elapsed;
	}

	eval_stddev_accum(&fs->exec_count,
					  &fs->total_time,
					  &fs->total_time_xx,
					  elapsed);

	if (aborted)
		fs->exec_count_err += 1;

	if (unlock_mutex)
		SpinLockRelease(&fs->mutex);

	LWLockRelease(profiler_ss->func_stats_lock);
}

static void
update_persistent_fstats(PLpgSQL_function *func,
						 uint64 elapsed,
						 bool aborted)
{
	func_hashkey hk;

	init_func_hashkey(&hk, func->fn_oid);

	if (USE_SHARED_FUNC_STATS)
		update_shared_persistent_fstats(&hk, elapsed, aborted);
	else
		update_local_persistent_fstats(&hk, elapsed, aborted);
}

static void
update_local_persistent_stmts_stats(profiler_info *pinfo)
{
	func_identity_hashkey hk;
	FuncStmtsStats *fss;
	bool		found;
	int			i;

	plfunction_init_function_identity_hashkey(&hk, pinfo->func);

	/* don't need too strong lock for reading shared memory */
	fss = (FuncStmtsStats *) hash_search(func_stmts_stats_ht,
										 (void *) &hk,
										 HASH_ENTER,
										 &found);

	if (!found)
	{
		MemoryContext oldcxt;

		fss->nstatements = pinfo->func->nstatements;

		if (((used_stmt_stats_count + fss->nstatements) * sizeof(StmtStats)) > (plch_max_stat_size * 1024))
		{
			/* remove invalid current func_stmts_stats */
			hash_search(func_stmts_stats_ht, &(fss->key), HASH_REMOVE, NULL);

			ereport(WARNING,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("cannot allocate local memory for profiler statistics"),
					 errdetail("the used memory exceeds \"plpgsql_check.max_stats_size\" (%dkB)",
							   plch_max_stat_size),
					 errdetail("Statistics can be cleaned by calling function \"plpgsql_profiler_reset_all()\".")));

			return;
		}

		oldcxt = MemoryContextSwitchTo(profiler_mcxt);
		fss->sstats = palloc(pinfo->func->nstatements * sizeof(StmtStats));
		MemoryContextSwitchTo(oldcxt);

		memcpy(fss->sstats,
			   pinfo->sstats,
			   pinfo->func->nstatements * sizeof(StmtStats));

		fss->shared_sstats_offset = -1;

		used_stmt_stats_count += fss->nstatements;

		return;
	}

	/* merge new statistics with persistent statistics */
	for (i = 0; i < fss->nstatements; i++)
	{
		StmtStats  *persist_sstats = &fss->sstats[i];
		StmtStats  *sstats = &pinfo->sstats[i];

		persist_sstats->queryid = sstats->queryid;
		persist_sstats->has_queryid = sstats->has_queryid;

		if (persist_sstats->us_max < sstats->us_max)
			persist_sstats->us_max = sstats->us_max;

		persist_sstats->us_total += sstats->us_total;
		persist_sstats->rows += sstats->rows;
		persist_sstats->exec_count += sstats->exec_count;
		persist_sstats->exec_count_err += sstats->exec_count_err;
	}
}

static void
update_shared_persistent_stmts_stats(profiler_info *pinfo)
{
	func_identity_hashkey hk;
	FuncStmtsStats *fss;
	bool		found;
	int			i;

	plfunction_init_function_identity_hashkey(&hk, pinfo->func);

	LWLockAcquire(profiler_ss->func_stmts_stats_lock, LW_SHARED);

	/* don't need too strong lock for reading shared memory */
	fss = (FuncStmtsStats *) hash_search(shared_func_stmts_stats_ht,
										 (void *) &hk,
										 HASH_FIND,
										 &found);

	if (!found)
	{
		LWLockRelease(profiler_ss->func_stmts_stats_lock);
		LWLockAcquire(profiler_ss->func_stmts_stats_lock, LW_EXCLUSIVE);

		fss = (FuncStmtsStats *) hash_search(shared_func_stmts_stats_ht,
										(void *) &hk,
										HASH_ENTER,
										&found);

		if (!fss)
			elog(ERROR,
				 "cannot to insert new entry to profiler's function statements statistics");
	}

	if (!found)
	{
		if (profiler_ss->used_stmt_stats_count + pinfo->func->nstatements > profiler_ss->used_stmt_stats_count)
		{
			/* remove invalid func stmts stats entry */
			hash_search(shared_func_stmts_stats_ht, (void *) &(fss->key), HASH_REMOVE, NULL);

			ereport(WARNING,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("cannot use share memory for profiler statistics"),
					 errdetail("the used memory exceeds \"plpgsql_check.max_stats_size\" (%dkB)",
							   plch_max_stat_size),
					 errdetail("Statistics can be cleaned by calling function \"plpgsql_profiler_reset_all()\".")));
		}

		SpinLockInit(&fss->mutex);

		fss->sstats = NULL;
		fss->nstatements = pinfo->func->nstatements;
		fss->shared_sstats_offset = profiler_ss->used_stmt_stats_count;

		profiler_ss->used_stmt_stats_count += fss->nstatements;

		for (i = 0; i < fss->nstatements; i++)
		{
			int			offset = fss->shared_sstats_offset + i;

			StmtStats  *persist_sstats = &SharedStmtStatsArray[offset];
			StmtStats  *sstats = &pinfo->sstats[i];

			persist_sstats->queryid = sstats->queryid;
			persist_sstats->has_queryid = sstats->has_queryid;
			persist_sstats->us_max = sstats->us_max;
			persist_sstats->us_total = sstats->us_total;
			persist_sstats->rows = sstats->rows;
			persist_sstats->exec_count = sstats->exec_count;
			persist_sstats->exec_count_err = sstats->exec_count_err;
		}
	}
	else
	{
		SpinLockAcquire(&fss->mutex);

		for (i = 0; i < fss->nstatements; i++)
		{
			int			offset = fss->shared_sstats_offset + i;

			StmtStats  *persist_sstats = &SharedStmtStatsArray[offset];
			StmtStats  *sstats = &pinfo->sstats[i];

			persist_sstats->queryid = sstats->queryid;
			persist_sstats->has_queryid = sstats->has_queryid;

			if (persist_sstats->us_max < sstats->us_max)
				persist_sstats->us_max = sstats->us_max;

			persist_sstats->us_total += sstats->us_total;
			persist_sstats->rows += sstats->rows;
			persist_sstats->exec_count += sstats->exec_count;
			persist_sstats->exec_count_err += sstats->exec_count_err;
		}

		SpinLockRelease(&fss->mutex);
	}

	LWLockRelease(profiler_ss->func_stmts_stats_lock);
}

static void
update_persistent_stmts_stats(profiler_info *pinfo)
{
	if (USE_SHARED_FUNC_STMTS_STATS)
		update_shared_persistent_stmts_stats(pinfo);
	else
		update_local_persistent_stmts_stats(pinfo);
}

/*
 * Calculate statement statistics from statement instrumentation.
 * statement exec time is calculated as statement total exec time
 * minus total exec time of nestated statements.
 */
typedef struct count_stmt_exec_time_context
{
	StmtInstr  *sinstrs;
	StmtStats  *sstats;
	int64		nested_us_time;
} count_stmt_exec_time_context;

static void
count_stmt_exec_time_walker(PLpgSQL_stmt *stmt, count_stmt_exec_time_context *context)
{
	count_stmt_exec_time_context loccontext;

	int			stmtid_0 = stmt->stmtid - 1;
	StmtInstr  *sinstr = &context->sinstrs[stmtid_0];
	StmtStats  *sstats = &context->sstats[stmtid_0];

	loccontext.sinstrs = context->sinstrs;
	loccontext.sstats = context->sstats;
	loccontext.nested_us_time = 0;

	plch_statement_tree_walker(stmt, count_stmt_exec_time_walker, NULL, &loccontext);

	context->nested_us_time += sinstr->us_total;

	sstats->has_queryid = sinstr->has_queryid;
	sstats->queryid = sinstr->queryid;
	sstats->us_max = sinstr->us_max;
	sstats->us_total = sinstr->us_total - loccontext.nested_us_time;
	sstats->rows = sinstr->rows;
	sstats->exec_count = sinstr->exec_count;
	sstats->exec_count_err = sinstr->exec_count_err;
}


/***************************************
 *
 * Profiler pldbg3 api plugin
 *
 ***************************************
 */

/*
 * pldebug3 methods
 *
 */
static bool
profiler_is_active(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	return plpgsql_check_profiler;
}

static void
profiler_func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func, plch_fextra *fextra)
{
	if (OidIsValid(func->fn_oid))
	{
		profiler_info *pinfo;

		pinfo = palloc0(sizeof(profiler_info));
		pinfo->nstatements = func->nstatements;
		pinfo->sinstrs = palloc0(func->nstatements * sizeof(StmtInstr));
		pinfo->sstats = palloc0(func->nstatements * sizeof(StmtStats));

		INSTR_TIME_SET_CURRENT(pinfo->start_time);

		pinfo->func = func;
		estate->plugin_info = pinfo;
	}
}

static void
_profiler_func_end(profiler_info *pinfo, Oid fn_oid, bool is_aborted, plch_fextra *fextra)
{
	instr_time	end_time;
	uint64		elapsed;
	count_stmt_exec_time_context loccontext;

	Assert(pinfo);
	Assert(pinfo->func);
	Assert(pinfo->func->fn_oid == fn_oid);

	INSTR_TIME_SET_CURRENT(end_time);
	INSTR_TIME_SUBTRACT(end_time, pinfo->start_time);

	elapsed = INSTR_TIME_GET_MICROSEC(end_time);

	/* finalize profile - get result profile */
	loccontext.sinstrs = pinfo->sinstrs;
	loccontext.sstats = pinfo->sstats;
	loccontext.nested_us_time = 0;

	count_stmt_exec_time_walker((PLpgSQL_stmt *) pinfo->func->action, &loccontext);

	update_persistent_stmts_stats(pinfo);
	update_persistent_fstats(pinfo->func, elapsed, is_aborted);
}

static void
profiler_func_end(PLpgSQL_execstate *estate,
				  PLpgSQL_function *func,
				  plch_fextra *fextra)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	if (!pinfo)
		return;

	_profiler_func_end(pinfo, func->fn_oid, false, fextra);
}

static void
profiler_func_abort(PLpgSQL_execstate *estate,
				  PLpgSQL_function *func,
				  plch_fextra *fextra)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	if (!pinfo)
		return;

	_profiler_func_end(pinfo, func->fn_oid, true, fextra);
}

static void
profiler_stmt_beg(PLpgSQL_execstate *estate,
				  PLpgSQL_stmt *stmt,
				  plch_fextra *fextra)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	if (pinfo)
	{
		StmtInstr *sinstr = &pinfo->sinstrs[stmt->stmtid - 1];

		INSTR_TIME_SET_CURRENT(sinstr->start_time);
	}
}

static void
_profiler_stmt_end(StmtInstr *sinstr, bool is_aborted)
{
	instr_time	end_time;
	uint64		elapsed;
	instr_time	end_time2;

	INSTR_TIME_SET_CURRENT(end_time);
	end_time2 = end_time;
	INSTR_TIME_ACCUM_DIFF(sinstr->total, end_time, sinstr->start_time);

	INSTR_TIME_SUBTRACT(end_time2, sinstr->start_time);
	elapsed = INSTR_TIME_GET_MICROSEC(end_time2);

	if (elapsed > sinstr->us_max)
		sinstr->us_max = elapsed;

	sinstr->us_total = INSTR_TIME_GET_MICROSEC(sinstr->total);
	sinstr->exec_count_err += is_aborted ? 1 : 0;
	sinstr->exec_count++;
}

/*
 * Cleaning mode is used for closing unfinished statements after an exception.
 */
static void
profiler_stmt_end(PLpgSQL_execstate *estate,
				  PLpgSQL_stmt *stmt,
				  plch_fextra *fextra)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	if (pinfo)
	{
		StmtInstr *sinstr = &pinfo->sinstrs[stmt->stmtid - 1];

		/*
		 * We can get query id only if stmt_end is not executed in cleaning
		 * mode, because we need to execute expression
		 */
		if (sinstr->queryid == NOQUERYID)
			sinstr->queryid = profiler_get_queryid(estate, stmt,
												  &sinstr->has_queryid,
												  &sinstr->qparams);

		_profiler_stmt_end(sinstr, false);
	}
}

static void
profiler_stmt_abort(PLpgSQL_execstate *estate,
					PLpgSQL_stmt *stmt,
					plch_fextra *fextra)
{
	profiler_info *pinfo = (profiler_info *) estate->plugin_info;

	if (pinfo)
	{
		StmtInstr *sinstr = &pinfo->sinstrs[stmt->stmtid - 1];

		_profiler_stmt_end(sinstr, true);
	}
}


/***************************************
 *
 * queryid and fake queryid functions
 *
 ***************************************
 */

/* Return the first queryid found in the given PLpgSQL_stmt, if any. */
static pc_queryid
profiler_get_queryid(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt,
					 bool *has_queryid, QParams **qparams)
{
	PLpgSQL_expr *expr;
	bool		dynamic;
	List	   *params;
	List	   *plan_sources;

	expr = plch_statement_get_expr(stmt, &dynamic, &params, NULL);
	*has_queryid = (expr != NULL);

	/* fast leaving, when expression has not assigned plan */
	if (!expr || !expr->plan)
		return NOQUERYID;

	if (dynamic)
	{
		Assert(expr);

		if (params && !*qparams)
		{
			QParams *qps = NULL;
			int			nparams = list_length(params);
			int			paramno = 0;
			MemoryContext oldcxt;
			ListCell   *lc;

			/* build array of Oid used like dynamic query parameters */
			oldcxt = MemoryContextSwitchTo(profiler_mcxt);
			qps = (QParams *) palloc(sizeof(Oid) * nparams + sizeof(int));
			MemoryContextSwitchTo(oldcxt);

			foreach(lc, params)
			{
				PLpgSQL_expr *param_expr = (PLpgSQL_expr *) lfirst(lc);

				if (!get_plpgsql_expr_type(param_expr, &qps->paramtypes[paramno++]))
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
			Query	   *q = linitial_node(Query, plan_source->query_list);

			return q->queryId;
		}
	}

	return NOQUERYID;
}

static pc_queryid
profiler_get_dyn_queryid(PLpgSQL_execstate *estate, PLpgSQL_expr *expr, QParams *qparams)
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

	profiler_plugin.assign_expr(estate, (PLpgSQL_datum *) &result, expr);

	query_string = TextDatumGetCString(result.value);

	/*
	 * Do basic parsing of the query or queries (this should be safe even if
	 * we are in aborted transaction state!)
	 */
	parsetree_list = pg_parse_query(query_string);

	/* The query can be empty. Returns NOQUERYID is this case. */
	if (!parsetree_list)
	{
		MemoryContextSwitchTo(oldcxt);
		MemoryContextReset(profiler_queryid_mcxt);
		return NOQUERYID;
	}

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

	query = parse_analyze_fixedparams(parsetree, query_string, paramtypes, nparams, NULL);

	if (snapshot_set)
		PopActiveSnapshot();

	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(profiler_queryid_mcxt);

	return query->queryId;
}


/***************************************
 *
 * Reporting
 *
 ***************************************
 */
static void
local_iterate_over_all_profiles(plpgsql_check_result_info *ri)
{
	HASH_SEQ_STATUS seqstatus;
	FuncStats	   *fs;

	hash_seq_init(&seqstatus, func_stats_ht);

	while ((fs = (FuncStats *) hash_seq_search(&seqstatus)) != NULL)
	{
		HeapTuple	tp;

		/* skip dropped functions */
		tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(fs->key.fn_oid));
		if (!HeapTupleIsValid(tp))
			continue;

		ReleaseSysCache(tp);

		plpgsql_check_put_profiler_functions_all_tb(ri,
													fs->key.fn_oid,
													fs->exec_count,
													fs->exec_count_err,
													(double) fs->total_time,
													ceil(fs->total_time / ((double) fs->exec_count)),
													ceil(sqrt(fs->total_time_xx / fs->exec_count)),
													(double) fs->min_time,
													(double) fs->max_time);
	}
}

static void
shared_iterate_over_all_profiles(plpgsql_check_result_info *ri)
{
	HASH_SEQ_STATUS seqstatus;
	FuncStats	   *fs;

	LWLockAcquire(profiler_ss->func_stats_lock, LW_SHARED);

	hash_seq_init(&seqstatus, shared_func_stats_ht);

	while ((fs = (FuncStats *) hash_seq_search(&seqstatus)) != NULL)
	{
		HeapTuple	tp;

		/*
		 * only function's statistics for current database can be displayed
		 * here, Oid of functions from other databases has unassigned oids to
		 * current system catalogue.
		 */
		if (fs->key.db_oid != MyDatabaseId)
			continue;

		SpinLockAcquire(&fs->mutex);

		PG_TRY();
		{
			/* check if function still exists */
			tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(fs->key.fn_oid));
			if (HeapTupleIsValid(tp))
			{
				plpgsql_check_put_profiler_functions_all_tb(ri,
															fs->key.fn_oid,
															fs->exec_count,
															fs->exec_count_err,
															(double) fs->total_time,
															ceil(fs->total_time / ((double) fs->exec_count)),
															ceil(sqrt(fs->total_time_xx / fs->exec_count)),
															(double) fs->min_time,
															(double) fs->max_time);
			}

			ReleaseSysCache(tp);
		}
		PG_CATCH();
		{
			SpinLockRelease(&fs->mutex);
			PG_RE_THROW();
		}
		PG_END_TRY();

		SpinLockRelease(&fs->mutex);
	}

	LWLockRelease(profiler_ss->func_stats_lock);
}

/*
 * Reports all persistent function's statistics
 */
void
plpgsql_check_profiler_iterate_over_all_profiles(plpgsql_check_result_info *ri)
{
	if (USE_SHARED_FUNC_STATS)
		shared_iterate_over_all_profiles(ri);
	else
		local_iterate_over_all_profiles(ri);
}


/*
 * Working horse for  plch_statements_stats_report.
 * Wrapped by SQL  plpgsql_profiler_function_statements_tb function
 *
 * Iterate over statement tree and fill tuplestore
 *
 * naturalid is calculated from scratch, because we want to remove
 * naturalid from fextra
 */
typedef struct statement_stats_report_context
{
	plpgsql_check_result_info *ri;
	plch_fextra *fextra;
	StmtStats  *sstats;
	PLpgSQL_stmt *parent_stmt;
	int			stmt_counter;		/* used for block_num */
} statement_stats_report_context;

static void
statement_stats_report_walker(PLpgSQL_stmt *stmt, statement_stats_report_context *context)
{
	statement_stats_report_context loccontext;
	
	int		   *naturalids = context->fextra->naturalids;

	if (stmt->lineno > 0)
	{
		int			parent_natural_stmtid = -1;

		if (context->parent_stmt && context->parent_stmt->lineno > 0)
			parent_natural_stmtid = naturalids[context->parent_stmt->stmtid];
		else
			parent_natural_stmtid = -1;

		if (context->sstats)
		{
			StmtStats  *sstats = &context->sstats[stmt->stmtid - 1];

			plpgsql_check_put_profile_statement(context->ri,
											    sstats->queryid,
											    naturalids[stmt->stmtid],
											    parent_natural_stmtid,
											    context->stmt_counter++,
											    stmt->lineno,
											    sstats->exec_count,
											    sstats->exec_count_err,
											    sstats->us_total,
											    sstats->us_max,
											    sstats->rows,
											    plpgsql_check__stmt_typename_p(stmt));
		}
		else
		{
			plpgsql_check_put_profile_statement_no_stats(context->ri,
														 naturalids[stmt->stmtid],
														 parent_natural_stmtid,
														 context->stmt_counter++,
														 stmt->lineno,
														 plpgsql_check__stmt_typename_p(stmt));
		}
	}

	loccontext.ri = context->ri;
	loccontext.fextra = context->fextra;
	loccontext.sstats = context->sstats;
	loccontext.parent_stmt = stmt;
	loccontext.stmt_counter = 1;

	plch_statement_tree_walker(stmt, statement_stats_report_walker, NULL, &loccontext);
}

/*
 * the work with local statistics are primitive, we don't need to play with locks
 */
static void
local_statements_stats_report(func_identity_hashkey *hk,
							  PLpgSQL_stmt *stmt,
							  statement_stats_report_context *context)
{
	FuncStmtsStats *fss;
	bool		found;

	fss = (FuncStmtsStats *) hash_search(func_stmts_stats_ht,
										 (void *) hk,
										 HASH_FIND,
										 &found);

	if (found)
		context->sstats = fss->sstats;

	statement_stats_report_walker(stmt, context);
}

/*
 * shared variant is more complex, we need to protect against race condition
 */
static void
shared_statements_stats_report(func_identity_hashkey *hk,
							   PLpgSQL_stmt *stmt,
							   statement_stats_report_context *context)
{
	FuncStmtsStats *fss;
	bool		found;

	LWLockAcquire(profiler_ss->func_stmts_stats_lock, LW_SHARED);

	fss = (FuncStmtsStats *) hash_search(shared_func_stmts_stats_ht,
										 (void *) hk,
										 HASH_FIND,
										 &found);

	if (found)
	{
		SpinLockAcquire(&fss->mutex);
		context->sstats = &SharedStmtStatsArray[fss->shared_sstats_offset];
	}

	PG_TRY();
	{
		statement_stats_report_walker(stmt, context);
	}
	PG_CATCH();
	{
		if (found)
			SpinLockRelease(&fss->mutex);

		PG_RE_THROW();
	}
	PG_END_TRY();

	if (found)
		SpinLockRelease(&fss->mutex);

	LWLockRelease(profiler_ss->func_stmts_stats_lock);
}

/*
 * Build simple statement statistics report (statement profile)
 *
 * This is simple report - one statement, one line (and then there is not any
 * cummulation per lines).
 */
void
plch_statements_stats_report(plpgsql_check_info *cinfo,
							 plpgsql_check_result_info *ri)
{
	statement_stats_report_context loccontext;
	func_identity_hashkey hk;
	PLpgSQL_function *func;
	PLpgSQL_stmt *stmt;

	func = cinfo_get_function(cinfo);
	plfunction_init_function_identity_hashkey(&hk, func);

	loccontext.ri = ri;
	loccontext.fextra = plch_get_fextra(func);
	loccontext.parent_stmt = NULL;
	loccontext.stmt_counter = 1;
	loccontext.sstats = NULL;

	stmt = (PLpgSQL_stmt *) func->action;

	PG_TRY();
	{
		if (USE_SHARED_FUNC_STMTS_STATS)
			shared_statements_stats_report(&hk, stmt, &loccontext);
		else
			local_statements_stats_report(&hk, stmt, &loccontext);
	}
	PG_CATCH();
	{
		plch_release_fextra(loccontext.fextra);
		PG_RE_THROW();
	}
	PG_END_TRY();

	plch_release_fextra(loccontext.fextra);
}

/*
 * Profiler report is more terrible becase maps statements to lines, and because
 * more statements can be placed on one line, then some stats are merged in an arrays.
 * Next complication is synchronization reported stats with function source code.
 */
typedef struct profiler_report_context
{
	plpgsql_check_result_info *ri;
	StmtStats  *sstats;
	MemoryContext tmp_cxt;
	ArrayBuildState *queryids_abs;
	ArrayBuildState *exec_count_abs;
	ArrayBuildState *exec_count_err_abs;
	ArrayBuildState *total_time_abs;
	ArrayBuildState *avg_time_abs;
	ArrayBuildState *max_time_abs;
	ArrayBuildState *processed_rows_abs;
	bool		queryid_on_row;
	int			buffered_stmt_lineno;
	int			lineno;
	int			cmds_on_row;
	char	   *src;
} profiler_report_context;

/*
 * Makes arrays from builders, push row and cleans context
 */
static void
put_profiler_report_row(profiler_report_context *context)
{
	char	   *srcrow = NULL;

	if (context->buffered_stmt_lineno < 0)
		return;

	while (context->lineno < context->buffered_stmt_lineno)
	{
		context->src = cutline(context->src, &srcrow);
		context->lineno++;

		if (context->lineno < context->buffered_stmt_lineno)
		{
			plpgsql_check_put_profile(context->ri,
									  (Datum) 0, context->lineno, -1, -1, (Datum) 0, (Datum) 0,
									  (Datum) 0, (Datum) 0, (Datum) 0, (Datum) 0, srcrow);
		}
	}

#define NullableArray(context, abs) \
	(((context)->abs) ? makeArrayResult((context)->abs, (context)->tmp_cxt) : (Datum) 0)

	if (context->cmds_on_row > 0)
	{
		plpgsql_check_put_profile(context->ri,
								  NullableArray(context, queryids_abs),
								  context->lineno,
								  context->buffered_stmt_lineno,
								  context->cmds_on_row,
								  NullableArray(context, exec_count_abs),
								  NullableArray(context, exec_count_err_abs),
								  NullableArray(context, total_time_abs),
								  NullableArray(context, avg_time_abs),
								  NullableArray(context, max_time_abs),
								  NullableArray(context, processed_rows_abs),
								  srcrow);

		MemoryContextReset(context->tmp_cxt);

		context->queryids_abs = NULL;
		context->exec_count_abs = NULL;
		context->exec_count_err_abs = NULL;
		context->total_time_abs = NULL;
		context->avg_time_abs = NULL;
		context->max_time_abs = NULL;
		context->processed_rows_abs = NULL;
		context->buffered_stmt_lineno = -1;
		context->cmds_on_row = 0;
	}
	else
	{
		plpgsql_check_put_profile(context->ri,
								  (Datum) 0, context->lineno, -1, -1, (Datum) 0, (Datum) 0,
								  (Datum) 0, (Datum) 0, (Datum) 0, (Datum) 0, srcrow);
	}
}

static void
profiler_report_walker(PLpgSQL_stmt *stmt, profiler_report_context *context)
{
	if (stmt->lineno > 0 && stmt->lineno != context->buffered_stmt_lineno)
	{
		put_profiler_report_row(context);
		context->buffered_stmt_lineno = stmt->lineno;
	}

#define accum_int64(context, abs, value)	accumArrayResult(context->abs, \
															 Int64GetDatum((int64) (value)), \
															 false, \
															 INT8OID, \
															 context->tmp_cxt)

#define accum_float8(context, abs, value)	accumArrayResult(context->abs, \
															 Float8GetDatum((value)), \
															 false, \
															 FLOAT8OID, \
															 context->tmp_cxt)

	if (stmt->lineno > 0)
	{
		context->cmds_on_row += 1;

		if (context->sstats)
		{
			StmtStats  *sstats = &context->sstats[stmt->stmtid-1];
			uint64		total_exec_count;
			float8		avg_time_us;

			if (sstats->has_queryid)
				context->queryids_abs = accumArrayResult(context->queryids_abs,
														 Int64GetDatum((int64) sstats->queryid),
														 sstats->queryid == NOQUERYID,
														 INT8OID,
														 context->tmp_cxt);

			context->exec_count_abs = accum_int64(context, exec_count_abs, sstats->exec_count);
			context->exec_count_err_abs = accum_int64(context, exec_count_err_abs, sstats->exec_count_err);

			context->total_time_abs = accum_float8(context, total_time_abs, sstats->us_total / 1000.0);

			total_exec_count = sstats->exec_count + sstats->exec_count_err;
			if (total_exec_count > 0)
				avg_time_us = ceil(((float8) sstats->us_total) / total_exec_count);
			else
				avg_time_us = 0.0;

			context->avg_time_abs = accum_float8(context, avg_time_abs, avg_time_us / 1000.0);
			context->max_time_abs = accum_float8(context, max_time_abs, sstats->us_max / 1000.0);
			context->processed_rows_abs = accum_int64(context, processed_rows_abs, sstats->rows);
		}
	}

	plch_statement_tree_walker(stmt, profiler_report_walker, NULL, context);
}

/*
 * Common part for reporting local or shared profile
 */
static void
show_profile(PLpgSQL_function *func, profiler_report_context *context)
{
	profiler_report_walker((PLpgSQL_stmt *) func->action, context);

	/* send unsent buffered statistics */
	put_profiler_report_row(context);

	/* send unprocessed source code rows */
	if (context->src)
	{
		char	   *srcrow;

		while (context->src)
		{
			context->src = cutline(context->src, &srcrow);
			context->lineno++;
			plpgsql_check_put_profile(context->ri,
									  (Datum) 0, context->lineno, -1, -1, (Datum) 0, (Datum) 0,
									  (Datum) 0, (Datum) 0, (Datum) 0, (Datum) 0, srcrow);
		}
	}
}

/*
 * simple - no locks for access to local stats
 */
static void
show_local_profile(PLpgSQL_function *func,
				   func_identity_hashkey *hk,
				   profiler_report_context *context)
{
	FuncStmtsStats *fss;
	bool		found;

	fss = (FuncStmtsStats *) hash_search(func_stmts_stats_ht,
										 (void *) hk,
										 HASH_FIND,
										 &found);

	if (found)
		context->sstats = fss->sstats;

	show_profile(func, context);
}

static void
show_shared_profile(PLpgSQL_function *func,
				    func_identity_hashkey *hk,
				    profiler_report_context *context)
{
	FuncStmtsStats *fss;
	bool		found;

	LWLockAcquire(profiler_ss->func_stmts_stats_lock, LW_SHARED);

	fss = (FuncStmtsStats *) hash_search(shared_func_stmts_stats_ht,
										 (void *) hk,
										 HASH_FIND,
										 &found);

	if (found)
	{
		SpinLockAcquire(&fss->mutex);
		context->sstats = &SharedStmtStatsArray[fss->shared_sstats_offset];
	}

	PG_TRY();
	{
		show_profile(func, context);
	}
	PG_CATCH();
	{
		if (found)
			SpinLockRelease(&fss->mutex);

		PG_RE_THROW();
	}
	PG_END_TRY();

	if (found)
		SpinLockRelease(&fss->mutex);

	LWLockRelease(profiler_ss->func_stmts_stats_lock);
}

/*
 * Prepare tuplestore with function profile
 *
 */
void
plpgsql_check_profiler_show_profile(plpgsql_check_result_info *ri,
									plpgsql_check_info *cinfo)
{
	profiler_report_context loccontext;
	func_identity_hashkey hk;
	PLpgSQL_function *func;

	memset(&loccontext, 0, sizeof(profiler_report_context));

	loccontext.ri = ri;
	loccontext.tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
												"profiler report temporary cxt",
												ALLOCSET_DEFAULT_SIZES);
	loccontext.buffered_stmt_lineno = -1;

	/* attention cinfo->src will be modified */
	loccontext.src = cinfo->src;

	func = cinfo_get_function(cinfo);
	plfunction_init_function_identity_hashkey(&hk, func);

	if (USE_SHARED_FUNC_STMTS_STATS)
		show_shared_profile(func, &hk, &loccontext);
	else
		show_local_profile(func, &hk, &loccontext);

	MemoryContextDelete(loccontext.tmp_cxt);
}


/***************************************
 
 * Coverage calculation
 *
 ***************************************
 */
typedef struct coverage_statements_context
{
	int			nstatements;
	int			nexecuted_statements;
	StmtStats  *sstats;
} coverage_statements_context;

/*
 * Coverage statement metric is simple, just count statements and executed
 * statements.
 */
static void
coverage_statements_walker(PLpgSQL_stmt *stmt, coverage_statements_context *context)
{
	/* calculate only visible statements */
	if (stmt->lineno > 0)
	{
		context->nstatements += 1;

		if (context->sstats && context->sstats[stmt->stmtid -1].exec_count > 0)
			context->nexecuted_statements += 1;
	}

	plch_statement_tree_walker(stmt, coverage_statements_walker, NULL, context);
}

/*
 * Coverage branches is more difficult, becase we have not branch walker.
 * Any branch is a list of statements, and we can check of number of execution
 * of first statement in the list.
 */
typedef struct coverage_branches_context
{
	int			nbranches;
	int			nexecuted_branches;
	StmtStats *sstats;
} coverage_branches_context;

static void
increment_branch_counters(coverage_branches_context *context, List *stmts,
						  int64 *sum_exec_count)
{
	context->nbranches += 1;

	if (stmts && context->sstats)
	{
		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *) linitial(stmts);
		int64 exec_count = context->sstats[stmt->stmtid - 1].exec_count;

		if (exec_count > 0)
			context->nexecuted_branches += 1;

		if (sum_exec_count)
			*sum_exec_count += exec_count;
	}
}

static void
coverage_branches_walker(PLpgSQL_stmt *stmt, coverage_branches_context *context)
{
	List	   *stmts;
	ListCell   *lc;

#define IS_PLPGSQL_STMT(stmt, typ)		(stmt->cmd_type == typ)

	if (plch_statement_is_loop(stmt))
	{
		stmts = plch_statement_get_loop_body(stmt);
		increment_branch_counters(context, stmts, NULL);
	}
	else if (IS_PLPGSQL_STMT(stmt, PLPGSQL_STMT_IF))
	{
		PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;
		int64		sum_exec_count = 0;

		increment_branch_counters(context, stmt_if->then_body, &sum_exec_count);

		foreach(lc, stmt_if->elsif_list)
		{
			stmts = ((PLpgSQL_if_elsif *) lfirst(lc))->stmts;

			increment_branch_counters(context, stmts, &sum_exec_count);
		}

		if (stmt_if->else_body)
		{
			increment_branch_counters(context, stmt_if->else_body, NULL);
		}
		else
		{
			context->nbranches += 1;

			/*
			 * When IF has not ELSE branch, we have to calculate it with
			 * hypothetical else branch. In this case we have a little problem
			 * how to detect if this branch was executed. We can derived it
			 * from IF statements execution and all real branch execution.
			 */
			if (context->sstats)
			{
				int64		hyp_exec_count;

				hyp_exec_count = context->sstats[stmt->stmtid - 1].exec_count - sum_exec_count;

				if (hyp_exec_count > 0)
					context->nexecuted_branches += 1;
			}
		}
	}
	else if (IS_PLPGSQL_STMT(stmt, PLPGSQL_STMT_CASE))
	{
		PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;

		foreach(lc, stmt_case->case_when_list)
		{
			stmts = ((PLpgSQL_case_when *) lfirst(lc))->stmts;
			increment_branch_counters(context, stmts, NULL);
		}

		increment_branch_counters(context, stmt_case->else_stmts, NULL);

		/*
		 * CASE has not hypothetical else branch. In this case
		 * an exception is raised.
		 */
	}
	else if (IS_PLPGSQL_STMT(stmt, PLPGSQL_STMT_BLOCK))
	{
		PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) stmt;

		if (stmt_block->exceptions)
		{
			/*
			 * block without exception handling doesn't make branches,
			 * in this case the check of execution of first statement
			 * from the list is maybe not accurate - but it is consistent
			 * with the variant without the exception handling. We check
			 * only first statement.
			 */
			increment_branch_counters(context, stmt_block->body, NULL);

			foreach(lc, stmt_block->exceptions->exc_list)
			{
				stmts = ((PLpgSQL_exception *) lfirst(lc))->action;
				increment_branch_counters(context, stmts, NULL);
			}
		}
	}

	plch_statement_tree_walker(stmt, coverage_branches_walker, NULL, context);
}

static double
coverage_compute(PLpgSQL_stmt *stmt, StmtStats *sstats, int ct)
{
	if (ct == COVERAGE_STATEMENTS)
	{
		coverage_statements_context loccontext;

		loccontext.nstatements = 0;
		loccontext.nexecuted_statements = 0;
		loccontext.sstats = sstats;

		coverage_statements_walker(stmt, &loccontext);

		if (loccontext.nstatements > 0)
			return (double) loccontext.nexecuted_statements / (double) loccontext.nstatements;
		else
			return (double) 1.0;
	}
	else
	{
		coverage_branches_context loccontext;

		loccontext.nbranches = 0;
		loccontext.nexecuted_branches = 0;
		loccontext.sstats = sstats;

		coverage_branches_walker(stmt, &loccontext);

		if (loccontext.nbranches > 0)
			return (double) loccontext.nexecuted_branches / (double) loccontext.nbranches;
		else
			return (double) 1.0;
	}
}

static double
local_coverage_compute(func_identity_hashkey *hk, PLpgSQL_stmt *stmt, int ct)
{
	FuncStmtsStats *fss;
	bool		found;

	fss = (FuncStmtsStats *) hash_search(func_stmts_stats_ht,
										 (void *) hk,
										 HASH_FIND,
										 &found);

	return coverage_compute(stmt, found ? fss->sstats : NULL, ct);
}

static double
shared_coverage_compute(func_identity_hashkey *hk, PLpgSQL_stmt *stmt, int ct)
{
	FuncStmtsStats *fss;
	StmtStats *sstats = NULL;
	bool		found;
	double		result;

	LWLockAcquire(profiler_ss->func_stmts_stats_lock, LW_SHARED);

	fss = (FuncStmtsStats *) hash_search(shared_func_stmts_stats_ht,
										 (void *) hk,
										 HASH_FIND,
										 &found);

	if (found)
	{
		SpinLockAcquire(&fss->mutex);
		sstats = &SharedStmtStatsArray[fss->shared_sstats_offset];
	}

	PG_TRY();
	{
		result = coverage_compute(stmt, sstats, ct);
	}
	PG_CATCH();
	{
		if (found)
			SpinLockRelease(&fss->mutex);

		PG_RE_THROW();
	}
	PG_END_TRY();

	if (found)
		SpinLockRelease(&fss->mutex);

	LWLockRelease(profiler_ss->func_stmts_stats_lock);

	return result;
}

/*
 * Prepare environment for reading profile and calculation of coverage metric
 */
static double
coverage_internal(Oid fnoid, int ct)
{
	plpgsql_check_info cinfo;
	PLpgSQL_function *func;
	func_identity_hashkey hk;

	plpgsql_check_info_init(&cinfo, fnoid);

	cinfo.proctuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(cinfo.fn_oid));
	if (!HeapTupleIsValid(cinfo.proctuple))
		elog(ERROR, "cache lookup failed for function %u", cinfo.fn_oid);

	plpgsql_check_get_function_info(&cinfo);
	plpgsql_check_precheck_conditions(&cinfo);

	func = cinfo_get_function(&cinfo);

	ReleaseSysCache(cinfo.proctuple);

	plfunction_init_function_identity_hashkey(&hk, func);

	if (USE_SHARED_FUNC_STMTS_STATS)
		return shared_coverage_compute(&hk, (PLpgSQL_stmt *) func->action, ct);
	else
		return local_coverage_compute(&hk, (PLpgSQL_stmt *) func->action, ct);
}

Datum
plpgsql_coverage_statements_name(PG_FUNCTION_ARGS)
{
	Oid			fnoid;
	char	   *name_or_signature;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	name_or_signature = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fnoid = plpgsql_check_parse_name_or_signature(name_or_signature);

	PG_RETURN_FLOAT8(coverage_internal(fnoid, COVERAGE_STATEMENTS));
}

Datum
plpgsql_coverage_branches_name(PG_FUNCTION_ARGS)
{
	Oid			fnoid;
	char	   *name_or_signature;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	name_or_signature = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fnoid = plpgsql_check_parse_name_or_signature(name_or_signature);

	PG_RETURN_FLOAT8(coverage_internal(fnoid, COVERAGE_BRANCHES));
}

Datum
plpgsql_coverage_statements(PG_FUNCTION_ARGS)
{
	Oid			fnoid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	fnoid = PG_GETARG_OID(0);

	PG_RETURN_FLOAT8(coverage_internal(fnoid, COVERAGE_STATEMENTS));
}

Datum
plpgsql_coverage_branches(PG_FUNCTION_ARGS)
{
	Oid			fnoid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	fnoid = PG_GETARG_OID(0);

	PG_RETURN_FLOAT8(coverage_internal(fnoid, COVERAGE_BRANCHES));
}

/***************************************
 *
 * Helper functionality
 *
 ***************************************
 */

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
		tmp = ((float8) newval) * ((float8) N) - ((float8) Sx);

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
 * helper function that returns compiled statement tree
 */
static PLpgSQL_function *
cinfo_get_function(plpgsql_check_info *cinfo)
{
	LOCAL_FCINFO(fake_fcinfo, 0);
	FmgrInfo	flinfo;
	TriggerData trigdata;
	EventTriggerData etrigdata;
	Trigger		tg_trigger;
	ReturnSetInfo rsinfo;
	bool		fake_rtd;

	plpgsql_check_setup_fcinfo(cinfo,
							   &flinfo,
							   fake_fcinfo,
							   &rsinfo,
							   &trigdata,
							   &etrigdata,
							   &tg_trigger,
							   &fake_rtd);

	return plpgsql_check__compile_p(fake_fcinfo, false);
}

/*
 * Assigned result type of already executed (has assigned plan) expression.
 *
 * Returns false when expression was not executed or when plpgsql expression
 * is not an expression (has more plans or returns more columns).
 */
static bool
get_plpgsql_expr_type(PLpgSQL_expr *expr, Oid *result_type)
{
	if (expr)
	{
		SPIPlanPtr	ptr = expr->plan;

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

/*
 * returns pointer in to next line in text or null, when there is not next line.
 * It modify input text.
 */
static char *
cutline(char *str, char **line)
{
	if (!str)
	{
		*line = NULL;

		return NULL;
	}

	*line = str;

	if (*str)
	{
		char	   *ptr = str;

		while (*ptr != '\0' && *ptr != '\n')
			ptr++;

		if (*ptr == '\n')
		{
			*ptr++ = '\0';

			/*
			 * ignore last empty rows, reduce garbage row. We cannot
			 * to skip first usuall empty row, because we don't want
			 * to break relation between lineno and statement's lineno.
			 * Last empty line we can throw without any impact.
			 */
			if (*ptr == '\0')
				return NULL;

			return ptr;
		}
	}

	return NULL;
}

static HTAB *
assign_shared_htab(const char *tabname,
			Size keysize,
			Size entrysize,
			int nelems)
{
	HASHCTL		info;

	memset(&info, 0, sizeof(info));
	info.keysize = keysize;
	info.entrysize = entrysize;


#if PG_VERSION_NUM >= 190000

	return ShmemInitHash(tabname, nelems, &info, HASH_ELEM | HASH_BLOBS);

#else

	return ShmemInitHash(tabname, nelems, nelems, &info,  HASH_ELEM | HASH_BLOBS);

#endif

}
