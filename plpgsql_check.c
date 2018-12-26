/*-------------------------------------------------------------------------
 *
 * plpgsql_check.c
 *
 *			  enhanced checks for plpgsql functions
 *
 * by Pavel Stehule 2013-2018
 *
 *-------------------------------------------------------------------------
 *
 * Notes:
 *
 * 1) Secondary hash table for function signature is necessary due holding is_checked
 *    attribute - this protection against unwanted repeated check.
 *
 * 2) Reusing some plpgsql_xxx functions requires full run-time environment. It is
 *    emulated by fake expression context and fake fceinfo (these are created when
 *    active checking is used) - see: setup_fake_fcinfo, setup_cstate.
 *
 * 3) The environment is referenced by stored execution plans. The actual plan should
 *    not be linked with fake environment. All expressions created in checking time
 *    should be relased by release_exprs(cstate.exprs) function.
 *
 */
#include "postgres.h"

#include "plpgsql.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "math.h"

#include "plpgsql_check_builtins.h"

#if PG_VERSION_NUM >= 110000

#include "utils/expandedrecord.h"

#endif

#if PG_VERSION_NUM >= 100000

#include "utils/regproc.h"

#endif

#if PG_VERSION_NUM < 110000

#include "storage/spin.h"

#endif


#include "access/htup_details.h"
#include "access/tupconvert.h"
#include "access/tupdesc.h"

#ifndef TupleDescAttr
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/spi_priv.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "tsearch/ts_locale.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/rel.h"
#include "utils/json.h"
#include "utils/reltrigger.h"
#include "utils/xml.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif




enum
{
	PLPGSQL_CHECK_MODE_DISABLED,		/* all functionality is disabled */
	PLPGSQL_CHECK_MODE_BY_FUNCTION,		/* checking is allowed via CHECK function only (default) */
	PLPGSQL_CHECK_MODE_FRESH_START,		/* check only when function is called first time */
	PLPGSQL_CHECK_MODE_EVERY_START		/* check on every start */
};

typedef struct PLpgSQL_stmt_stack_item
{
	PLpgSQL_stmt			*stmt;
	char				*label;
	struct PLpgSQL_stmt_stack_item	*outer;
} PLpgSQL_stmt_stack_item;


static void assign_tupdesc_dno(PLpgSQL_checkstate *cstate, int varno, TupleDesc tupdesc, bool isnull);
static void assign_tupdesc_row_or_rec(PLpgSQL_checkstate *cstate,
						  PLpgSQL_row *row, PLpgSQL_rec *rec,
						  TupleDesc tupdesc, bool isnull);
static void check_assignment(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
				 PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
				 int targetdno);
static void check_assignment_with_possible_slices(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
						 PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
						 int targetdno, bool use_element_type);
static void check_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
static void check_expr_with_expected_scalar_type(PLpgSQL_checkstate *cstate,
								 PLpgSQL_expr *expr,
								 Oid expected_typoid,
								 bool required);
static void check_expr_as_rvalue(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
					  PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
				   int targetdno, bool use_element_type, bool is_expression);
static void check_returned_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool is_expression);
static void check_expr_as_sqlstmt_nodata(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
static void check_expr_as_sqlstmt_data(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
static bool check_expr_as_sqlstmt(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
static void check_assign_to_target_type(PLpgSQL_checkstate *cstate,
							 Oid target_typoid, int32 target_typmod,
							 Oid value_typoid,
									    bool isnull);
static void check_function_epilog(PLpgSQL_checkstate *cstate);
static void check_function_prolog(PLpgSQL_checkstate *cstate);
static void check_on_func_beg(PLpgSQL_execstate * estate, PLpgSQL_function * func);
static void check_plpgsql_function(HeapTuple procTuple, Oid relid, PLpgSQL_trigtype trigtype,
					   TupleDesc tupdesc,
					   Tuplestorestate *tupstore,
							    int format,
									  bool fatal_errors,
									  bool other_warnings,
									  bool performance_warnings,
									  bool extra_warnings);
static void check_row_or_rec(PLpgSQL_checkstate *cstate, PLpgSQL_row *row, PLpgSQL_rec *rec);

#if PG_VERSION_NUM >= 110000

static void check_variable(PLpgSQL_checkstate *cstate, PLpgSQL_variable *var);
static void check_assignment_to_variable(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
				 PLpgSQL_variable *var, int targetdno);

#define get_eval_mcontext(estate) \
	((estate)->eval_econtext->ecxt_per_tuple_memory)
#define eval_mcontext_alloc(estate, sz) \
	MemoryContextAlloc(get_eval_mcontext(estate), sz)
#define eval_mcontext_alloc0(estate, sz) \
	MemoryContextAllocZero(get_eval_mcontext(estate), sz)

#endif

static void check_stmt(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt, int *closing, List **exceptions);
static void check_stmts(PLpgSQL_checkstate *cstate, List *stmts, int *closing, List **exceptions);
static void check_target(PLpgSQL_checkstate *cstate, int varno, Oid *expected_typoid, int *expected_typmod);
static PLpgSQL_datum *copy_plpgsql_datum(PLpgSQL_checkstate *cstate, PLpgSQL_datum *datum);
static char *datum_get_refname(PLpgSQL_datum *d);
static TupleDesc expr_get_desc(PLpgSQL_checkstate *cstate,
							  PLpgSQL_expr *query,
										  bool use_element_type,
										  bool expand_record,
										  bool is_expression,
										  Oid *first_level_typoid);
static void format_error_xml(StringInfo str,
						  PLpgSQL_execstate *estate,
								 int sqlerrcode, int lineno,
								 const char *message, const char *detail, const char *hint,
								 int level, int position,
								 const char *query,
								 const char *context);
static void format_error_json(StringInfo str,
	PLpgSQL_execstate *estate,
	int sqlerrcode, int lineno,
	const char *message, const char *detail, const char *hint,
	int level, int position,
	const char *query,
	const char *context);
static void function_check(PLpgSQL_function *func, FunctionCallInfo fcinfo,
								   PLpgSQL_execstate *estate, PLpgSQL_checkstate *cstate);
static PLpgSQL_trigtype get_trigtype(HeapTuple procTuple);
static void init_datum_dno(PLpgSQL_checkstate *cstate, int varno);
static bool is_checked(PLpgSQL_function *func);
static int load_configuration(HeapTuple procTuple, bool *reload_config);
static void mark_as_checked(PLpgSQL_function *func);
static void plpgsql_check_HashTableInit(void);
static void prohibit_write_plan(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query);
static void check_fishy_qual(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query);
static void check_seq_functions(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query);
static void collect_volatility(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
static void put_error(PLpgSQL_checkstate *cstate,
					  int sqlerrcode, int lineno,
					  const char *message, const char *detail, const char *hint,
					  int level, int position,
						 const char *query, const char *context);
static void put_error_edata(PLpgSQL_checkstate *cstate, ErrorData *edata);
static void precheck_conditions(HeapTuple procTuple, PLpgSQL_trigtype trigtype, Oid relid);
static void prepare_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, int cursorOptions);
static void release_exprs(List *exprs);
static void setup_cstate(PLpgSQL_checkstate *cstate,
							 Oid fn_oid, Oid prorettype, char decl_volatility,
							 TupleDesc tupdesc, Tuplestorestate *tupstore,
							 bool fatal_errors,
								 bool other_warnings, bool perform_warnings, bool extra_warnings,
												    int format,
												    bool is_active_mode, bool fake_rtd);
static void setup_fake_fcinfo(HeapTuple procTuple,
						 FmgrInfo *flinfo,
						 FunctionCallInfoData *fcinfo,
						 ReturnSetInfo *rsinfo,
										 TriggerData *trigdata,
										 Oid relid,
										 EventTriggerData *etrigdata,
										 Oid funcoid,
										 PLpgSQL_trigtype trigtype,
										 Trigger *tg_trigger,
										 bool *fake_rtd);
static void setup_plpgsql_estate(PLpgSQL_execstate *estate,
								 PLpgSQL_function *func, ReturnSetInfo *rsi);
static void trigger_check(PLpgSQL_function *func,
							  Node *trigdata,
								  PLpgSQL_execstate *estate, PLpgSQL_checkstate *cstate);
static void tuplestore_put_error_text(Tuplestorestate *tuple_store, TupleDesc tupdesc,
						  PLpgSQL_execstate *estate, Oid fn_oid,
								 int sqlerrcode, int lineno,
								 const char *message, const char *detail, const char *hint,
								 int level, int position,
									 const char *query, const char *context);
static void tuplestore_put_error_tabular(Tuplestorestate *tuple_store, TupleDesc tupdesc,
						  PLpgSQL_execstate *estate, Oid fn_oid,
								 int sqlerrcode, int lineno,
								 const char *message, const char *detail, const char *hint,
								 int level, int position,
									 const char *query, const char *context);
static void tuplestore_put_text_line(Tuplestorestate *tuple_store, TupleDesc tupdesc,
								    const char *message, int len);
static void report_unused_variables(PLpgSQL_checkstate *cstate);
static void record_variable_usage(PLpgSQL_checkstate *cstate, int dno, bool write);
static bool datum_is_used(PLpgSQL_checkstate *cstate, int dno, bool write);
static void report_too_high_volatility(PLpgSQL_checkstate *cstate);
static bool is_const_null_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query);
static void prohibit_transaction_stmt(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query);
static int merge_closing(int c, int c_local, List **exceptions, List *exceptions_local, int err_code);
static int possibly_closed(int c);
static Query *ExprGetQuery(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query);
static char *ExprGetString(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query, bool *IsConst);
static bool exception_matches_conditions(int err_code, PLpgSQL_condition *cond);
static bool is_internal_variable(PLpgSQL_variable *var);
static void detect_dependency(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
static void tuplestore_put_dependency(Tuplestorestate *tuple_store,
									  TupleDesc tupdesc, char *type, Oid oid,
									  char *schema, char *name, char *params);

static void SetReturningFunctionCheck(ReturnSetInfo *rsinfo);

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
	int64	us_max;
	int64	us_total;
	int64	rows;
	int64	exec_count;
	instr_time	start_time;
	instr_time	total;
} profiler_stmt;

typedef struct profiler_stmt_reduced
{
	int		lineno;
	int64	us_max;
	int64	us_total;
	int64	rows;
	int64	exec_count;
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

static HTAB *shared_profiler_chunks_HashTable = NULL;
static HTAB *profiler_chunks_HashTable = NULL;

static void profiler_chunks_HashTableInit(void);

static profiler_shared_state *profiler_ss = NULL;

/*
 * It is used for fast mapping plpgsql stmt -> stmtid
 */
typedef struct profiler_map_entry
{
	PLpgSQL_stmt *stmt;
	int			stmtid;
	struct profiler_map_entry *next;
} profiler_map_entry;

typedef struct profiler_profile
{
	profiler_hashkey key;
	int			nstatements;
	PLpgSQL_stmt *entry_stmt;
	int			stmts_map_max_lineno;
	profiler_map_entry *stmts_map;
} profiler_profile;

typedef struct profiler_info
{
	profiler_profile *profile;
	profiler_stmt *stmts;
	instr_time	start_time;
} profiler_info;

static HTAB *profiler_HashTable = NULL;

static void profiler_localHashTableInit(void);

static void profiler_func_init(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void profiler_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void profiler_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);
static void profiler_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

static void profiler_touch_stmt(profiler_info *pinfo,
								PLpgSQL_stmt *stmt,
								bool generate_map,
								bool finelize_profile,
								int64 *us_total);

static bool plpgsql_check_profiler = true;

#if PG_VERSION_NUM >= 110000

static bool compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc);
static PLpgSQL_row *CallExprGetRowTarget(PLpgSQL_checkstate *cstate, PLpgSQL_expr *CallExpr);

#endif

static bool plpgsql_check_other_warnings = false;
static bool plpgsql_check_extra_warnings = false;
static bool plpgsql_check_performance_warnings = false;
static bool plpgsql_check_fatal_errors = true;
static int plpgsql_check_mode = PLPGSQL_CHECK_MODE_BY_FUNCTION;

static PLpgSQL_plugin plugin_funcs = { profiler_func_init,
									   check_on_func_beg,
									   profiler_func_end,
									   profiler_stmt_beg,
									   profiler_stmt_end,
									   NULL,
									   NULL};

static const struct config_enum_entry plpgsql_check_mode_options[] = {
	{"disabled", PLPGSQL_CHECK_MODE_DISABLED, false},
	{"by_function", PLPGSQL_CHECK_MODE_BY_FUNCTION, false},
	{"fresh_start", PLPGSQL_CHECK_MODE_FRESH_START, false},
	{"every_start", PLPGSQL_CHECK_MODE_EVERY_START, false},
	{NULL, 0, false}
};


static MemoryContext profiler_mcxt = NULL;

#if PG_VERSION_NUM >= 110000

#define recvar_tuple(rec)		(rec->erh ? expanded_record_get_tuple(rec->erh) : NULL)
#define recvar_tupdesc(rec)		(rec->erh ? expanded_record_fetch_tupdesc(rec->erh) : NULL)

#define is_procedure(estate)		((estate)->func && (estate)->func->fn_rettype == InvalidOid)

#else

#define recvar_tuple(rec)		(rec->tup)
#define recvar_tupdesc(rec)		(rec->tupdesc)

#define is_procedure(estate)	(false)

#endif

/* ----------
 * Hash table for checked functions
 * ----------
 */
static HTAB *plpgsql_check_HashTable = NULL;

typedef struct plpgsql_hashent
{
	PLpgSQL_func_hashkey key;
	TransactionId fn_xmin;
	ItemPointerData fn_tid;
	bool is_checked;
} plpgsql_check_HashEnt;

#define FUNCS_PER_USER		128 /* initial table size */

PG_FUNCTION_INFO_V1(plpgsql_check_function);
PG_FUNCTION_INFO_V1(plpgsql_check_function_tb);

PG_FUNCTION_INFO_V1(plpgsql_show_dependency_tb);

PG_FUNCTION_INFO_V1(plpgsql_profiler_function_tb);
PG_FUNCTION_INFO_V1(plpgsql_profiler_reset_all);
PG_FUNCTION_INFO_V1(plpgsql_profiler_reset);

void			_PG_init(void);
void			_PG_fini(void);

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void
profiler_shmem_startup(void)
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
	info.hash = tag_hash;

#if PG_VERSION_NUM >= 90500

	shared_profiler_chunks_HashTable = ShmemInitHash("plpgsql_check profiler chunks",
													MAX_SHARED_CHUNKS,
													MAX_SHARED_CHUNKS,
													&info,
													HASH_ELEM | HASH_BLOBS);

#else

	info.hash = tag_hash;

	shared_profiler_chunks_HashTable = ShmemInitHash("plpgsql_check profiler chunks",
													MAX_SHARED_CHUNKS,
													MAX_SHARED_CHUNKS,
													&info,
													HASH_ELEM | HASH_FUNCTION);



#endif

	LWLockRelease(AddinShmemInitLock);
}


/*
 * Module initialization
 *
 * join to PLpgSQL executor
 *
 */
void 
_PG_init(void)
{
	PLpgSQL_plugin ** var_ptr = (PLpgSQL_plugin **) find_rendezvous_variable( "PLpgSQL_plugin" );

	/* Be sure we do initialization only once (should be redundant now) */
	static bool inited = false;

	if (inited)
		return;

	*var_ptr = &plugin_funcs;

	DefineCustomEnumVariable("plpgsql_check.mode",
					    "choose a mode for enhanced checking",
					    NULL,
					    &plpgsql_check_mode,
					    PLPGSQL_CHECK_MODE_BY_FUNCTION,
					    plpgsql_check_mode_options,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.show_nonperformance_extra_warnings",
					    "when is true, then extra warning (except performance warnings) are showed",
					    NULL,
					    &plpgsql_check_extra_warnings,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.show_nonperformance_warnings",
					    "when is true, then warning (except performance warnings) are showed",
					    NULL,
					    &plpgsql_check_other_warnings,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.show_performance_warnings",
					    "when is true, then performance warnings are showed",
					    NULL,
					    &plpgsql_check_performance_warnings,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.fatal_errors",
					    "when is true, then plpgsql check stops execution on detected error",
					    NULL,
					    &plpgsql_check_fatal_errors,
					    true,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.profiler",
					    "when is true, then function execution profile is updated",
					    NULL,
					    &plpgsql_check_profiler,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	plpgsql_check_HashTableInit();

	profiler_init_hash_tables();

	/* Use shared memory when we can register more for self */
	if (process_shared_preload_libraries_in_progress)
	{
		Size		num_bytes = 0;

		num_bytes = MAXALIGN(sizeof(profiler_shared_state));
		num_bytes = add_size(num_bytes, hash_estimate_size(MAX_SHARED_CHUNKS, sizeof(profiler_stmt_chunk)));

		RequestAddinShmemSpace(num_bytes);

#if PG_VERSION_NUM >= 90600

		RequestNamedLWLockTranche("plpgsql_check profiler", 1);

#else

		RequestAddinLWLocks(1);

#endif

		/*
		 * Install hooks.
		 */
		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = profiler_shmem_startup;
	}

	inited = true;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;
}






/*
 * own implementation
 *
 */
static void
check_plpgsql_function(HeapTuple procTuple, Oid relid, PLpgSQL_trigtype trigtype,
					   TupleDesc tupdesc,
					   Tuplestorestate *tupstore,
							    int format,
									  bool fatal_errors,
									  bool other_warnings, 
									  bool performance_warnings,
									  bool extra_warnings)
{
	PLpgSQL_checkstate cstate;
	PLpgSQL_function *volatile function = NULL;
	int			save_nestlevel = 0;
	bool		reload_config;
	Oid			funcoid;
	Oid			prorettype;
	FunctionCallInfoData fake_fcinfo;
	FmgrInfo	flinfo;
	TriggerData trigdata;
	EventTriggerData etrigdata;
	Trigger tg_trigger;
	int			rc;
	ResourceOwner oldowner;
	PLpgSQL_execstate *cur_estate = NULL;
	MemoryContext old_cxt;
	PLpgSQL_execstate estate;
	ReturnSetInfo rsinfo;
	char		provolatile;
	bool		fake_rtd;

#if PG_VERSION_NUM >= 120000

	funcoid = ((Form_pg_proc) GETSTRUCT(procTuple))->oid;

#else

	funcoid = HeapTupleGetOid(procTuple);

#endif

	provolatile = ((Form_pg_proc) GETSTRUCT(procTuple))->provolatile;
	prorettype = ((Form_pg_proc) GETSTRUCT(procTuple))->prorettype;

	/*
	 * Connect to SPI manager
	 */
	if ((rc = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));

	setup_fake_fcinfo(procTuple, &flinfo, &fake_fcinfo, &rsinfo, &trigdata, relid, &etrigdata,
										  funcoid, trigtype, &tg_trigger, &fake_rtd);

	setup_cstate(&cstate, funcoid, prorettype, provolatile, tupdesc, tupstore,
							    fatal_errors,
							    other_warnings, performance_warnings, extra_warnings,
										    format,
										    true,
										    fake_rtd);

	old_cxt = MemoryContextSwitchTo(cstate.check_cxt);

	check_function_prolog(&cstate);

	/*
	 * Copy argument names for later check, only when other warnings are required.
	 * Argument names are used for check parameter versus local variable collision.
	 */
	if (other_warnings)
	{
		int		numargs;
		Oid		   *argtypes;
		char	  **argnames;
		char	   *argmodes;
		int			i;

		numargs = get_func_arg_info(procTuple,
							&argtypes, &argnames, &argmodes);

		if (argnames != NULL)
		{
			for (i = 0; i < numargs; i++)
			{
				if (argnames[i][0] != '\0')
					cstate.argnames = lappend(cstate.argnames, argnames[i]);
			}
		}
	}

	oldowner = CurrentResourceOwner;

	PG_TRY();
	{
		BeginInternalSubTransaction(NULL);
		MemoryContextSwitchTo(cstate.check_cxt);

		save_nestlevel = load_configuration(procTuple, &reload_config);

		/* have to wait for this decision to loaded configuration */
		if (plpgsql_check_mode != PLPGSQL_CHECK_MODE_DISABLED)
		{
			/* Get a compiled function */
			function = plpgsql_compile(&fake_fcinfo, false);

			/* Must save and restore prior value of cur_estate */
			cur_estate = function->cur_estate;

			/* recheck trigtype */

			Assert(function->fn_is_trigger == trigtype);

			setup_plpgsql_estate(&estate, function, (ReturnSetInfo *) fake_fcinfo.resultinfo);
			cstate.estate = &estate;

			/*
			 * Mark the function as busy, ensure higher than zero usage. There is no
			 * reason for protection function against delete, but I afraid of asserts.
			 */
			function->use_count++;

			/* Create a fake runtime environment and process check */
			switch (trigtype)
			{
				case PLPGSQL_DML_TRIGGER:
					trigger_check(function, (Node *) &trigdata, &estate, &cstate);
					break;

				case PLPGSQL_EVENT_TRIGGER:
					trigger_check(function, (Node *) &etrigdata, &estate, &cstate);
					break;

				case PLPGSQL_NOT_TRIGGER:
					function_check(function, &fake_fcinfo, &estate, &cstate);
					break;
			}

			function->cur_estate = cur_estate;
			function->use_count--;
		}
		else
			elog(NOTICE, "plpgsql_check is disabled");

		/*
		 * reload back a GUC. XXX: isn't this done automatically by subxact
		 * rollback?
		 */
		if (reload_config)
			AtEOXact_GUC(true, save_nestlevel);

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(cstate.check_cxt);
		CurrentResourceOwner = oldowner;

		if (OidIsValid(relid))
			relation_close(trigdata.tg_relation, AccessShareLock);

		release_exprs(cstate.exprs);

		SPI_restore_connection();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(cstate.check_cxt);
		edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(cstate.check_cxt);
		CurrentResourceOwner = oldowner;

		if (OidIsValid(relid))
			relation_close(trigdata.tg_relation, AccessShareLock);

		if (function)
		{
			function->cur_estate = cur_estate;
			function->use_count--;
			release_exprs(cstate.exprs);
		}

		put_error_edata(&cstate, edata);

		/* reconnect spi */
		SPI_restore_connection();
	}
	PG_END_TRY();

	check_function_epilog(&cstate);

	MemoryContextSwitchTo(old_cxt);
	MemoryContextDelete(cstate.check_cxt);

	/*
	 * Disconnect from SPI manager
	 */
	if ((rc = SPI_finish()) != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));
}

/*
 * Check function - it prepare variables and starts a prepare plan walker
 *
 */
static void
function_check(PLpgSQL_function *func, FunctionCallInfo fcinfo,
			   PLpgSQL_execstate *estate, PLpgSQL_checkstate *cstate)
{
	int			i;
	int closing = PLPGSQL_CHECK_UNCLOSED;
	List	   *exceptions;

	/*
	 * Make local execution copies of all the datums
	 */
	for (i = 0; i < cstate->estate->ndatums; i++)
		cstate->estate->datums[i] = copy_plpgsql_datum(cstate, func->datums[i]);

	/*
	 * Store the actual call argument values (fake) into the appropriate
	 * variables
	 */
	for (i = 0; i < func->fn_nargs; i++)
	{
		init_datum_dno(cstate, func->fn_argvarnos[i]);
	}

	/*
	 * Now check the toplevel block of statements
	 */
	check_stmt(cstate, (PLpgSQL_stmt *) func->action, &closing, &exceptions);

	/* clean state values - next errors are not related to any command */
	cstate->estate->err_stmt = NULL;

	if (closing != PLPGSQL_CHECK_CLOSED && closing != PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS &&
		!is_procedure(cstate->estate))
		put_error(cstate,
						  ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT, 0,
						  "control reached end of function without RETURN",
						  NULL,
						  NULL,
						  closing == PLPGSQL_CHECK_UNCLOSED ? PLPGSQL_CHECK_ERROR : PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);

	report_unused_variables(cstate);
	report_too_high_volatility(cstate);
}

/*
 * Check trigger - prepare fake environments for testing trigger
 *
 */
static void
trigger_check(PLpgSQL_function *func, Node *tdata,
			  PLpgSQL_execstate *estate, PLpgSQL_checkstate *cstate)
{
	PLpgSQL_rec *rec_new,
			   *rec_old;
	int			i;
	int closing = PLPGSQL_CHECK_UNCLOSED;
	List	   *exceptions;

	/*
	 * Make local execution copies of all the datums
	 */
	for (i = 0; i < cstate->estate->ndatums; i++)
		cstate->estate->datums[i] = copy_plpgsql_datum(cstate, func->datums[i]);

	if (IsA(tdata, TriggerData))
	{
		TriggerData *trigdata = (TriggerData *) tdata;

		/*
		 * Put the OLD and NEW tuples into record variables
		 *
		 * We make the tupdescs available in both records even though only one
		 * may have a value.  This allows parsing of record references to
		 * succeed in functions that are used for multiple trigger types. For
		 * example, we might have a test like "if (TG_OP = 'INSERT' and
		 * NEW.foo = 'xyz')", which should parse regardless of the current
		 * trigger type.
		 */
#if PG_VERSION_NUM >= 110000

		/*
		 * find all PROMISE VARIABLES and initit their
		 */
		for (i = 0; i < func->ndatums; i++)
		{
			PLpgSQL_datum *datum = func->datums[i];

			if (datum->dtype == PLPGSQL_DTYPE_PROMISE)
				init_datum_dno(cstate, datum->dno);
		}

		rec_new = (PLpgSQL_rec *) (cstate->estate->datums[func->new_varno]);
		recval_assign_tupdesc(cstate, rec_new, trigdata->tg_relation->rd_att, false);
		rec_old = (PLpgSQL_rec *) (cstate->estate->datums[func->old_varno]);
		recval_assign_tupdesc(cstate, rec_old, trigdata->tg_relation->rd_att, false);

#else

		rec_new = (PLpgSQL_rec *) (cstate->estate->datums[func->new_varno]);
		rec_new->freetup = false;
		rec_new->freetupdesc = false;
		assign_tupdesc_row_or_rec(cstate, NULL, rec_new, trigdata->tg_relation->rd_att, false);

		rec_old = (PLpgSQL_rec *) (cstate->estate->datums[func->old_varno]);
		rec_old->freetup = false;
		rec_old->freetupdesc = false;
		assign_tupdesc_row_or_rec(cstate, NULL, rec_old, trigdata->tg_relation->rd_att, false);

		/*
		 * Assign the special tg_ variables
		 */
		init_datum_dno(cstate, func->tg_op_varno);
		init_datum_dno(cstate, func->tg_name_varno);
		init_datum_dno(cstate, func->tg_when_varno);
		init_datum_dno(cstate, func->tg_level_varno);
		init_datum_dno(cstate, func->tg_relid_varno);
		init_datum_dno(cstate, func->tg_relname_varno);
		init_datum_dno(cstate, func->tg_table_name_varno);
		init_datum_dno(cstate, func->tg_table_schema_varno);
		init_datum_dno(cstate, func->tg_nargs_varno);
		init_datum_dno(cstate, func->tg_argv_varno);

#endif

	}
	else if (IsA(tdata, EventTriggerData))
	{

#if PG_VERSION_NUM < 110000

		init_datum_dno(cstate, func->tg_event_varno);
		init_datum_dno(cstate, func->tg_tag_varno);

#endif

	}
	else
		elog(ERROR, "unexpected environment");

	/*
	 * Now check the toplevel block of statements
	 */
	check_stmt(cstate, (PLpgSQL_stmt *) func->action, &closing, &exceptions);

	/* clean state values - next errors are not related to any command */
	cstate->estate->err_stmt = NULL;

	if (closing != PLPGSQL_CHECK_CLOSED && closing != PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS &&
		!is_procedure(cstate->estate))
		put_error(cstate,
						  ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT, 0,
						  "control reached end of function without RETURN",
						  NULL,
						  NULL,
						  closing == PLPGSQL_CHECK_UNCLOSED ? PLPGSQL_CHECK_ERROR : PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);

	report_unused_variables(cstate);
	report_too_high_volatility(cstate);
}

/*
 * Loads function's configuration
 *
 * Before checking function we have to load configuration related to
 * function. This is function manager job, but we don't use it for checking.
 */
static int
load_configuration(HeapTuple procTuple, bool *reload_config)
{
	Datum		datum;
	bool		isnull;
	int			new_nest_level;

	*reload_config = false;
	new_nest_level = 0;

	datum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proconfig, &isnull);
	if (!isnull)
	{
		ArrayType  *set_items;

		/* Set per-function configuration parameters */
		set_items = DatumGetArrayTypeP(datum);

		if (set_items != NULL)
		{						/* Need a new GUC nesting level */
			new_nest_level = NewGUCNestLevel();
			*reload_config = true;
			ProcessGUCArray(set_items,
							(superuser() ? PGC_SUSET : PGC_USERSET),
							PGC_S_SESSION,
							GUC_ACTION_SAVE);
		}
	}
	return new_nest_level;
}

/*
 * Release all plans created in check time
 *
 */
static void
release_exprs(List *exprs)
{
	ListCell *l;

	foreach(l, exprs)
	{
		PLpgSQL_expr *expr = (PLpgSQL_expr *) lfirst(l);

		SPI_freeplan(expr->plan);
		expr->plan = NULL;
	}
}

/****************************************************************************************
 * Prepare environment
 *
 ****************************************************************************************
 *
 */



/*
 * prepare PLpgSQL_checkstate structure
 *
 */
static void
setup_cstate(PLpgSQL_checkstate *cstate,
			 Oid fn_oid,
			 Oid prorettype,
			 char decl_volatility,
			 TupleDesc tupdesc,
			 Tuplestorestate *tupstore,
			 bool fatal_errors,
			 bool other_warnings,
			 bool performance_warnings,
			 bool extra_warnings,
			 int format,
			 bool is_active_mode,
			 bool fake_rtd)
{
	cstate->fn_oid = fn_oid;

	cstate->decl_volatility = decl_volatility;
	cstate->volatility = PROVOLATILE_IMMUTABLE;
	cstate->skip_volatility_check = (prorettype == TRIGGEROID ||
									 prorettype == OPAQUEOID ||
									 prorettype == EVTTRIGGEROID);
	cstate->estate = NULL;
	cstate->tupdesc = tupdesc;
	cstate->tuple_store = tupstore;
	cstate->fatal_errors = fatal_errors;
	cstate->other_warnings = other_warnings;
	cstate->performance_warnings = performance_warnings;
	cstate->extra_warnings = extra_warnings;
	cstate->argnames = NIL;
	cstate->exprs = NIL;
	cstate->used_variables = NULL;
	cstate->modif_variables = NULL;
	cstate->top_stmt_stack = NULL;

	cstate->format = format;
	cstate->is_active_mode = is_active_mode;

	cstate->func_oids = NULL;
	cstate->rel_oids = NULL;

	cstate->sinfo = NULL;

#if PG_VERSION_NUM >= 110000

	cstate->check_cxt = AllocSetContextCreate(CurrentMemoryContext,
										 "plpgsql_check temporary cxt",
										   ALLOCSET_DEFAULT_SIZES);

#else

	cstate->check_cxt = AllocSetContextCreate(CurrentMemoryContext,
										 "plpgsql_check temporary cxt",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

#endif

	cstate->found_return_query = false;

	cstate->fake_rtd = fake_rtd;
}


/*
 * Initialize plpgsql datum to NULL. This routine is used only for function
 * and trigger parameters so it should not support all dtypes.
 *
 */
static void
init_datum_dno(PLpgSQL_checkstate *cstate, int dno)
{
	switch (cstate->estate->datums[dno]->dtype)
	{

#if PG_VERSION_NUM >= 110000

		case PLPGSQL_DTYPE_PROMISE:

#endif

		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) cstate->estate->datums[dno];

				var->value = (Datum) 0;
				var->isnull = true;
				var->freeval = false;
			}
			break;

#if PG_VERSION_NUM >= 110000

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) cstate->estate->datums[dno];

				recval_init(rec);
				recval_assign_tupdesc(cstate, rec, NULL, false);
			}
			break;

#endif

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) cstate->estate->datums[dno];
				int			fnum;

				for (fnum = 0; fnum < row->nfields; fnum++)
				{
					if (row->varnos[fnum] < 0)
						continue;		/* skip dropped column in row struct */

					init_datum_dno(cstate, row->varnos[fnum]);
				}
			}
			break;

		default:
			elog(ERROR, "unexpected dtype: %d", cstate->estate->datums[dno]->dtype);
	}
}

/*
 * initializing local execution variables
 *
 */
PLpgSQL_datum *
copy_plpgsql_datum(PLpgSQL_checkstate *cstate, PLpgSQL_datum *datum)
{
	PLpgSQL_datum *result;

	switch (datum->dtype)
	{
		case PLPGSQL_DTYPE_VAR:

#if PG_VERSION_NUM >= 110000

		case PLPGSQL_DTYPE_PROMISE:

#endif

			{
				PLpgSQL_var *new = palloc(sizeof(PLpgSQL_var));

				memcpy(new, datum, sizeof(PLpgSQL_var));
				/* Ensure the value is null (possibly not needed?) */
				new->value = 0;
				new->isnull = true;
				new->freeval = false;

				result = (PLpgSQL_datum *) new;
			}
			break;

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *new = palloc(sizeof(PLpgSQL_rec));

				memcpy(new, datum, sizeof(PLpgSQL_rec));

				/* Ensure the value is well initialized with correct type */
				recval_init(new);
				recval_assign_tupdesc(cstate, new, NULL, false);

				result = (PLpgSQL_datum *) new;
			}
			break;

		case PLPGSQL_DTYPE_ROW:
		case PLPGSQL_DTYPE_RECFIELD:
		case PLPGSQL_DTYPE_ARRAYELEM:

			/*
			 * These datum records are read-only at runtime, so no need to
			 * copy them (well, ARRAYELEM contains some cached type data, but
			 * we'd just as soon centralize the caching anyway)
			 */
			result = datum;
			break;

		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			result = NULL;		/* keep compiler quiet */
			break;
	}

	return result;
}


/****************************************************************************************
 * Extended check walker
 *
 ****************************************************************************************
 *
 */






