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

#if PG_VERSION_NUM >= 100000

#define PLPGSQL_STMT_TYPES

#else

#define PLPGSQL_STMT_TYPES		(enum PLpgSQL_stmt_types)

#endif

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

/*
 * columns of plpgsql_check_function_table result
 *
 */
#define Natts_result					11

#define Anum_result_functionid			0
#define Anum_result_lineno			1
#define Anum_result_statement			2
#define Anum_result_sqlstate			3
#define Anum_result_message			4
#define Anum_result_detail			5
#define Anum_result_hint			6
#define Anum_result_level			7
#define Anum_result_position			8
#define Anum_result_query			9
#define Anum_result_context			10

/*
 * columns of plpgsql_show_dependency_tb result 
 *
 */
#define Natts_dependency				5

#define Anum_dependency_type			0
#define Anum_dependency_oid				1
#define Anum_dependency_schema			2
#define Anum_dependency_name			3
#define Anum_dependency_params			4

/*
 * columns of plpgsql_profiler_function_tb result
 *
 */
#define Natts_profiler					9

#define Anum_profiler_lineno			0
#define Anum_profiler_stmt_lineno		1
#define Anum_profiler_cmds_on_row		2
#define Anum_profiler_exec_count		3
#define Anum_profiler_total_time		4
#define Anum_profiler_avg_time			5
#define Anum_profiler_max_time			6
#define Anum_profiler_processed_rows	7
#define Anum_profiler_source			8

enum
{
	PLPGSQL_CHECK_ERROR,
	PLPGSQL_CHECK_WARNING_OTHERS,
	PLPGSQL_CHECK_WARNING_EXTRA,					/* check shadowed variables */
	PLPGSQL_CHECK_WARNING_PERFORMANCE
};

enum
{
	PLPGSQL_CHECK_FORMAT_ELOG,
	PLPGSQL_CHECK_FORMAT_TEXT,
	PLPGSQL_CHECK_FORMAT_TABULAR,
	PLPGSQL_CHECK_FORMAT_XML,
	PLPGSQL_CHECK_FORMAT_JSON,
	PLPGSQL_SHOW_DEPENDENCY_FORMAT_TABULAR
};

enum
{
	PLPGSQL_CHECK_CLOSED,
	PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS,
	PLPGSQL_CHECK_POSSIBLY_CLOSED,
	PLPGSQL_CHECK_UNCLOSED,
	PLPGSQL_CHECK_UNKNOWN
};

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

typedef struct PLpgSQL_checkstate
{
	Oid			fn_oid;						/* oid of checked function */
	List	    *argnames;					/* function arg names */
	char		decl_volatility;			/* declared function volatility */
	char		volatility;					/* detected function volatility */
	PLpgSQL_execstate	   *estate;			/* check state is estate extension */
	Tuplestorestate		   *tuple_store;	/* result target */
	TupleDesc	tupdesc;					/* result description */
	bool		fatal_errors;				/* stop on first error */
	bool		performance_warnings;		/* show performace warnings */
	bool		other_warnings;				/* show other warnings */
	bool		extra_warnings;				/* show extra warnings */
	int			format;						/* output format */
	StringInfo	sinfo;						/* aux. stringInfo used for result string concat */
	MemoryContext			check_cxt;
	List	   *exprs;						/* list of all expression created by checker */
	bool		is_active_mode;				/* true, when checking is started by plpgsql_check_function */
	Bitmapset  *used_variables;				/* track which variables have been used; bit per varno */
	Bitmapset  *modif_variables;			/* track which variables had been changed; bit per varno */
	PLpgSQL_stmt_stack_item *top_stmt_stack;	/* list of known labels + related command */
	bool		found_return_query;			/* true, when code contains RETURN query */
	bool		is_procedure;				/* true, when checked code is a procedure */
	Bitmapset	   *func_oids;				/* list of used (and displayed) functions */
	Bitmapset	   *rel_oids;				/* list of used (and displayed) relations */
	
} PLpgSQL_checkstate;

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
							 Oid fn_oid, char decl_volatility,
							 TupleDesc tupdesc, Tuplestorestate *tupstore,
							 bool fatal_errors,
								 bool other_warnings, bool perform_warnings, bool extra_warnings,
												    int format,
												    bool is_active_mode);
static void setup_fake_fcinfo(HeapTuple procTuple,
						 FmgrInfo *flinfo,
						 FunctionCallInfoData *fcinfo,
						 ReturnSetInfo *rsinfo,
										 TriggerData *trigdata,
										 Oid relid,
										 EventTriggerData *etrigdata,
										 Oid funcoid,
										 PLpgSQL_trigtype trigtype,
										 Trigger *tg_trigger);
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

static void
profiler_init_hash_tables(void)
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
					    PGC_SUSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.show_nonperformance_extra_warnings",
					    "when is true, then extra warning (except performance warnings) are showed",
					    NULL,
					    &plpgsql_check_extra_warnings,
					    false,
					    PGC_SUSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.show_nonperformance_warnings",
					    "when is true, then warning (except performance warnings) are showed",
					    NULL,
					    &plpgsql_check_other_warnings,
					    false,
					    PGC_SUSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.show_performance_warnings",
					    "when is true, then performance warnings are showed",
					    NULL,
					    &plpgsql_check_performance_warnings,
					    false,
					    PGC_SUSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.fatal_errors",
					    "when is true, then plpgsql check stops execution on detected error",
					    NULL,
					    &plpgsql_check_fatal_errors,
					    true,
					    PGC_SUSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.profiler",
					    "when is true, then function execution profile is updated",
					    NULL,
					    &plpgsql_check_profiler,
					    false,
					    PGC_SUSET, 0,
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
 * recval_init, recval_release, recval_assign_tupdesc
 *
 *   a set of functions designed to better portability between PostgreSQL 11
 *   with expanded records support and older PostgreSQL versions.
 */
static void
recval_init(PLpgSQL_rec *rec)
{
	Assert(rec->dtype == PLPGSQL_DTYPE_REC);

#if PG_VERSION_NUM >= 110000

	rec->erh = NULL;

#else

	rec->tup = NULL;
	rec->freetup = false;
	rec->freetupdesc = false;

#endif
}

static void
recval_release(PLpgSQL_rec *rec)
{

#if PG_VERSION_NUM >= 110000

	Assert(rec->dtype == PLPGSQL_DTYPE_REC);

	if (rec->erh)
		DeleteExpandedObject(ExpandedRecordGetDatum(rec->erh));
	rec->erh = NULL;

#else

	if (rec->freetup)
		heap_freetuple(rec->tup);

	if (rec->freetupdesc)
		FreeTupleDesc(rec->tupdesc);

	rec->freetup = false;
	rec->freetupdesc = false;

#endif

}

/*
 * is_null is true, when we assign NULL expression and type should not be checked.
 */
static void
recval_assign_tupdesc(PLpgSQL_checkstate *cstate, PLpgSQL_rec *rec, TupleDesc tupdesc, bool is_null)
{

#if PG_VERSION_NUM >= 110000

	PLpgSQL_execstate	   *estate = cstate->estate;
	ExpandedRecordHeader   *newerh;
	MemoryContext			mcontext;
	TupleDesc	var_tupdesc;
	Datum	   *newvalues;
	bool	   *newnulls;
	char	   *chunk;
	int			vtd_natts;
	int			i;

	mcontext = get_eval_mcontext(estate);
	recval_release(rec);

	/*
	 * code is reduced version of make_expanded_record_for_rec
	 */
	if (rec->rectypeid != RECORDOID)
	{
		newerh = make_expanded_record_from_typeid(rec->rectypeid, -1,
													  mcontext);
	}
	else
	{
		if (!tupdesc)
			return;

		newerh = make_expanded_record_from_tupdesc(tupdesc,
													   mcontext);
	}

	/*
	 * code is reduced version of exec_move_row_from_field
	 */
	var_tupdesc = expanded_record_get_tupdesc(newerh);
	vtd_natts = var_tupdesc->natts;

	if (!is_null && tupdesc != NULL && !compatible_tupdescs(var_tupdesc, tupdesc))
	{
		int		i = 0;
		int		j = 0;
		int		target_nfields = 0;
		int		src_nfields = 0;
		bool	src_field_is_valid = false;
		bool	target_field_is_valid = false;
		Form_pg_attribute sattr = NULL;
		Form_pg_attribute tattr = NULL;

		while (i < var_tupdesc->natts || j < tupdesc->natts)
		{
			if (!target_field_is_valid && i < var_tupdesc->natts)
			{
				tattr = TupleDescAttr(var_tupdesc, i);
				if (tattr->attisdropped)
				{
					i += 1;
					continue;
				}
				target_field_is_valid = true;
				target_nfields += 1;
			}

			if (!src_field_is_valid && j < tupdesc->natts)
			{
				sattr = TupleDescAttr(tupdesc, j);
				if (sattr->attisdropped)
				{
					j += 1;
					continue;
				}
				src_field_is_valid = true;
				src_nfields += 1;
			}

			if (src_field_is_valid && target_field_is_valid)
			{
				check_assign_to_target_type(cstate,
												tattr->atttypid, tattr->atttypmod,
												sattr->atttypid,
												false);

				/* try to search next tuple of fields */
				src_field_is_valid =  false;
				target_field_is_valid = false;
				i += 1;
				j += 1;
			}
			else
				break;
		}

		if (src_nfields < target_nfields)
			put_error(cstate,
						  0, 0,
						  "too few attributes for composite variable",
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_OTHERS,
						  0, NULL, NULL);
		else if (src_nfields > target_nfields)
			put_error(cstate,
						  0, 0,
						  "too many attributes for composite variable",
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_OTHERS,
						  0, NULL, NULL);
	}

	chunk = eval_mcontext_alloc(estate,
								vtd_natts * (sizeof(Datum) + sizeof(bool)));
	newvalues = (Datum *) chunk;
	newnulls = (bool *) (chunk + vtd_natts * sizeof(Datum));

	for (i = 0; i < vtd_natts; i++)
	{
		newvalues[i] = (Datum) 0;
		newnulls[i] = true;
	}

	expanded_record_set_fields(newerh, newvalues, newnulls, true);

	TransferExpandedRecord(newerh, estate->datum_context);
	rec->erh = newerh;

#else

	bool	   *nulls;
	HeapTuple	tup;

	recval_release(rec);

	if (!tupdesc)
		return;

	/* initialize rec by NULLs */
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	rec->tupdesc = CreateTupleDescCopy(tupdesc);
	rec->freetupdesc = true;

	tup = heap_form_tuple(tupdesc, NULL, nulls);
	if (HeapTupleIsValid(tup))
	{
		rec->tup = tup;
		rec->freetup = true;
	}
	else
		elog(ERROR, "cannot to build valid composite value");

#endif

}

static int
TupleDescNVatts(TupleDesc tupdesc)
{
	int		natts = 0;
	int		i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
			natts += 1;
	}

	return natts;
}

/*
 * row->nfields can cound dropped columns. When this behave can raise
 * false alarms, we should to count fields more precisely.
 */
static int
RowGetValidFields(PLpgSQL_row *row)
{
	int		i;
	int		result = 0;

	for (i = 0; i < row->nfields; i++)
	{
		if (row->varnos[i] != -1)
			result += 1;
	}

	return result;
}

/*
 * plpgsql_check_func_beg 
 *
 *      callback function - called by plgsql executor, when function is started
 *      and local variables are initialized.
 *
 */
static void
check_on_func_beg(PLpgSQL_execstate * estate, PLpgSQL_function * func)
{
	const char *err_text = estate->err_text;
	int closing;
	List		*exceptions;

	if (plpgsql_check_mode == PLPGSQL_CHECK_MODE_FRESH_START ||
		   plpgsql_check_mode == PLPGSQL_CHECK_MODE_EVERY_START)
	{
		int i;
		PLpgSQL_rec *saved_records;
		PLpgSQL_var *saved_vars;
		MemoryContext oldcontext,
					 old_cxt;
		ResourceOwner oldowner;
		PLpgSQL_checkstate cstate;
		char		provolatile;

		/*
		 * don't allow repeated execution on checked function
		 * when it is not requsted. 
		 */
		if (plpgsql_check_mode == PLPGSQL_CHECK_MODE_FRESH_START &&
			is_checked(func))
			return;

		mark_as_checked(func);

		if (OidIsValid(func->fn_oid))
		{
			HeapTuple	procTuple;

			procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->fn_oid));
			if (!HeapTupleIsValid(procTuple))
				elog(ERROR, "cache lookup failed for function %u", func->fn_oid);
			provolatile = ((Form_pg_proc) GETSTRUCT(procTuple))->provolatile;
			ReleaseSysCache(procTuple);
		}
		else
			provolatile = PROVOLATILE_IMMUTABLE;

		setup_cstate(&cstate, func->fn_oid, provolatile,
								NULL, NULL,
							    plpgsql_check_fatal_errors,
							    plpgsql_check_other_warnings,
							    plpgsql_check_performance_warnings,
							    plpgsql_check_extra_warnings,
							    PLPGSQL_CHECK_FORMAT_ELOG,
							    false);

		/* use real estate */
		cstate.estate = estate;

		cstate.is_procedure = func->fn_rettype == InvalidOid;

		old_cxt = MemoryContextSwitchTo(cstate.check_cxt);

		/*
		 * During the check stage a rec and vars variables are modified, so we should
		 * to save their content
		 */
		saved_records = palloc(sizeof(PLpgSQL_rec) * estate->ndatums);
		saved_vars = palloc(sizeof(PLpgSQL_var) * estate->ndatums);

		for (i = 0; i < estate->ndatums; i++)
		{
			if (estate->datums[i]->dtype == PLPGSQL_DTYPE_REC)
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) estate->datums[i];

#if PG_VERSION_NUM >= 110000

				if (rec->erh)
					expanded_record_set_tuple(saved_records[i].erh,
											  expanded_record_get_tuple(rec->erh),
											  true,
											  true);
				else
					saved_records[i].erh = NULL;

#else

				saved_records[i].tup = rec->tup;
				saved_records[i].tupdesc = rec->tupdesc;
				saved_records[i].freetup = rec->freetup;
				saved_records[i].freetupdesc = rec->freetupdesc;

				/* don't release a original tupdesc and original tup */
				rec->freetup = false;
				rec->freetupdesc = false;

#endif

			}
			else if (estate->datums[i]->dtype == PLPGSQL_DTYPE_VAR)
			{
				PLpgSQL_var *var = (PLpgSQL_var *) estate->datums[i];

				saved_vars[i].value = var->value;
				saved_vars[i].isnull = var->isnull;
				saved_vars[i].freeval = var->freeval;

				var->freeval = false;
			}
		}

		estate->err_text = NULL;

		/*
		 * Raised exception should be trapped in outer functtion. Protection
		 * against outer trap is QUERY_CANCELED exception. 
		 */
		oldcontext = CurrentMemoryContext;
		oldowner = CurrentResourceOwner;

		PG_TRY();
		{
			/*
			 * Now check the toplevel block of statements
			 */
			check_stmt(&cstate, (PLpgSQL_stmt *) func->action, &closing, &exceptions);

			estate->err_stmt = NULL;

			if (closing != PLPGSQL_CHECK_CLOSED && closing != PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS &&
				!is_procedure(estate))
				put_error(&cstate,
								  ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT, 0,
								  "control reached end of function without RETURN",
								  NULL,
								  NULL,
								  closing == PLPGSQL_CHECK_UNCLOSED ?
										PLPGSQL_CHECK_ERROR : PLPGSQL_CHECK_WARNING_EXTRA,
								  0, NULL, NULL);

			report_unused_variables(&cstate);
			report_too_high_volatility(&cstate);

		}
		PG_CATCH();
		{
			ErrorData  *edata;

			/* Save error info */
			MemoryContextSwitchTo(oldcontext);
			edata = CopyErrorData();
			FlushErrorState();
			CurrentResourceOwner = oldowner;

			release_exprs(cstate.exprs);

			edata->sqlerrcode = ERRCODE_QUERY_CANCELED;
			ReThrowError(edata);
		}
		PG_END_TRY();

		estate->err_text = err_text;
		estate->err_stmt = NULL;

		/* return back a original rec variables */
		for (i = 0; i < estate->ndatums; i++)
		{
			if (estate->datums[i]->dtype == PLPGSQL_DTYPE_REC)
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) estate->datums[i];

#if PG_VERSION_NUM >= 110000

				expanded_record_set_tuple(rec->erh,
										  expanded_record_get_tuple(saved_records[i].erh),
										  false,
										  false);

#else

				if (rec->freetupdesc)
					FreeTupleDesc(rec->tupdesc);

				rec->tup = saved_records[i].tup;
				rec->tupdesc = saved_records[i].tupdesc;
				rec->freetup = saved_records[i].freetup;
				rec->freetupdesc = saved_records[i].freetupdesc;

#endif

			}
			else if (estate->datums[i]->dtype == PLPGSQL_DTYPE_VAR)
			{
				PLpgSQL_var *var = (PLpgSQL_var *) estate->datums[i];

				var->value = saved_vars[i].value;
				var->isnull = saved_vars[i].isnull;
				var->freeval = saved_vars[i].freeval;
			}
		}

		MemoryContextSwitchTo(old_cxt);
		MemoryContextDelete(cstate.check_cxt);
	}
}

/*
 * plpgsql_check_function
 *
 * Extended check with formatted text output
 *
 */
Datum
plpgsql_check_function(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	Oid			relid = PG_GETARG_OID(1);
	char			*format_str = text_to_cstring(PG_GETARG_TEXT_PP(2));
	bool			fatal_errors = PG_GETARG_BOOL(3);
	bool			other_warnings = PG_GETARG_BOOL(4);
	bool			performance_warnings = PG_GETARG_BOOL(5);
	bool			extra_warnings;
	TupleDesc	tupdesc;
	HeapTuple	procTuple;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	PLpgSQL_trigtype trigtype;
	char *format_lower_str;
	int format = PLPGSQL_CHECK_FORMAT_TEXT;
	ErrorContextCallback *prev_errorcontext;

	if (PG_NARGS() != 7)
		elog(ERROR, "unexpected number of parameters, you should to update extension");

	extra_warnings = PG_GETARG_BOOL(6);

	format_lower_str = lowerstr(format_str);
	if (strcmp(format_lower_str, "text") == 0)
		format = PLPGSQL_CHECK_FORMAT_TEXT;
	else if (strcmp(format_lower_str, "xml") == 0)
		format = PLPGSQL_CHECK_FORMAT_XML;
	else if (strcmp(format_lower_str, "json") == 0)
		format = PLPGSQL_CHECK_FORMAT_JSON;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognize format: \"%s\"",
									 format_lower_str),
			errhint("Only \"text\", \"xml\" and \"json\" formats are supported.")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);

	trigtype = get_trigtype(procTuple);
	precheck_conditions(procTuple, trigtype, relid);

	/* need to build tuplestore in query context */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
	tupstore = tuplestore_begin_heap(false, false, work_mem);
	MemoryContextSwitchTo(oldcontext);

	prev_errorcontext = error_context_stack;
	error_context_stack = NULL;

	check_plpgsql_function(procTuple, relid, trigtype,
							   tupdesc, tupstore,
							   format,
								   fatal_errors,
								   other_warnings, performance_warnings, extra_warnings);
	error_context_stack = prev_errorcontext;

	ReleaseSysCache(procTuple);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	return (Datum) 0;
}

/*
 * plpgsql_check_function_tb
 *
 * It ensure a detailed validation and returns result as multicolumn table
 *
 */
Datum
plpgsql_check_function_tb(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	Oid			relid = PG_GETARG_OID(1);
	bool			fatal_errors = PG_GETARG_BOOL(2);
	bool			other_warnings = PG_GETARG_BOOL(3);
	bool			performance_warnings = PG_GETARG_BOOL(4);
	bool			extra_warnings;
	TupleDesc	tupdesc;
	HeapTuple	procTuple;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	PLpgSQL_trigtype trigtype;
	ErrorContextCallback *prev_errorcontext;

	if (PG_NARGS() != 6)
		elog(ERROR, "unexpected number of parameters, you should to update extension");

	extra_warnings = PG_GETARG_BOOL(5);

	/* check to see if caller supports us returning a tuplestore */
	SetReturningFunctionCheck(rsinfo);

	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);

	trigtype = get_trigtype(procTuple);
	precheck_conditions(procTuple, trigtype, relid);

	/* need to build tuplestore in query context */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
	tupstore = tuplestore_begin_heap(false, false, work_mem);
	MemoryContextSwitchTo(oldcontext);

	prev_errorcontext = error_context_stack;

	/* Envelope outer plpgsql function is not interesting */
	error_context_stack = NULL;

	check_plpgsql_function(procTuple, relid, trigtype,
							   tupdesc, tupstore,
							   PLPGSQL_CHECK_FORMAT_TABULAR,
								   fatal_errors,
								   other_warnings, performance_warnings, extra_warnings);
	error_context_stack = prev_errorcontext;

	ReleaseSysCache(procTuple);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	return (Datum) 0;
}

/*
 * Add label to stack of labels
 */
static PLpgSQL_stmt_stack_item *
push_stmt_to_stmt_stack(PLpgSQL_checkstate *cstate)
{
	PLpgSQL_stmt *stmt = cstate->estate->err_stmt;
	PLpgSQL_stmt_stack_item *stmt_stack_item;
	PLpgSQL_stmt_stack_item *current = cstate->top_stmt_stack;

	stmt_stack_item = (PLpgSQL_stmt_stack_item *) palloc(sizeof(PLpgSQL_stmt_stack_item));
	stmt_stack_item->stmt = stmt;

	switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			stmt_stack_item->label = ((PLpgSQL_stmt_block *) stmt)->label;
			break;

		case PLPGSQL_STMT_EXIT:
			stmt_stack_item->label = ((PLpgSQL_stmt_exit *) stmt)->label;
			break;

		case PLPGSQL_STMT_LOOP:
			stmt_stack_item->label = ((PLpgSQL_stmt_loop *) stmt)->label;
			break;

		case PLPGSQL_STMT_WHILE:
			stmt_stack_item->label = ((PLpgSQL_stmt_while *) stmt)->label;
			break;

		case PLPGSQL_STMT_FORI:
			stmt_stack_item->label = ((PLpgSQL_stmt_fori *) stmt)->label;
			break;

		case PLPGSQL_STMT_FORS:
			stmt_stack_item->label = ((PLpgSQL_stmt_fors *) stmt)->label;
			break;

		case PLPGSQL_STMT_FORC:
			stmt_stack_item->label = ((PLpgSQL_stmt_forc *) stmt)->label;
			break;

		case PLPGSQL_STMT_DYNFORS:
			stmt_stack_item->label = ((PLpgSQL_stmt_dynfors *) stmt)->label;
			break;

		case PLPGSQL_STMT_FOREACH_A:
			stmt_stack_item->label = ((PLpgSQL_stmt_foreach_a *) stmt)->label;
			break;

		default:
			stmt_stack_item->label = NULL;
	}

	stmt_stack_item->outer = current;
	cstate->top_stmt_stack = stmt_stack_item;

	return current;
}

static void
pop_stmt_from_stmt_stack(PLpgSQL_checkstate *cstate)
{
	PLpgSQL_stmt_stack_item *current = cstate->top_stmt_stack;

	Assert(cstate->top_stmt_stack != NULL);

	cstate->top_stmt_stack = current->outer;
	pfree(current);
}

/*
 * Returns true, when stmt is any loop statement
 */
static bool
is_any_loop_stmt(PLpgSQL_stmt *stmt)
{
	switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
	{
		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_WHILE:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_FORS:
		case PLPGSQL_STMT_FORC:
		case PLPGSQL_STMT_DYNFORS:
		case PLPGSQL_STMT_FOREACH_A:
			return true;
		default:
			return false;
	}
}

/*
 * Searching a any statement related to CONTINUE/EXIT statement.
 * label cannot be NULL.
 */
static PLpgSQL_stmt *
find_stmt_with_label(char *label, PLpgSQL_stmt_stack_item *current)
{
	while (current != NULL)
	{
		if (current->label != NULL
				&& strcmp(current->label, label) == 0)
			return current->stmt;

		current = current->outer;
	}

	return NULL;
}

static PLpgSQL_stmt *
find_nearest_loop(PLpgSQL_stmt_stack_item *current)
{
	while (current != NULL)
	{
		if (is_any_loop_stmt(current->stmt))
			return current->stmt;

		current = current->outer;
	}

	return NULL;
}

/*
 * returns false, when a variable doesn't shadows any other variable
 */
static bool
found_shadowed_variable(char *varname, PLpgSQL_stmt_stack_item *current, PLpgSQL_checkstate *cstate)
{
	while (current != NULL)
	{
		if (current->stmt->cmd_type == PLPGSQL_STMT_BLOCK)
		{
			PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) current->stmt;
			int			i;
			PLpgSQL_datum *d;

			for (i = 0; i < stmt_block->n_initvars; i++)
			{
				char	   *refname;

				d = cstate->estate->func->datums[stmt_block->initvarnos[i]];
				refname = datum_get_refname(d);

				if (refname != NULL && strcmp(refname, varname) == 0)
					return true;
			}
		}

		current = current->outer;
	}

	return false;
}

/*
 * Returns PLpgSQL_trigtype based on prorettype
 */
static PLpgSQL_trigtype
get_trigtype(HeapTuple procTuple)
{
	Form_pg_proc proc;
	char		functyptype;

	proc = (Form_pg_proc) GETSTRUCT(procTuple);

	functyptype = get_typtype(proc->prorettype);

	/*
	 * Disallow pseudotype result  except for TRIGGER, RECORD, VOID, or
	 * polymorphic
	 */
	if (functyptype == TYPTYPE_PSEUDO)
	{
		/* we assume OPAQUE with no arguments means a trigger */
		if (proc->prorettype == TRIGGEROID ||
			(proc->prorettype == OPAQUEOID && proc->pronargs == 0))
			return PLPGSQL_DML_TRIGGER;
		else if (proc->prorettype == EVTTRIGGEROID)
			return PLPGSQL_EVENT_TRIGGER;
		else if (proc->prorettype != RECORDOID &&
				 proc->prorettype != VOIDOID &&
				 !IsPolymorphicType(proc->prorettype))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("PL/pgSQL functions cannot return type %s",
							format_type_be(proc->prorettype))));
	}

	return PLPGSQL_NOT_TRIGGER;
}

/*
 * Process necessary checking before code checking
 *     a) disallow other than plpgsql check function,
 *     b) when function is trigger function, then reloid must be defined
 */
static void
precheck_conditions(HeapTuple procTuple, PLpgSQL_trigtype trigtype, Oid relid)
{
	Form_pg_proc proc;
	Form_pg_language languageStruct;
	HeapTuple	languageTuple;
	char	   *funcname;

	proc = (Form_pg_proc) GETSTRUCT(procTuple);

#if PG_VERSION_NUM >= 120000

	funcname = format_procedure(proc->oid);

#else

	funcname = format_procedure(HeapTupleGetOid(procTuple));

#endif

	/* used language must be plpgsql */
	languageTuple = SearchSysCache1(LANGOID, ObjectIdGetDatum(proc->prolang));
	Assert(HeapTupleIsValid(languageTuple));

	languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);
	if (strcmp(NameStr(languageStruct->lanname), "plpgsql") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s is not a plpgsql function", funcname)));

	ReleaseSysCache(languageTuple);

	/* dml trigger needs valid relid, others not */
	if (trigtype == PLPGSQL_DML_TRIGGER)
	{
		if (!OidIsValid(relid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("missing trigger relation"),
					 errhint("Trigger relation oid must be valid")));
	}
	else
	{
		if (OidIsValid(relid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("function is not trigger"),
					 errhint("Trigger relation oid must not be valid for non dml trigger function.")));
	}

	pfree(funcname);
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

#if PG_VERSION_NUM >= 120000

	funcoid = ((Form_pg_proc) GETSTRUCT(procTuple))->oid;

#else

	funcoid = HeapTupleGetOid(procTuple);

#endif

	provolatile = ((Form_pg_proc) GETSTRUCT(procTuple))->provolatile;

	/*
	 * Connect to SPI manager
	 */
	if ((rc = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));

	setup_fake_fcinfo(procTuple, &flinfo, &fake_fcinfo, &rsinfo, &trigdata, relid, &etrigdata,
										  funcoid, trigtype, &tg_trigger);

	setup_cstate(&cstate, funcoid, provolatile, tupdesc, tupstore,
							    fatal_errors,
							    other_warnings, performance_warnings, extra_warnings,
										    format,
										    true);

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

static bool
is_polymorphic_tupdesc(TupleDesc tupdesc)
{
	int	i;

	for (i = 0; i < tupdesc->natts; i++)
		if (IsPolymorphicType(TupleDescAttr(tupdesc, i)->atttypid))
			return true;

	return false;
}

/*
 * Set up a fake fcinfo with just enough info to satisfy plpgsql_compile().
 *
 * There should be a different real argtypes for polymorphic params.
 *
 */
static void
setup_fake_fcinfo(HeapTuple procTuple,
						  FmgrInfo *flinfo,
						  FunctionCallInfoData *fcinfo,
						  ReturnSetInfo *rsinfo,
						  TriggerData *trigdata,
						  Oid relid,
						  EventTriggerData *etrigdata,
						  Oid funcoid,
						  PLpgSQL_trigtype trigtype,
						  Trigger *tg_trigger)
{
	Form_pg_proc procform;
	Oid		rettype;
	TupleDesc resultTupleDesc;

	procform = (Form_pg_proc) GETSTRUCT(procTuple);
	rettype = procform->prorettype;

	/* clean structures */
	MemSet(fcinfo, 0, sizeof(FunctionCallInfoData));
	MemSet(flinfo, 0, sizeof(FmgrInfo));
	MemSet(rsinfo, 0, sizeof(ReturnSetInfo));

	fcinfo->flinfo = flinfo;
	flinfo->fn_oid = funcoid;
	flinfo->fn_mcxt = CurrentMemoryContext;

	if (trigtype == PLPGSQL_DML_TRIGGER)
	{
		Assert(trigdata != NULL);

		MemSet(trigdata, 0, sizeof(TriggerData));
		MemSet(tg_trigger, 0, sizeof(Trigger));

		trigdata->type = T_TriggerData;
		trigdata->tg_trigger = tg_trigger;

		fcinfo->context = (Node *) trigdata;

		if (OidIsValid(relid))
			trigdata->tg_relation = relation_open(relid, AccessShareLock);
	}
	else if (trigtype == PLPGSQL_EVENT_TRIGGER)
	{
		MemSet(etrigdata, 0, sizeof(etrigdata));
		etrigdata->type = T_EventTriggerData;
		fcinfo->context = (Node *) etrigdata;
	}

	/* 
	 * prepare ReturnSetInfo
	 *
	 * necessary for RETURN NEXT and RETURN QUERY
	 *
	 */
	resultTupleDesc = build_function_result_tupdesc_t(procTuple);
	if (resultTupleDesc)
	{
		/* we cannot to solve polymorphic params now */
		if (is_polymorphic_tupdesc(resultTupleDesc))
		{
			FreeTupleDesc(resultTupleDesc);
			resultTupleDesc = NULL;
		}
	}
	else if (rettype == TRIGGEROID || rettype == OPAQUEOID)
	{
		/* trigger - return value should be ROW or RECORD based on relid */
		if (trigdata->tg_relation)
			resultTupleDesc = CreateTupleDescCopy(trigdata->tg_relation->rd_att);
	}
	else if (!IsPolymorphicType(rettype))
	{
		if (get_typtype(rettype) == TYPTYPE_COMPOSITE)
			resultTupleDesc = lookup_rowtype_tupdesc_copy(rettype, -1);
		else
		{
#if PG_VERSION_NUM >= 120000

			resultTupleDesc = CreateTemplateTupleDesc(1);

#else

			resultTupleDesc = CreateTemplateTupleDesc(1, false);

#endif

			TupleDescInitEntry(resultTupleDesc,
							    (AttrNumber) 1, "__result__",
							    rettype, -1, 0);
			resultTupleDesc = BlessTupleDesc(resultTupleDesc);
		}
	}

	if (resultTupleDesc)
	{
		fcinfo->resultinfo = (Node *) rsinfo;

		rsinfo->type = T_ReturnSetInfo;
		rsinfo->expectedDesc = resultTupleDesc;
		rsinfo->allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
		rsinfo->returnMode = SFRM_ValuePerCall;

		/*
		 * ExprContext is created inside CurrentMemoryContext,
		 * without any additional source allocation. It is released
		 * on end of transaction.
		 */
		rsinfo->econtext = CreateStandaloneExprContext();
	}
}

/*
 * prepare PLpgSQL_checkstate structure
 *
 */
static void
setup_cstate(PLpgSQL_checkstate *cstate,
			 Oid fn_oid,
			 char decl_volatility,
			 TupleDesc tupdesc,
			 Tuplestorestate *tupstore,
			 bool fatal_errors,
			 bool other_warnings,
			 bool performance_warnings,
			 bool extra_warnings,
			 int format,
			 bool is_active_mode)
{
	cstate->fn_oid = fn_oid;

	cstate->decl_volatility = decl_volatility;
	cstate->volatility = PROVOLATILE_IMMUTABLE;
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
}

/* ----------
 * Initialize a plpgsql fake execution state
 * ----------
 */
static void
setup_plpgsql_estate(PLpgSQL_execstate *estate,
					 PLpgSQL_function *func,
					 ReturnSetInfo *rsi)
{
	/* this link will be restored at exit from plpgsql_call_handler */
	func->cur_estate = estate;

	estate->func = func;

	estate->retval = (Datum) 0;
	estate->retisnull = true;
	estate->rettype = InvalidOid;

	estate->fn_rettype = func->fn_rettype;
	estate->retistuple = func->fn_retistuple;
	estate->retisset = func->fn_retset;

	estate->readonly_func = func->fn_readonly;

#if PG_VERSION_NUM < 110000

	estate->rettupdesc = NULL;
	estate->eval_econtext = NULL;

#else

	estate->eval_econtext = makeNode(ExprContext);
	estate->eval_econtext->ecxt_per_tuple_memory = AllocSetContextCreate(CurrentMemoryContext,
													"ExprContext",
													ALLOCSET_DEFAULT_SIZES);
	estate->datum_context = CurrentMemoryContext;

#endif

	estate->exitlabel = NULL;
	estate->cur_error = NULL;

	estate->tuple_store = NULL;
	if (rsi)
	{
		estate->tuple_store_cxt = rsi->econtext->ecxt_per_query_memory;
		estate->tuple_store_owner = CurrentResourceOwner;

#if PG_VERSION_NUM >= 110000

		estate->tuple_store_desc = rsi->expectedDesc;

#else

		if (estate->retisset)
			estate->rettupdesc = rsi->expectedDesc;

#endif

	}
	else
	{
		estate->tuple_store_cxt = NULL;
		estate->tuple_store_owner = NULL;
	}
	estate->rsi = rsi;

	estate->found_varno = func->found_varno;
	estate->ndatums = func->ndatums;
	estate->datums = palloc(sizeof(PLpgSQL_datum *) * estate->ndatums);
	/* caller is expected to fill the datums array */

	estate->eval_tuptable = NULL;
	estate->eval_processed = 0;

#if PG_VERSION_NUM < 120000

	estate->eval_lastoid = InvalidOid;


#if PG_VERSION_NUM < 90500

	estate->cur_expr = NULL;

#endif

#endif


	estate->err_stmt = NULL;
	estate->err_text = NULL;

	estate->plugin_info = NULL;
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


/*
 * walk over all plpgsql statements - search and check expressions
 *
 */
static void
check_stmt(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt, int *closing, List **exceptions)
{
	TupleDesc	tupdesc = NULL;
	PLpgSQL_function *func;
	ListCell   *l;
	ResourceOwner oldowner;
	MemoryContext oldCxt = CurrentMemoryContext;
	PLpgSQL_stmt_stack_item *outer_stmt;

	if (stmt == NULL)
		return;

	cstate->estate->err_stmt = stmt;
	func = cstate->estate->func;

	/*
	 * Attention - returns NULL, when there are not any outer level
	 */
	outer_stmt = push_stmt_to_stmt_stack(cstate);

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
		{
			case PLPGSQL_STMT_BLOCK:
				{
					PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) stmt;
					int			i;
					PLpgSQL_datum *d;

					for (i = 0; i < stmt_block->n_initvars; i++)
					{
						char	   *refname;

						d = func->datums[stmt_block->initvarnos[i]];

						if (d->dtype == PLPGSQL_DTYPE_VAR)
						{
							PLpgSQL_var *var = (PLpgSQL_var *) d;

							check_expr(cstate, var->default_val);
						}
						refname = datum_get_refname(d);
						if (refname != NULL)
						{
							ListCell   *l;

							foreach(l, cstate->argnames)
							{
								char	   *argname = (char *) lfirst(l);

								if (strcmp(argname, refname) == 0)
								{
									StringInfoData str;

									initStringInfo(&str);
									appendStringInfo(&str, "parameter \"%s\" is overlapped",
													 refname);

									put_error(cstate,
												  0, 0,
												  str.data,
												  "Local variable overlap function parameter.",
												  NULL,
												  PLPGSQL_CHECK_WARNING_OTHERS,
												  0, NULL, NULL);
									pfree(str.data);
								}
							}

							if (found_shadowed_variable(refname, outer_stmt, cstate))
							{
								StringInfoData str;

								initStringInfo(&str);
								appendStringInfo(&str, "variable \"%s\" shadows a previously defined variable",
													 refname);

								put_error(cstate,
												  0, 0,
												  str.data,
												  NULL,
												  "SET plpgsql.extra_warnings TO 'shadowed_variables'",
												  PLPGSQL_CHECK_WARNING_EXTRA,
												  0, NULL, NULL);
								pfree(str.data);
							}
						}
					}

					check_stmts(cstate, stmt_block->body, closing, exceptions);

					if (stmt_block->exceptions)
					{
						int closing_local;
						List   *exceptions_local = NIL;
						int		closing_handlers = PLPGSQL_CHECK_UNKNOWN;
						List   *exceptions_transformed = NIL;

						if (*closing == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
						{
							ListCell   *lc;
							int			i = 0;
							int    *err_codes = NULL;
							int		nerr_codes = 0;

							/* copy errcodes to a array */
							nerr_codes = list_length(*exceptions);
							err_codes = palloc(sizeof(int) * nerr_codes);

							foreach(lc, *exceptions)
							{
								err_codes[i++] = lfirst_int(lc);
							}

							foreach(l, stmt_block->exceptions->exc_list)
							{
								PLpgSQL_exception *exception = (PLpgSQL_exception *) lfirst(l);

								/* RETURN in exception handler ~ is possible closing */
								check_stmts(cstate, exception->action,
												&closing_local, &exceptions_local);

								if (*exceptions != NIL)
								{
									int		i;

									for (i = 0; i < nerr_codes; i++)
									{
										int		err_code = err_codes[i];

										if (err_code != -1 &&
											exception_matches_conditions(err_code, exception->conditions))
										{
											closing_handlers = merge_closing(closing_handlers, closing_local,
																			 &exceptions_transformed, exceptions_local,
																			 err_code);
											*exceptions = list_delete_int(*exceptions, err_code);
											err_codes[i] = -1;
										}
									}
								}
							}

							Assert(err_codes != NULL);
							pfree(err_codes);

							if (closing_handlers != PLPGSQL_CHECK_UNKNOWN)
							{
								*closing = closing_handlers;
								if (closing_handlers == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
									*exceptions = list_concat_unique_int(*exceptions, exceptions_transformed);
								else
									*exceptions = NIL;
							}
						}
						else
						{
							foreach(l, stmt_block->exceptions->exc_list)
							{
								PLpgSQL_exception *exception = (PLpgSQL_exception *) lfirst(l);

								/* RETURN in exception handler ~ it is possible closing only */
								check_stmts(cstate, exception->action,
												&closing_local, &exceptions_local);

								closing_handlers = merge_closing(closing_handlers, closing_local,
																 &exceptions_transformed, exceptions_local,
																 -1);
							}

							if (closing_handlers != *closing)
								*closing = PLPGSQL_CHECK_POSSIBLY_CLOSED;
						}

						/*
						 * Mark the hidden variables SQLSTATE and SQLERRM used
						 * even if they actually weren't.  Not using them
						 * should practically never be a sign of a problem, so
						 * there's no point in annoying the user.
						 */
						record_variable_usage(cstate, stmt_block->exceptions->sqlstate_varno, false);
						record_variable_usage(cstate, stmt_block->exceptions->sqlerrm_varno, false);
					}
				}
				break;

#if PG_VERSION_NUM >= 90500

			case PLPGSQL_STMT_ASSERT:
				{
					PLpgSQL_stmt_assert *stmt_assert = (PLpgSQL_stmt_assert *) stmt;

					/*
					 * Should or should not to depends on plpgsql_check_asserts?
					 * I am thinking, so any code (active or inactive) should be valid,
					 * so I ignore plpgsql_check_asserts option.
					 */
					check_expr_with_expected_scalar_type(cstate,
									 stmt_assert->cond, BOOLOID, true);
					if (stmt_assert->message != NULL)
						check_expr(cstate, stmt_assert->message);
				}
				break;

#endif

			case PLPGSQL_STMT_ASSIGN:
				{
					PLpgSQL_stmt_assign *stmt_assign = (PLpgSQL_stmt_assign *) stmt;

					check_assignment(cstate, stmt_assign->expr, NULL, NULL,
									 stmt_assign->varno);
				}
				break;

			case PLPGSQL_STMT_IF:
				{
					PLpgSQL_stmt_if	*stmt_if = (PLpgSQL_stmt_if *) stmt;
					ListCell    *l;
					int		closing_local;
					int		closing_all_paths = PLPGSQL_CHECK_UNKNOWN;
					List   *exceptions_local;

					check_expr_with_expected_scalar_type(cstate,
									     stmt_if->cond, BOOLOID, true);

					check_stmts(cstate, stmt_if->then_body, &closing_local,
								&exceptions_local);
					closing_all_paths = merge_closing(closing_all_paths,
													  closing_local,
													  exceptions,
													  exceptions_local,
													  -1);

					foreach(l, stmt_if->elsif_list)
					{
						PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(l);

						check_expr_with_expected_scalar_type(cstate,
										     elif->cond, BOOLOID, true);
						check_stmts(cstate, elif->stmts, &closing_local,
									&exceptions_local);
						closing_all_paths = merge_closing(closing_all_paths,
														  closing_local,
														  exceptions,
														  exceptions_local,
														  -1);
					}

					check_stmts(cstate, stmt_if->else_body, &closing_local,
								&exceptions_local);
					closing_all_paths = merge_closing(closing_all_paths,
													  closing_local,
													  exceptions,
													  exceptions_local,
													  -1);

					if (stmt_if->else_body != NULL)
						*closing = closing_all_paths;
					else if (closing_all_paths == PLPGSQL_CHECK_UNCLOSED)
						*closing = PLPGSQL_CHECK_UNCLOSED;
					else
						*closing = PLPGSQL_CHECK_POSSIBLY_CLOSED;
				}
				break;

			case PLPGSQL_STMT_CASE:
				{
					PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;
					Oid			result_oid;
					int		closing_local;
					List	*exceptions_local;
					int		closing_all_paths = PLPGSQL_CHECK_UNKNOWN;

					if (stmt_case->t_expr != NULL)
					{
						PLpgSQL_var *t_var = (PLpgSQL_var *) cstate->estate->datums[stmt_case->t_varno];

						/*
						 * we need to set hidden variable type
						 */
						prepare_expr(cstate, stmt_case->t_expr, 0);

						/* record all variables used by the query */
						cstate->used_variables = bms_add_members(cstate->used_variables,
																 stmt_case->t_expr->paramnos);

						tupdesc = expr_get_desc(cstate,
												stmt_case->t_expr,
												false,	/* no element type */
												true,	/* expand record */
												true,	/* is expression */
												NULL);
						result_oid = TupleDescAttr(tupdesc, 0)->atttypid;

						/*
						 * When expected datatype is different from real,
						 * change it. Note that what we're modifying here is
						 * an execution copy of the datum, so this doesn't
						 * affect the originally stored function parse tree.
						 */
						if (t_var->datatype->typoid != result_oid)
							t_var->datatype = plpgsql_build_datatype(result_oid,
																	 -1,
								   cstate->estate->func->fn_input_collation);
						ReleaseTupleDesc(tupdesc);
					}
					foreach(l, stmt_case->case_when_list)
					{
						PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);

						check_expr(cstate, cwt->expr);
						check_stmts(cstate, cwt->stmts, &closing_local, &exceptions_local);
						closing_all_paths = merge_closing(closing_all_paths,
														  closing_local,
														  exceptions,
														  exceptions_local,
														  -1);
					}

					if (stmt_case->else_stmts)
					{
						check_stmts(cstate, stmt_case->else_stmts, &closing_local, &exceptions_local);
						*closing = merge_closing(closing_all_paths,
														  closing_local,
														  exceptions,
														  exceptions_local,
														  -1);
					}
					else
						/* is not ensured all path evaluation */
						*closing = possibly_closed(closing_all_paths);
				}
				break;

			case PLPGSQL_STMT_LOOP:
				check_stmts(cstate, ((PLpgSQL_stmt_loop *) stmt)->body, closing, exceptions);
				break;

			case PLPGSQL_STMT_WHILE:
				{
					PLpgSQL_stmt_while *stmt_while = (PLpgSQL_stmt_while *) stmt;
					int		closing_local;
					List   *exceptions_local;

					check_expr_with_expected_scalar_type(cstate,
										     stmt_while->cond,
										     BOOLOID,
										     true);

					/*
					 * When is not guaranteed execution (possible zero loops),
					 * then ignore closing info from body.
					 */
					check_stmts(cstate, stmt_while->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_FORI:
				{
					PLpgSQL_stmt_fori *stmt_fori = (PLpgSQL_stmt_fori *) stmt;
					int			dno = stmt_fori->var->dno;
					int		closing_local;
					List   *exceptions_local;

					/* prepare plan if desn't exist yet */
					check_assignment(cstate, stmt_fori->lower, NULL, NULL, dno);
					check_assignment(cstate, stmt_fori->upper, NULL, NULL, dno);

					if (stmt_fori->step)
						check_assignment(cstate, stmt_fori->step, NULL, NULL, dno);

					check_stmts(cstate, stmt_fori->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_FORS:
				{
					PLpgSQL_stmt_fors *stmt_fors = (PLpgSQL_stmt_fors *) stmt;
					int		closing_local;
					List   *exceptions_local;

#if PG_VERSION_NUM >= 110000

					check_variable(cstate, stmt_fors->var);

					/* we need to set hidden variable type */
					check_assignment_to_variable(cstate, stmt_fors->query,
									 stmt_fors->var, -1);

#else

					check_row_or_rec(cstate, stmt_fors->row, stmt_fors->rec);

					/* we need to set hidden variable type */
					check_assignment(cstate, stmt_fors->query,
									 stmt_fors->rec, stmt_fors->row, -1);

#endif

					check_stmts(cstate, stmt_fors->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_FORC:
				{
					PLpgSQL_stmt_forc *stmt_forc = (PLpgSQL_stmt_forc *) stmt;
					PLpgSQL_var *var = (PLpgSQL_var *) func->datums[stmt_forc->curvar];
					int		closing_local;
					List   *exceptions_local;

#if PG_VERSION_NUM >= 110000

					check_variable(cstate, stmt_forc->var);

#else

					check_row_or_rec(cstate, stmt_forc->row, stmt_forc->rec);

#endif

					check_expr(cstate, stmt_forc->argquery);

#if PG_VERSION_NUM >= 110000

					if (var->cursor_explicit_expr != NULL)
						check_assignment_to_variable(cstate, var->cursor_explicit_expr,
										 stmt_forc->var, -1);

#else

					if (var->cursor_explicit_expr != NULL)
						check_assignment(cstate, var->cursor_explicit_expr,
										 stmt_forc->rec, stmt_forc->row, -1);

#endif

					check_stmts(cstate, stmt_forc->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);

					cstate->used_variables = bms_add_member(cstate->used_variables,
										 stmt_forc->curvar);
				}
				break;

			case PLPGSQL_STMT_DYNFORS:
				{
					PLpgSQL_stmt_dynfors *stmt_dynfors = (PLpgSQL_stmt_dynfors *) stmt;
					int		closing_local;
					List   *exceptions_local;

#if PG_VERSION_NUM >= 110000

					check_variable(cstate, stmt_dynfors->var);

#else

					check_row_or_rec(cstate, stmt_dynfors->row, stmt_dynfors->rec);

#endif

					check_expr(cstate, stmt_dynfors->query);

					foreach(l, stmt_dynfors->params)
					{
						check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}

#if PG_VERSION_NUM >= 110000

					if (stmt_dynfors->var->dtype == PLPGSQL_DTYPE_REC)

#else

					if (stmt_dynfors->rec != NULL)

#endif

					{
						put_error(cstate,
									  0, 0,
								"cannot determinate a result of dynamic SQL",
									  "Cannot to contine in check.",
					  "Don't use dynamic SQL and record type together, when you would check function.",
									  PLPGSQL_CHECK_WARNING_OTHERS,
									  0, NULL, NULL);
					}

					check_stmts(cstate, stmt_dynfors->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_FOREACH_A:
				{
					PLpgSQL_stmt_foreach_a *stmt_foreach_a = (PLpgSQL_stmt_foreach_a *) stmt;
					bool use_element_type;
					int		closing_local;
					List   *exceptions_local;

					check_target(cstate, stmt_foreach_a->varno, NULL, NULL);

					/*
					 * When slice > 0, then result and target are a array.
					 * We shoudl to disable a array element refencing.
					 */
					use_element_type = stmt_foreach_a->slice == 0;

					check_assignment_with_possible_slices(cstate,
											 stmt_foreach_a->expr, 
											 NULL, NULL,
											 stmt_foreach_a->varno,
											 use_element_type);

					check_stmts(cstate, stmt_foreach_a->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_EXIT:
				{
					PLpgSQL_stmt_exit *stmt_exit = (PLpgSQL_stmt_exit *) stmt;

					check_expr_with_expected_scalar_type(cstate,
										     stmt_exit->cond,
										     BOOLOID,
										     false);

					if (stmt_exit->label != NULL)
					{
						PLpgSQL_stmt *labeled_stmt = find_stmt_with_label(stmt_exit->label,
												    outer_stmt);
						if (labeled_stmt == NULL)
							ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("label \"%s\" does not exist", stmt_exit->label)));

						/* CONTINUE only allows loop labels */
						if (!is_any_loop_stmt(labeled_stmt) && !stmt_exit->is_exit)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("block label \"%s\" cannot be used in CONTINUE",
									 stmt_exit->label)));
					}
					else
					{
						if (find_nearest_loop(outer_stmt) == NULL)
							ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("%s cannot be used outside a loop",
								 plpgsql_stmt_typename((PLpgSQL_stmt *) stmt_exit))));
					}
				}
				break;

			case PLPGSQL_STMT_PERFORM:
				check_expr(cstate, ((PLpgSQL_stmt_perform *) stmt)->expr);

				/*
				 * Note: if you want to raise warning when used expressions returns
				 * some value (other than VOID type), change previous command check_expr
				 * to following check_expr_with_expected_scalar_type. This should be 
				 * not enabled by default, because PERFORM can be used with reason
				 * to ignore result.
				 *
				 * check_expr_with_expected_scalar_type(cstate,
				 * 					     ((PLpgSQL_stmt_perform *) stmt)->expr,
				 * 					     VOIDOID,
				 * 					     true);
				 */

				break;

			case PLPGSQL_STMT_RETURN:
				{
					PLpgSQL_stmt_return *stmt_rt = (PLpgSQL_stmt_return *) stmt;

					if (stmt_rt->retvarno >= 0)
					{
						PLpgSQL_datum *retvar = cstate->estate->datums[stmt_rt->retvarno];
						PLpgSQL_execstate *estate = cstate->estate;

						cstate->used_variables = bms_add_member(cstate->used_variables, stmt_rt->retvarno);

						switch (retvar->dtype)
						{
							case PLPGSQL_DTYPE_VAR:
								{
									PLpgSQL_var *var = (PLpgSQL_var *) retvar;

									check_assign_to_target_type(cstate,
										 cstate->estate->func->fn_rettype, -1,
										 var->datatype->typoid, false);
								}
								break;

							case PLPGSQL_DTYPE_REC:
								{
									PLpgSQL_rec *rec = (PLpgSQL_rec *) retvar;

									if (recvar_tupdesc(rec) && estate->rsi && IsA(estate->rsi, ReturnSetInfo))
									{
										TupleDesc	rettupdesc = estate->rsi->expectedDesc;
										TupleConversionMap *tupmap ;

										tupmap = convert_tuples_by_position(recvar_tupdesc(rec), rettupdesc,
											 gettext_noop("returned record type does not match expected record type"));

										if (tupmap)
											free_conversion_map(tupmap);
									}
								}
								break;

							case PLPGSQL_DTYPE_ROW:
								{
									PLpgSQL_row *row = (PLpgSQL_row *) retvar;

									if (row->rowtupdesc && estate->rsi && IsA(estate->rsi, ReturnSetInfo))
									{
										TupleDesc	rettupdesc = estate->rsi->expectedDesc;
										TupleConversionMap *tupmap ;

										tupmap = convert_tuples_by_position(row->rowtupdesc, rettupdesc,
											 gettext_noop("returned record type does not match expected record type"));

										if (tupmap)
											free_conversion_map(tupmap);
									}
								}
								break;

							default:
								;		/* nope */
						}
					}

					*closing = PLPGSQL_CHECK_CLOSED;

					if (stmt_rt->expr)
						check_returned_expr(cstate, stmt_rt->expr, true);
				}
				break;

			case PLPGSQL_STMT_RETURN_NEXT:
				{
					PLpgSQL_stmt_return_next *stmt_rn = (PLpgSQL_stmt_return_next *) stmt;

					if (stmt_rn->retvarno >= 0)
					{
						PLpgSQL_datum *retvar = cstate->estate->datums[stmt_rn->retvarno];
						PLpgSQL_execstate *estate = cstate->estate;
						TupleDesc	tupdesc;
						int		natts;

						cstate->used_variables = bms_add_member(cstate->used_variables, stmt_rn->retvarno);

						if (!estate->retisset)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use RETURN NEXT in a non-SETOF function")));

#if PG_VERSION_NUM >= 110000

						tupdesc = estate->tuple_store_desc;

#else

						tupdesc = estate->rettupdesc;

#endif

						natts = tupdesc ? tupdesc->natts : 0;

						switch (retvar->dtype)
						{
							case PLPGSQL_DTYPE_VAR:
								{
									PLpgSQL_var *var = (PLpgSQL_var *) retvar;

									if (natts > 1)
										ereport(ERROR,
												(errcode(ERRCODE_DATATYPE_MISMATCH),
												 errmsg("wrong result type supplied in RETURN NEXT")));

									check_assign_to_target_type(cstate,
										 cstate->estate->func->fn_rettype, -1,
										 var->datatype->typoid, false);
								}
								break;

							case PLPGSQL_DTYPE_REC:
								{
									PLpgSQL_rec *rec = (PLpgSQL_rec *) retvar;
									TupleConversionMap *tupmap;

									if (!HeapTupleIsValid(recvar_tuple(rec)))
										ereport(ERROR,
												  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
												   errmsg("record \"%s\" is not assigned yet",
												   rec->refname),
										errdetail("The tuple structure of a not-yet-assigned"
															  " record is indeterminate.")));

									if (tupdesc)
									{
										tupmap = convert_tuples_by_position(recvar_tupdesc(rec),
																tupdesc,
											gettext_noop("wrong record type supplied in RETURN NEXT"));
										if (tupmap)
											free_conversion_map(tupmap);
									}
								}
								break;

							case PLPGSQL_DTYPE_ROW:
								{
									PLpgSQL_row *row = (PLpgSQL_row *) retvar;
									bool	row_is_valid_result;

									row_is_valid_result = true;

									if (tupdesc)
									{
										if (row->nfields == natts)
										{
											int		i;

											for (i = 0; i < natts; i++)
											{
												PLpgSQL_var *var;

												if (TupleDescAttr(tupdesc, i)->attisdropped)
													continue;
												if (row->varnos[i] < 0)
													elog(ERROR, "dropped rowtype entry for non-dropped column");

												var = (PLpgSQL_var *) (cstate->estate->datums[row->varnos[i]]);
												if (var->datatype->typoid != TupleDescAttr(tupdesc, i)->atttypid)
												{
													row_is_valid_result = false;
													break;
												}
											}
										}
										else
											row_is_valid_result = false;

										if (!row_is_valid_result)
											ereport(ERROR,
													(errcode(ERRCODE_DATATYPE_MISMATCH),
											errmsg("wrong record type supplied in RETURN NEXT")));
									}
								}
								break;

							default:
								;		/* nope */
						}
					}

					if (stmt_rn->expr)
						check_returned_expr(cstate, stmt_rn->expr, true);
				}
				break;

			case PLPGSQL_STMT_RETURN_QUERY:
				{
					PLpgSQL_stmt_return_query *stmt_rq = (PLpgSQL_stmt_return_query *) stmt;

					check_expr(cstate, stmt_rq->dynquery);

					if (stmt_rq->query)
					{
						check_returned_expr(cstate, stmt_rq->query, false);
						cstate->found_return_query = true;
					}

					foreach(l, stmt_rq->params)
					{
						check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}
				}
				break;

			case PLPGSQL_STMT_RAISE:
				{
					PLpgSQL_stmt_raise *stmt_raise = (PLpgSQL_stmt_raise *) stmt;
					ListCell   *current_param;
					char	   *cp;
					int			err_code = 0;

					if (stmt_raise->condname != NULL)
						err_code = plpgsql_recognize_err_condition(stmt_raise->condname, true);

					foreach(l, stmt_raise->params)
					{
						check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}

					foreach(l, stmt_raise->options)
					{
						PLpgSQL_raise_option *opt = (PLpgSQL_raise_option *) lfirst(l);

						check_expr(cstate, opt->expr);

						if (opt->opt_type == PLPGSQL_RAISEOPTION_ERRCODE)
						{
							bool		IsConst;
							char	   *value = ExprGetString(cstate, opt->expr, &IsConst);

							if (IsConst && value != NULL)
								err_code = plpgsql_recognize_err_condition(value, true);
							else
								err_code = -1;		/* cannot be calculated now */
						}
					}

					current_param = list_head(stmt_raise->params);

					/* ensure any single % has a own parameter */
					if (stmt_raise->message != NULL)
					{
						for (cp = stmt_raise->message; *cp; cp++)
						{
							if (cp[0] == '%')
							{
								if (cp[1] == '%')
								{
									cp++;
									continue;
								}
								if (current_param == NULL)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
											 errmsg("too few parameters specified for RAISE")));

								current_param = lnext(current_param);
							}
						}
					}
					if (current_param != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("too many parameters specified for RAISE")));

					if (stmt_raise->elog_level >= ERROR)
					{
						*closing = PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS;
						if (err_code == 0)
							err_code = ERRCODE_RAISE_EXCEPTION;
						else if (err_code == -1)
							err_code = 0; /* cannot be calculated */
						*exceptions = list_make1_int(err_code);
					}
					/* without any parameters it is reRAISE */
					if (stmt_raise->condname == NULL && stmt_raise->message == NULL &&
						stmt_raise->options == NIL)
					{
						*closing = PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS;
						/* should be enhanced in future */
						*exceptions = list_make1_int(-2); /* reRAISE */
					}
				}
				break;

			case PLPGSQL_STMT_EXECSQL:
				{
					PLpgSQL_stmt_execsql *stmt_execsql = (PLpgSQL_stmt_execsql *) stmt;

					if (stmt_execsql->into)
					{

#if PG_VERSION_NUM >= 110000

						check_variable(cstate, stmt_execsql->target);
						check_assignment_to_variable(cstate, stmt_execsql->sqlstmt,
													  stmt_execsql->target, -1);

#else

						check_row_or_rec(cstate, stmt_execsql->row, stmt_execsql->rec);
						check_assignment(cstate, stmt_execsql->sqlstmt,
								   stmt_execsql->rec, stmt_execsql->row, -1);

#endif

					}
					else
						/* only statement */
						check_expr_as_sqlstmt_nodata(cstate, stmt_execsql->sqlstmt);
				}
				break;

			case PLPGSQL_STMT_DYNEXECUTE:
				{
					PLpgSQL_stmt_dynexecute *stmt_dynexecute = (PLpgSQL_stmt_dynexecute *) stmt;

					check_expr(cstate, stmt_dynexecute->query);

					foreach(l, stmt_dynexecute->params)
					{
						check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}

					if (stmt_dynexecute->into)
					{

#if PG_VERSION_NUM >= 110000

						check_variable(cstate, stmt_dynexecute->target);

						if (stmt_dynexecute->target->dtype == PLPGSQL_DTYPE_REC)

#else

						check_row_or_rec(cstate, stmt_dynexecute->row, stmt_dynexecute->rec);

						if (stmt_dynexecute->rec != NULL)

#endif

						{
							put_error(cstate,
										  0, 0,
								"cannot determinate a result of dynamic SQL",
										  "Cannot to contine in check.",
						  "Don't use dynamic SQL and record type together, when you would check function.",
										  PLPGSQL_CHECK_WARNING_OTHERS,
										  0, NULL, NULL);
						}
					}
				}
				break;

			case PLPGSQL_STMT_OPEN:
				{
					PLpgSQL_stmt_open *stmt_open = (PLpgSQL_stmt_open *) stmt;
					PLpgSQL_var *var = (PLpgSQL_var *) (cstate->estate->datums[stmt_open->curvar]);

					if (var->cursor_explicit_expr)
						check_expr_as_sqlstmt_data(cstate, var->cursor_explicit_expr);

					if (stmt_open->query)
						check_expr_as_sqlstmt_data(cstate, stmt_open->query);

					if (var != NULL && stmt_open->query != NULL)
						var->cursor_explicit_expr = stmt_open->query;

					check_expr(cstate, stmt_open->argquery);
					check_expr(cstate, stmt_open->dynquery);
					foreach(l, stmt_open->params)
					{
						check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}

					cstate->used_variables = bms_add_member(cstate->used_variables,
									 stmt_open->curvar);

				}
				break;

			case PLPGSQL_STMT_GETDIAG:
				{
					PLpgSQL_stmt_getdiag *stmt_getdiag = (PLpgSQL_stmt_getdiag *) stmt;
					ListCell   *lc;

					foreach(lc, stmt_getdiag->diag_items)
					{
						PLpgSQL_diag_item *diag_item = (PLpgSQL_diag_item *) lfirst(lc);

						check_target(cstate, diag_item->target, NULL, NULL);
					}
				}
				break;

			case PLPGSQL_STMT_FETCH:
				{
					PLpgSQL_stmt_fetch *stmt_fetch = (PLpgSQL_stmt_fetch *) stmt;
					PLpgSQL_var *var = (PLpgSQL_var *) (cstate->estate->datums[stmt_fetch->curvar]);

#if PG_VERSION_NUM >= 110000

					check_variable(cstate, stmt_fetch->target);

					if (var != NULL && var->cursor_explicit_expr != NULL)
						check_assignment_to_variable(cstate, var->cursor_explicit_expr,
									   stmt_fetch->target, -1);

#else

					check_row_or_rec(cstate, stmt_fetch->row, stmt_fetch->rec);

					if (var != NULL && var->cursor_explicit_expr != NULL)
						check_assignment(cstate, var->cursor_explicit_expr,
									   stmt_fetch->rec, stmt_fetch->row, -1);

#endif

					check_expr(cstate, stmt_fetch->expr);

					cstate->used_variables = bms_add_member(cstate->used_variables, stmt_fetch->curvar);
				}
				break;

			case PLPGSQL_STMT_CLOSE:
				cstate->used_variables = bms_add_member(cstate->used_variables,
								 ((PLpgSQL_stmt_close *) stmt)->curvar);

				break;

#if PG_VERSION_NUM >= 110000

			case PLPGSQL_STMT_SET:
				/*
				 * We can not check this now, syntax should be ok.
				 * The expression there has not plan.
				 */
				break;

			case PLPGSQL_STMT_COMMIT:
			case PLPGSQL_STMT_ROLLBACK:
				/* These commands are allowed only in procedures */
				if (!is_procedure(cstate->estate))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION),
							 errmsg("invalid transaction termination")));
				break;

			case PLPGSQL_STMT_CALL:
				{
					PLpgSQL_stmt_call *stmt_call = (PLpgSQL_stmt_call *) stmt;
					PLpgSQL_row *target;
					bool		has_data;

					has_data = check_expr_as_sqlstmt(cstate, stmt_call->expr);

					/* any check_expr_xxx should be called before CallExprGetRowTarget */
					target = CallExprGetRowTarget(cstate, stmt_call->expr);

					if (has_data != (target != NULL))
						elog(ERROR, "plpgsql internal error, broken CALL statement");

					if (target != NULL)
					{
						check_variable(cstate, (PLpgSQL_variable *) target);
						check_assignment_to_variable(cstate, stmt_call->expr,
																(PLpgSQL_variable *) target, -1);

						pfree(target->varnos);
						pfree(target);
					}
				}
				break;

#endif

			default:
				elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
		}

		pop_stmt_from_stmt_stack(cstate);

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		SPI_restore_connection();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldCxt);
		edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		pop_stmt_from_stmt_stack(cstate);

		/*
		 * If fatal_errors is true, we just propagate the error up to the
		 * highest level. Otherwise the error is appended to our current list
		 * of errors, and we continue checking.
		 */
		if (cstate->fatal_errors)
			ReThrowError(edata);
		else
			put_error_edata(cstate, edata);
		MemoryContextSwitchTo(oldCxt);

		/* reconnect spi */
		SPI_restore_connection();
	}
	PG_END_TRY();
}

/*
 * Ensure check for all statements in list
 *
 */
static void
check_stmts(PLpgSQL_checkstate *cstate, List *stmts, int *closing, List **exceptions)
{
	ListCell   *lc;
	int			closing_local;
	List	   *exceptions_local;
	bool		dead_code_alert = false;

	*closing = PLPGSQL_CHECK_UNCLOSED;
	*exceptions = NIL;

	foreach(lc, stmts)
	{
		PLpgSQL_stmt	   *stmt = (PLpgSQL_stmt *) lfirst(lc);

		closing_local = PLPGSQL_CHECK_UNCLOSED;
		exceptions_local = NIL;
		check_stmt(cstate, stmt, &closing_local, &exceptions_local);

		if (dead_code_alert)
		{
			put_error(cstate,
						  0, stmt->lineno,
						  "unreachable code",
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);
			/* don't raise this warning every line */
			dead_code_alert = false;
		}

		if (closing_local == PLPGSQL_CHECK_CLOSED)
		{
			dead_code_alert = true;
			*closing = PLPGSQL_CHECK_CLOSED;
			*exceptions = NIL;
		}
		else if (closing_local == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
		{
			dead_code_alert = true;
			if (*closing == PLPGSQL_CHECK_UNCLOSED ||
				*closing == PLPGSQL_CHECK_POSSIBLY_CLOSED ||
				*closing == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
			{
				*closing = PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS;
				*exceptions = exceptions_local;
			}
		}
		else if (closing_local == PLPGSQL_CHECK_POSSIBLY_CLOSED)
		{
			if (*closing == PLPGSQL_CHECK_UNCLOSED)
			{
				*closing = PLPGSQL_CHECK_POSSIBLY_CLOSED;
				*exceptions = NIL;
			}
		}
	}
}

static int
possibly_closed(int c)
{
	switch (c)
	{
		case PLPGSQL_CHECK_CLOSED:
		case PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS:
		case PLPGSQL_CHECK_POSSIBLY_CLOSED:
			return PLPGSQL_CHECK_POSSIBLY_CLOSED;
		default:
			return PLPGSQL_CHECK_UNCLOSED;
	}
}

static int
merge_closing(int c, int c_local, List **exceptions, List *exceptions_local, int err_code)
{
	*exceptions = NIL;

	if (c == PLPGSQL_CHECK_UNKNOWN)
	{
		if (c_local == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
			*exceptions = exceptions_local;

		return c_local;
	}

	if (c_local == PLPGSQL_CHECK_UNKNOWN)
		return c;

	if (c == c_local)
	{
		if (c == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
		{

			if (err_code != -1)
			{
				ListCell *lc;

				/* replace reRAISE symbol (-2) by real err_code */
				foreach(lc, exceptions_local)
				{
					int		t_err_code = lfirst_int(lc);

					*exceptions = list_append_unique_int(*exceptions,
														t_err_code != -2 ? t_err_code : err_code);
				}
			}
			else
				*exceptions = list_concat_unique_int(*exceptions, exceptions_local);
		}

		return c_local;
	}

	if (c == PLPGSQL_CHECK_CLOSED || c_local == PLPGSQL_CHECK_CLOSED)
	{
		if (c == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS ||
			c_local == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
		return PLPGSQL_CHECK_CLOSED;
	}

	return PLPGSQL_CHECK_POSSIBLY_CLOSED;
}

static bool
exception_matches_conditions(int sqlerrstate, PLpgSQL_condition *cond)
{
	for (; cond != NULL; cond = cond->next)
	{
		int			_sqlerrstate = cond->sqlerrstate;

		/*
		 * OTHERS matches everything *except* query-canceled and
		 * assert-failure.  If you're foolish enough, you can match those
		 * explicitly.
		 */
		if (_sqlerrstate == 0)
		{
			if (sqlerrstate != ERRCODE_QUERY_CANCELED

#if PG_VERSION_NUM >= 90500

				 && sqlerrstate != ERRCODE_ASSERT_FAILURE

#endif

				)
				return true;
		}
		/* Exact match? */
		else if (sqlerrstate == _sqlerrstate)
			return true;
		/* Category match? */
		else if (ERRCODE_IS_CATEGORY(_sqlerrstate) &&
				 ERRCODE_TO_CATEGORY(sqlerrstate) == _sqlerrstate)
			return true;
	}
	return false;
}

/*
 * Verify a expression
 *
 */
static void
check_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	if (expr)
		check_expr_as_rvalue(cstate, expr, NULL, NULL, -1, false, true);
}

static void
record_variable_usage(PLpgSQL_checkstate *cstate, int dno, bool write)
{
	if (dno >= 0)
	{
		if (!write)
			cstate->used_variables = bms_add_member(cstate->used_variables, dno);
		else
			cstate->modif_variables = bms_add_member(cstate->modif_variables, dno);
	}
}

static bool
is_internal(char *refname, int lineno)
{
	if (lineno <= 0)
		return true;
	if (refname == NULL)
		return true;
	if (strcmp(refname, "*internal*") == 0)
		return true;
	if (strcmp(refname, "(unnamed row)") == 0)
		return true;
	return false;
}

static bool
is_internal_variable(PLpgSQL_variable *var)
{
	return is_internal(var->refname, var->lineno);
}


/*
 * Returns true if dno is explicitly declared. It should not be used
 * for arguments.
 */
static bool
datum_is_explicit(PLpgSQL_checkstate *cstate, int dno)
{
	PLpgSQL_execstate *estate = cstate->estate;

	switch (estate->datums[dno]->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[dno];
				return !is_internal(var->refname, var->lineno);
			}

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) estate->datums[dno];
				return !is_internal(row->refname, row->lineno);
			}
		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) estate->datums[dno];
				return !is_internal(rec->refname, rec->lineno);
			}

		default:
			return false;
	}
}

/*
 * returns true, when datum or some child is used
 */
static bool
datum_is_used(PLpgSQL_checkstate *cstate, int dno, bool write)
{
	PLpgSQL_execstate *estate = cstate->estate;

	switch (estate->datums[dno]->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				return bms_is_member(dno,
						write ? cstate->modif_variables : cstate->used_variables);
			}
			break;

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) estate->datums[dno];
				int	     i;

				if (bms_is_member(dno,
						  write ? cstate->modif_variables : cstate->used_variables))
					return true;

				for (i = 0; i < row->nfields; i++)
				{
					if (row->varnos[i] < 0)
						continue;

					if (datum_is_used(cstate, row->varnos[i], write))
						return true;
				}

				return false;
			}
			break;

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) estate->datums[dno];
				int	     i;

				if (bms_is_member(dno,
						  write ? cstate->modif_variables : cstate->used_variables))
					return true;

				/* search any used recfield with related recparentno */
				for (i = 0; i < estate->ndatums; i++)
				{
					if (estate->datums[i]->dtype == PLPGSQL_DTYPE_RECFIELD)
					{
						PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) estate->datums[i];

						if (recfield->recparentno == rec->dno
								    && datum_is_used(cstate, i, write))
							return true;
					}
				}
			}
			break;

		case PLPGSQL_DTYPE_RECFIELD:
			return bms_is_member(dno,
					write ? cstate->modif_variables : cstate->used_variables);

		default:
			return false;
	}

	return false;
}

#define UNUSED_VARIABLE_TEXT			"unused variable \"%s\""
#define UNUSED_VARIABLE_TEXT_CHECK_LENGTH	15
#define NEVER_READ_VARIABLE_TEXT		"never read variable \"%s\""
#define NEVER_READ_VARIABLE_TEXT_CHECK_LENGTH		19
#define UNUSED_PARAMETER_TEXT			"unused parameter \"%s\""
#define NEVER_READ_PARAMETER_TEXT		"parameter \"%s\" is never read"
#define UNMODIFIED_VARIABLE_TEXT		"unmodified OUT variable \"%s\""
#define OUT_COMPOSITE_IS_NOT_SINGLE_TEXT	"composite OUT variable \"%s\" is not single argument"

/*
 * Reports all unused variables explicitly DECLAREd by the user.  Ignores
 * special variables created by PL/PgSQL.
 */
static void
report_unused_variables(PLpgSQL_checkstate *cstate)
{
	int i;
	PLpgSQL_execstate *estate = cstate->estate;

	/* now, there are no active plpgsql statement */
	estate->err_stmt = NULL;

	for (i = 0; i < estate->ndatums; i++)
		if (datum_is_explicit(cstate, i) &&
			!(datum_is_used(cstate, i, false) || datum_is_used(cstate, i, true)))
		{
			PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[i];
			StringInfoData message;

			initStringInfo(&message);

			appendStringInfo(&message, UNUSED_VARIABLE_TEXT, var->refname);
			put_error(cstate,
					  0, var->lineno,
					  message.data,
					  NULL,
					  NULL,
					  PLPGSQL_CHECK_WARNING_OTHERS,
					  0, NULL, NULL);

			pfree(message.data);
			message.data = NULL;
		}

	if (cstate->extra_warnings)
	{
		PLpgSQL_function *func = estate->func;

		/* check never read variables */

		for (i = 0; i < estate->ndatums; i++)
		{
			if (datum_is_explicit(cstate, i)
				 && !datum_is_used(cstate, i, false) && datum_is_used(cstate, i, true))
			{
				PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[i];
				StringInfoData message;

				initStringInfo(&message);

				appendStringInfo(&message, NEVER_READ_VARIABLE_TEXT, var->refname);
				put_error(cstate,
						  0, var->lineno,
						  message.data,
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);

				pfree(message.data);
				message.data = NULL;
			}
		}

		/* check IN parameters */
		for (i = 0; i < func->fn_nargs; i++)
		{
			int		varno = func->fn_argvarnos[i];

			bool	is_read = datum_is_used(cstate, varno, false);
			bool	is_write = datum_is_used(cstate, varno, true);

			if (!is_read)
			{
				PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[varno];
				StringInfoData message;

				initStringInfo(&message);

				appendStringInfo(&message, NEVER_READ_PARAMETER_TEXT, var->refname);
				put_error(cstate,
						  0, 0,
						  message.data,
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);

				pfree(message.data); message.data = NULL;
			}
			else if (!(is_read || is_write))
			{
				PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[varno];
				StringInfoData message;

				initStringInfo(&message);

				appendStringInfo(&message, UNUSED_PARAMETER_TEXT, var->refname);
				put_error(cstate,
						  0, 0,
						  message.data,
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);

				pfree(message.data); message.data = NULL;
			}
		}

		/* are there some OUT parameters (expect modification)? */
		if (func->out_param_varno != -1 && !cstate->found_return_query)
		{
			int		varno = func->out_param_varno;
			PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[varno];

			if (var->dtype == PLPGSQL_DTYPE_ROW && is_internal_variable(var))
			{
				/* this function has more OUT parameters */
				PLpgSQL_row *row = (PLpgSQL_row*) var;
				int		fnum;

				for (fnum = 0; fnum < row->nfields; fnum++)
				{
					int		varno2 = row->varnos[fnum];
					PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[varno2];
					StringInfoData message;

					if (var->dtype == PLPGSQL_DTYPE_ROW ||
						  var->dtype == PLPGSQL_DTYPE_REC)
					{
						initStringInfo(&message);
						appendStringInfo(&message,
									  OUT_COMPOSITE_IS_NOT_SINGLE_TEXT, var->refname);
						put_error(cstate,
								  0, 0,
								  message.data,
								  NULL,
								  NULL,
								  PLPGSQL_CHECK_WARNING_EXTRA,
								  0, NULL, NULL);

						pfree(message.data);
						message.data = NULL;
					}

					if (!datum_is_used(cstate, varno2, true))
					{
						initStringInfo(&message);
						appendStringInfo(&message, UNMODIFIED_VARIABLE_TEXT, var->refname);
						put_error(cstate,
								  0, 0,
								  message.data,
								  NULL,
								  NULL,
								  PLPGSQL_CHECK_WARNING_EXTRA,
								  0, NULL, NULL);

						pfree(message.data);
						message.data = NULL;
					}
				}
			}
			else
			{
				if (!datum_is_used(cstate, varno, true))
				{
					PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[varno];
					StringInfoData message;

					initStringInfo(&message);

					appendStringInfo(&message, UNMODIFIED_VARIABLE_TEXT, var->refname);
					put_error(cstate,
							  0, 0,
							  message.data,
							  NULL,
							  NULL,
							  PLPGSQL_CHECK_WARNING_EXTRA,
							  0, NULL, NULL);

					pfree(message.data);
					message.data = NULL;
				}
			}
		}
	}
}

/*
 * Report too high volatility
 *
 */
static void
report_too_high_volatility(PLpgSQL_checkstate *cstate)
{
	if (cstate->performance_warnings)
	{
		char	   *current;
		char	   *should_be;
		bool 		raise_warning;

		if (cstate->volatility == PROVOLATILE_IMMUTABLE &&
				(cstate->decl_volatility == PROVOLATILE_VOLATILE ||
				 cstate->decl_volatility == PROVOLATILE_STABLE))
		{
			should_be = "IMMUTABLE";
			current = cstate->decl_volatility == PROVOLATILE_VOLATILE ?
						"VOLATILE" : "STABLE";
			raise_warning = true;
		}
		else if (cstate->volatility == PROVOLATILE_STABLE &&
				(cstate->decl_volatility == PROVOLATILE_VOLATILE))
		{
			should_be = "STABLE";
			current = "VOLATILE";
			raise_warning = true;
		}
		else
			raise_warning = false;

		if (raise_warning)
		{
			StringInfoData message;

			initStringInfo(&message);

			appendStringInfo(&message, "routine is marked as %s, should be %s", current, should_be);
			put_error(cstate,
					  0, -1,
					  message.data,
					  NULL,
					  "When you fix this issue, please, recheck other functions that uses this function.",
					  PLPGSQL_CHECK_WARNING_PERFORMANCE,
					  0, NULL, NULL);

			pfree(message.data);
			message.data = NULL;
		}
	}
}

/*
 * Verify an assignment of 'expr' to 'target'
 *
 */
static void
check_assignment(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
				 PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
				 int targetdno)
{
	bool		is_expression = (targetrec == NULL && targetrow == NULL);

	check_expr_as_rvalue(cstate, expr, targetrec, targetrow, targetdno, false,
						  is_expression);
}

#if PG_VERSION_NUM >= 110000

static void
check_assignment_to_variable(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
				 PLpgSQL_variable *targetvar, int targetdno)
{
	if (targetvar != NULL)
	{
		if (targetvar->dtype == PLPGSQL_DTYPE_ROW)
			check_expr_as_rvalue(cstate, expr, NULL, (PLpgSQL_row *) targetvar, targetdno,
								  false, false);
		else if (targetvar->dtype == PLPGSQL_DTYPE_REC)
			check_expr_as_rvalue(cstate, expr, (PLpgSQL_rec *) targetvar, NULL, targetdno,
								  false, false);
		else
			elog(ERROR, "unsupported target variable type");
	}
	else
	{
		check_expr_as_rvalue(cstate, expr, NULL, NULL, targetdno, false, true);
	}
}

#endif

/*
 * Verify an assignment of 'expr' to 'target' with possible slices
 *
 * it is used in FOREACH ARRAY where SLICE change a target type
 *
 */
static void
check_assignment_with_possible_slices(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
				 PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
				 int targetdno, bool use_element_type)
{
	bool		is_expression = (targetrec == NULL && targetrow == NULL);

	check_expr_as_rvalue(cstate, expr, targetrec, targetrow, targetdno, use_element_type,
						  is_expression);
}

/*
 * Verify to possible cast to bool, integer, ..
 *
 */
static void
check_expr_with_expected_scalar_type(PLpgSQL_checkstate *cstate,
								 PLpgSQL_expr *expr,
								 Oid expected_typoid,
								 bool required)
{
	ResourceOwner oldowner;
	MemoryContext oldCxt = CurrentMemoryContext;

	if (!expr)
	{
		if (required)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("required expression is empty")));

		return;
	}

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		TupleDesc	tupdesc;
		bool		is_immutable_null;

		prepare_expr(cstate, expr, 0);
		/* record all variables used by the query */
		cstate->used_variables = bms_add_members(cstate->used_variables, expr->paramnos);

		tupdesc = expr_get_desc(cstate, expr, false, true, true, NULL);
		is_immutable_null = is_const_null_expr(cstate, expr);

		if (tupdesc)
		{
			/* when we know value or type */
			if (!is_immutable_null)
				check_assign_to_target_type(cstate,
								    expected_typoid, -1,
								    TupleDescAttr(tupdesc, 0)->atttypid,
								    is_immutable_null);
		}

		ReleaseTupleDesc(tupdesc);

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		SPI_restore_connection();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldCxt);
		edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		/*
		 * If fatal_errors is true, we just propagate the error up to the
		 * highest level. Otherwise the error is appended to our current list
		 * of errors, and we continue checking.
		 */
		if (cstate->fatal_errors)
			ReThrowError(edata);
		else
			put_error_edata(cstate, edata);
		MemoryContextSwitchTo(oldCxt);

		/* reconnect spi */
		SPI_restore_connection();
	}
	PG_END_TRY();
}

/*
 * Checks used for RETURN QUERY
 *
 */
static void
check_returned_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool is_expression)
{
	PLpgSQL_execstate *estate = cstate->estate;
	PLpgSQL_function *func = estate->func;
	bool		is_return_query = !is_expression;

	ResourceOwner oldowner;
	MemoryContext oldCxt = CurrentMemoryContext;

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		TupleDesc	tupdesc;
		bool		is_immutable_null;
		Oid			first_level_typ = InvalidOid;

		prepare_expr(cstate, expr, 0);
		/* record all variables used by the query */
		cstate->used_variables = bms_add_members(cstate->used_variables, expr->paramnos);

		tupdesc = expr_get_desc(cstate, expr, false, true, is_expression, &first_level_typ);
		is_immutable_null = is_const_null_expr(cstate, expr);

		if (tupdesc)
		{
			/* enforce check for trigger function - result must be composit */
			if (func->fn_retistuple && is_expression 
						  && !(type_is_rowtype(TupleDescAttr(tupdesc, 0)->atttypid) ||
							   type_is_rowtype(first_level_typ) || tupdesc->natts > 1))
			{
				/* but we should to allow NULL */
				if (!is_immutable_null)
					put_error(cstate,
								ERRCODE_DATATYPE_MISMATCH, 0,
					"cannot return non-composite value from function returning composite type",
												NULL,
												NULL,
												PLPGSQL_CHECK_ERROR,
												0, NULL, NULL);
			}
			/* tupmap is used when function returns tuple or RETURN QUERY was used */
			else if (func->fn_retistuple || is_return_query)
			{
				/* should to know expected result */
				if (estate->rsi && IsA(estate->rsi, ReturnSetInfo))
				{
					TupleDesc	rettupdesc = estate->rsi->expectedDesc;
					TupleConversionMap *tupmap ;

					tupmap = convert_tuples_by_position(tupdesc, rettupdesc,
			    !is_expression ? gettext_noop("structure of query does not match function result type")
			                   : gettext_noop("returned record type does not match expected record type"));

					if (tupmap)
						free_conversion_map(tupmap);
				}
			}
			else
			{
				/* returns scalar */
				if (!IsPolymorphicType(func->fn_rettype))
				{
					check_assign_to_target_type(cstate,
									    func->fn_rettype, -1,
									    TupleDescAttr(tupdesc, 0)->atttypid,
									    is_immutable_null);
				}
			}

			ReleaseTupleDesc(tupdesc);
		}

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		SPI_restore_connection();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldCxt);
		edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		/*
		 * If fatal_errors is true, we just propagate the error up to the
		 * highest level. Otherwise the error is appended to our current list
		 * of errors, and we continue checking.
		 */
		if (cstate->fatal_errors)
			ReThrowError(edata);
		else
			put_error_edata(cstate, edata);
		MemoryContextSwitchTo(oldCxt);

		/* reconnect spi */
		SPI_restore_connection();
	}
	PG_END_TRY();
}

/*
 * Check expression as rvalue - on right in assign statement. It is used for
 * only expression check too - when target is unknown.
 *
 */
static void
check_expr_as_rvalue(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
					  PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
					int targetdno, bool use_element_type, bool is_expression)
{
	ResourceOwner oldowner;
	MemoryContext oldCxt = CurrentMemoryContext;
	TupleDesc	tupdesc;
	bool is_immutable_null;
	bool			expand = true;
	Oid			first_level_typoid;
	Oid expected_typoid = InvalidOid;
	int expected_typmod = InvalidOid;

	if (targetdno != -1)
	{
		check_target(cstate, targetdno, &expected_typoid, &expected_typmod);

		/*
		 * When target variable is not compossite, then we should not
		 * to expand result tupdesc.
		 */
		if (!type_is_rowtype(expected_typoid))
			expand = false;
	}

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		prepare_expr(cstate, expr, 0);
		/* record all variables used by the query */
		cstate->used_variables = bms_add_members(cstate->used_variables, expr->paramnos);

		tupdesc = expr_get_desc(cstate, expr, use_element_type, expand, is_expression, &first_level_typoid);
		is_immutable_null = is_const_null_expr(cstate, expr);

		if (expected_typoid != InvalidOid && type_is_rowtype(expected_typoid) && first_level_typoid != InvalidOid)
		{
			/* simple error, scalar source to composite target */
			if (!type_is_rowtype(first_level_typoid) && !is_immutable_null)
			{
				put_error(cstate,
						  ERRCODE_DATATYPE_MISMATCH, 0,
							  "cannot assign scalar variable to composite target",
							  NULL,
							  NULL,
							  PLPGSQL_CHECK_ERROR,
							  0, NULL, NULL);

				goto no_other_check;
			}

			/* simple ok, target and source composite types are same */
			if (type_is_rowtype(first_level_typoid)
				    && first_level_typoid != RECORDOID && first_level_typoid == expected_typoid)
				goto no_other_check;
		}

		if (tupdesc)
		{
			if (targetrow != NULL || targetrec != NULL)
				assign_tupdesc_row_or_rec(cstate, targetrow, targetrec, tupdesc, is_immutable_null);
			if (targetdno != -1)
				assign_tupdesc_dno(cstate, targetdno, tupdesc, is_immutable_null);

			if (targetrow)
			{
				if (RowGetValidFields(targetrow) > TupleDescNVatts(tupdesc))
					put_error(cstate,
								  0, 0,
								  "too few attributes for target variables",
								  "There are more target variables than output columns in query.",
						  "Check target variables in SELECT INTO statement.",
								  PLPGSQL_CHECK_WARNING_OTHERS,
								  0, NULL, NULL);
				else if (RowGetValidFields(targetrow) < TupleDescNVatts(tupdesc))
					put_error(cstate,
								  0, 0,
								  "too many attributes for target variables",
								  "There are less target variables than output columns in query.",
						   "Check target variables in SELECT INTO statement",
								  PLPGSQL_CHECK_WARNING_OTHERS,
								  0, NULL, NULL);
			}
		}

no_other_check:
		if (tupdesc)
			ReleaseTupleDesc(tupdesc);

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		SPI_restore_connection();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldCxt);
		edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		/*
		 * If fatal_errors is true, we just propagate the error up to the
		 * highest level. Otherwise the error is appended to our current list
		 * of errors, and we continue checking.
		 */
		if (cstate->fatal_errors)
			ReThrowError(edata);
		else
			put_error_edata(cstate, edata);
		MemoryContextSwitchTo(oldCxt);

		/* reconnect spi */
		SPI_restore_connection();
	}
	PG_END_TRY();
}

/*
 * Check a SQL statement, should not to return data
 *
 */
static void
check_expr_as_sqlstmt_nodata(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	if (check_expr_as_sqlstmt(cstate, expr))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("query has no destination for result data")));
}

/*
 * Check a SQL statement, should to return data
 *
 */
static void
check_expr_as_sqlstmt_data(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	if (!check_expr_as_sqlstmt(cstate, expr))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("query does not return data")));
}


/*
 * Check a SQL statement, can (not) returs data. Returns true
 * when statement returns data - we are able to get tuple descriptor.
 */
static bool
check_expr_as_sqlstmt(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	ResourceOwner oldowner;
	MemoryContext oldCxt = CurrentMemoryContext;
	TupleDesc	tupdesc;
	volatile bool result = false;

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		prepare_expr(cstate, expr, 0);
		/* record all variables used by the query */
		cstate->used_variables = bms_add_members(cstate->used_variables, expr->paramnos);

		tupdesc = expr_get_desc(cstate, expr, false, false, false, NULL);
		if (tupdesc)
		{
			result = true;
			ReleaseTupleDesc(tupdesc);
		}

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		SPI_restore_connection();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldCxt);
		edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		/*
		 * If fatal_errors is true, we just propagate the error up to the
		 * highest level. Otherwise the error is appended to our current list
		 * of errors, and we continue checking.
		 */
		if (cstate->fatal_errors)
			ReThrowError(edata);
		else
			put_error_edata(cstate, edata);
		MemoryContextSwitchTo(oldCxt);

		/* reconnect spi */
		SPI_restore_connection();
	}
	PG_END_TRY();

	return result;
}


#if PG_VERSION_NUM >= 110000

static void
check_variable(PLpgSQL_checkstate *cstate, PLpgSQL_variable *var)
{
	/* leave quickly when var is not defined */
	if (var == NULL)
		return;

	if (var->dtype == PLPGSQL_DTYPE_ROW)
	{
		PLpgSQL_row *row = (PLpgSQL_row *) var;
		int		fnum;

		for (fnum = 0; fnum < row->nfields; fnum++)
		{
			/* skip dropped columns */
			if (row->varnos[fnum] < 0)
				continue;

			check_target(cstate, row->varnos[fnum], NULL, NULL);
		}
		record_variable_usage(cstate, row->dno, true);

		return;
	}

	if (var->dtype == PLPGSQL_DTYPE_REC)
	{
		PLpgSQL_rec *rec = (PLpgSQL_rec *) var;

		/*
		 * There are no checks done on records currently; just record that the
		 * variable is not unused.
		 */
		record_variable_usage(cstate, rec->dno, true);

		return;
	}

	elog(ERROR, "unsupported dtype %d", var->dtype);
}

#endif

/*
 * Check composed lvalue There is nothing to check on rec variables
 */
static void
check_row_or_rec(PLpgSQL_checkstate *cstate, PLpgSQL_row *row, PLpgSQL_rec *rec)
{
	int			fnum;

	if (row != NULL)
	{

		for (fnum = 0; fnum < row->nfields; fnum++)
		{
			/* skip dropped columns */
			if (row->varnos[fnum] < 0)
				continue;

			check_target(cstate, row->varnos[fnum], NULL, NULL);
		}
		record_variable_usage(cstate, row->dno, true);
	}
	else if (rec != NULL)
	{
		/*
		 * There are no checks done on records currently; just record that the
		 * variable is not unused.
		 */
		record_variable_usage(cstate, rec->dno, true);
	}
}

/*
 * Verify lvalue It doesn't repeat a checks that are done. Checks a subscript
 * expressions, verify a validity of record's fields.
 */
static void
check_target(PLpgSQL_checkstate *cstate, int varno, Oid *expected_typoid, int *expected_typmod)
{
	PLpgSQL_datum *target = cstate->estate->datums[varno];

	record_variable_usage(cstate, varno, true);

	switch (target->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) target;
				PLpgSQL_type *tp = var->datatype;

				if (expected_typoid != NULL)
					*expected_typoid = tp->typoid;
				if (expected_typmod != NULL)
					*expected_typmod = tp->atttypmod;
			}
			break;

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) target;

#if PG_VERSION_NUM >= 110000

				if (rec->rectypeid != RECORDOID)
				{
					if (expected_typoid != NULL)
						*expected_typoid = rec->rectypeid;
					if (expected_typmod != NULL)
						*expected_typmod = -1;
				}
				else

#endif

				if (recvar_tupdesc(rec) != NULL)
				{
					if (expected_typoid != NULL)
						*expected_typoid = recvar_tupdesc(rec)->tdtypeid;
					if (expected_typmod != NULL)
						*expected_typmod = recvar_tupdesc(rec)->tdtypmod;
				}
				else
				{
					if (expected_typoid != NULL)
						*expected_typoid = RECORDOID;
					if (expected_typmod != NULL)
						*expected_typmod = -1;
				}
			}
			break;

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) target;

				if (row->rowtupdesc != NULL)
				{
					if (expected_typoid != NULL)
						*expected_typoid = row->rowtupdesc->tdtypeid;
					if (expected_typmod != NULL)
						*expected_typmod = row->rowtupdesc->tdtypmod;
				}
				else
				{
					if (expected_typoid != NULL)
						*expected_typoid = RECORDOID;
					if (expected_typmod != NULL)
						*expected_typmod = -1;
				}

				check_row_or_rec(cstate, row, NULL);

			}
			break;

		case PLPGSQL_DTYPE_RECFIELD:
			{
				PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) target;
				PLpgSQL_rec *rec;
				int			fno;

				rec = (PLpgSQL_rec *) (cstate->estate->datums[recfield->recparentno]);

				/*
				 * Check that there is already a tuple in the record. We need
				 * that because records don't have any predefined field
				 * structure.
				 */
				if (!HeapTupleIsValid(recvar_tuple(rec)))
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("record \"%s\" is not assigned to tuple structure",
						   rec->refname)));

				/*
				 * Get the number of the records field to change and the
				 * number of attributes in the tuple.  Note: disallow system
				 * column names because the code below won't cope.
				 */
				fno = SPI_fnumber(recvar_tupdesc(rec), recfield->fieldname);
				if (fno <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("record \"%s\" has no field \"%s\"",
									rec->refname, recfield->fieldname)));

				if (expected_typoid)
					*expected_typoid = SPI_gettypeid(recvar_tupdesc(rec), fno);

				if (expected_typmod)
					*expected_typmod = TupleDescAttr(recvar_tupdesc(rec), fno - 1)->atttypmod;
			}
			break;

		case PLPGSQL_DTYPE_ARRAYELEM:
			{
				/*
				 * Target is an element of an array
				 */
				int			nsubscripts;
				Oid			arrayelemtypeid;
				Oid			arraytypeid;

				/*
				 * To handle constructs like x[1][2] := something, we have to
				 * be prepared to deal with a chain of arrayelem datums. Chase
				 * back to find the base array datum, and save the subscript
				 * expressions as we go.  (We are scanning right to left here,
				 * but want to evaluate the subscripts left-to-right to
				 * minimize surprises.)
				 */
				nsubscripts = 0;
				do
				{
					PLpgSQL_arrayelem *arrayelem = (PLpgSQL_arrayelem *) target;

					if (nsubscripts++ >= MAXDIM)
						ereport(ERROR,
								(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
								 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
										nsubscripts + 1, MAXDIM)));

					/* Validate expression. */
					/* XXX is_expression */
					check_expr(cstate, arrayelem->subscript);

					target = cstate->estate->datums[arrayelem->arrayparentno];
				} while (target->dtype == PLPGSQL_DTYPE_ARRAYELEM);

				/*
				 * If target is domain over array, reduce to base type
				 */

#if PG_VERSION_NUM >= 90600

				arraytypeid = plpgsql_exec_get_datum_type(cstate->estate, target);

#else

				arraytypeid = exec_get_datum_type(cstate->estate, target);

#endif
				arraytypeid = getBaseType(arraytypeid);

				arrayelemtypeid = get_element_type(arraytypeid);

				if (!OidIsValid(arrayelemtypeid))
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("subscripted object is not an array")));

				if (expected_typoid)
					*expected_typoid = arrayelemtypeid;

				if (expected_typmod)
					*expected_typmod = ((PLpgSQL_var *) target)->datatype->atttypmod;

				record_variable_usage(cstate, target->dno, true);
			}
			break;

		default:
			;		/* nope */
	}
}

/*
 * Generate a prepared plan - this is simplified copy from pl_exec.c Is not
 * necessary to check simple plan, returns true, when expression is
 * succesfully prepared.
 */
static void
prepare_expr(PLpgSQL_checkstate *cstate,
			 PLpgSQL_expr *expr, int cursorOptions)
{
	SPIPlanPtr	plan;

	if (expr->plan == NULL)
	{
		/*
		 * The grammar can't conveniently set expr->func while building the parse
		 * tree, so make sure it's set before parser hooks need it.
		 */
		expr->func = cstate->estate->func;

		/*
		 * Generate and save the plan
		 */
		plan = SPI_prepare_params(expr->query,
								  (ParserSetupHook) plpgsql_parser_setup,
								  (void *) expr,
								  cursorOptions);

		if (plan == NULL)
		{
			/* Some SPI errors deserve specific error messages */
			switch (SPI_result)
			{
				case SPI_ERROR_COPY:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot COPY to/from client in PL/pgSQL")));
					break;

				case SPI_ERROR_TRANSACTION:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot begin/end transactions in PL/pgSQL"),
							 errhint("Use a BEGIN block with an EXCEPTION clause instead.")));
					break;

				default:
					elog(ERROR, "SPI_prepare_params failed for \"%s\": %s",
						 expr->query, SPI_result_code_string(SPI_result));
			}
		}

		/*
		 * We would to check all plans, but when plan exists, then don't 
		 * overwrite existing plan.
		 */
		if (expr->plan == NULL)
		{
			expr->plan = SPI_saveplan(plan);
			cstate->exprs = lappend(cstate->exprs, expr);
		}

		SPI_freeplan(plan);
	}

	/* Don't allow write plan when function is read only */
	if (cstate->estate->readonly_func)
		prohibit_write_plan(cstate, expr);

	if (cstate->performance_warnings)
		check_fishy_qual(cstate, expr);

	check_seq_functions(cstate, expr);

	collect_volatility(cstate, expr);

	if (cstate->format == PLPGSQL_SHOW_DEPENDENCY_FORMAT_TABULAR)
		detect_dependency(cstate, expr);

	prohibit_transaction_stmt(cstate, expr);
}

/*
 * Check so target can accept typoid value
 *
 */
static void
check_assign_to_target_type(PLpgSQL_checkstate *cstate,
							 Oid target_typoid, int32 target_typmod,
												 Oid value_typoid,
												 bool isnull)
{

#if PG_VERSION_NUM < 90500

	/* any used typmod enforces IO cast - performance warning for older than 9.5*/
	if (target_typmod != -1)
		put_error(cstate,
					  ERRCODE_DATATYPE_MISMATCH, 0,
					  "target type has type modificator",
					  NULL,
					  "Usage of type modificator enforces slower IO casting.",
					  PLPGSQL_CHECK_WARNING_PERFORMANCE,
					  0, NULL, NULL);

#endif

	if (type_is_rowtype(value_typoid))
		put_error(cstate,
					  ERRCODE_DATATYPE_MISMATCH, 0,
					  "cannot cast composite value to a scalar type",
					  NULL,
					  NULL,
					  PLPGSQL_CHECK_ERROR,
					  0, NULL, NULL);

	else if (target_typoid != value_typoid && !isnull)
	{
		StringInfoData	str;

		initStringInfo(&str);
		appendStringInfo(&str, "cast \"%s\" value to \"%s\" type",
									format_type_be(value_typoid),
									format_type_be(target_typoid));

		/* accent warning when cast is without supported explicit casting */
		if (!can_coerce_type(1, &value_typoid, &target_typoid, COERCION_EXPLICIT))
			put_error(cstate,
						  ERRCODE_DATATYPE_MISMATCH, 0,
						  "target type is different type than source type",
						  str.data,
						  "There are no possible explicit coercion between those types, possibly bug!",
						  PLPGSQL_CHECK_WARNING_OTHERS,
						  0, NULL, NULL);
		else if (!can_coerce_type(1, &value_typoid, &target_typoid, COERCION_ASSIGNMENT))
			put_error(cstate,
						  ERRCODE_DATATYPE_MISMATCH, 0,
						  "target type is different type than source type",
						  str.data,
						  "The input expression type does not have an assignment cast to the target type.",
						  PLPGSQL_CHECK_WARNING_OTHERS,
						  0, NULL, NULL);
		else
		{
			/* highly probably only performance issue */
			if (!isnull)
				put_error(cstate,
							  ERRCODE_DATATYPE_MISMATCH, 0,
							  "target type is different type than source type",
							  str.data,
							  "Hidden casting can be a performance issue.",
							  PLPGSQL_CHECK_WARNING_PERFORMANCE,
							  0, NULL, NULL);
		}

		pfree(str.data);
	}
}

/*
 * Assign a tuple descriptor to variable specified by dno
 */
static void
assign_tupdesc_dno(PLpgSQL_checkstate *cstate, int varno, TupleDesc tupdesc, bool isnull)
{
	PLpgSQL_datum *target = cstate->estate->datums[varno];

	switch (target->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) target;

				check_assign_to_target_type(cstate,
									 var->datatype->typoid, var->datatype->atttypmod,
									 TupleDescAttr(tupdesc, 0)->atttypid,
									 isnull);
			}
			break;

		case PLPGSQL_DTYPE_ROW:
			assign_tupdesc_row_or_rec(cstate, (PLpgSQL_row *) target, NULL, tupdesc, isnull);
			break;

		case PLPGSQL_DTYPE_REC:
			assign_tupdesc_row_or_rec(cstate, NULL, (PLpgSQL_rec *) target, tupdesc, isnull);
			break;

		case PLPGSQL_DTYPE_ARRAYELEM:
			{
				Oid expected_typoid;
				int expected_typmod;

				check_target(cstate, varno, &expected_typoid, &expected_typmod);

				/* When target is composite type, then source is expanded already */
				if (type_is_rowtype(expected_typoid))
				{
					PLpgSQL_rec rec;

					recval_init(&rec);

					PG_TRY();
					{
						recval_assign_tupdesc(cstate, &rec,
											  lookup_rowtype_tupdesc_noerror(expected_typoid,
																			 expected_typmod,
																			 true),
																			 isnull);

						assign_tupdesc_row_or_rec(cstate, NULL, &rec, tupdesc, isnull);
						recval_release(&rec);
					}
					PG_CATCH();
					{
						recval_release(&rec);

						PG_RE_THROW();
					}
					PG_END_TRY();
				}
				else
					check_assign_to_target_type(cstate,
									    expected_typoid, expected_typmod,
									    TupleDescAttr(tupdesc, 0)->atttypid,
									    isnull);
			}
			break;

		default:
			;		/* nope */
	}
}

/*
 * We have to assign TupleDesc to all used record variables step by step. We
 * would to use a exec routines for query preprocessing, so we must to create
 * a typed NULL value, and this value is assigned to record variable.
 */
static void
assign_tupdesc_row_or_rec(PLpgSQL_checkstate *cstate,
						  PLpgSQL_row *row, PLpgSQL_rec *rec,
						  TupleDesc tupdesc, bool isnull)
{
	if (tupdesc == NULL)
	{
		put_error(cstate,
					  0, 0,
					  "tuple descriptor is empty", NULL, NULL,
					  PLPGSQL_CHECK_WARNING_OTHERS,
					  0, NULL, NULL);
		return;
	}

	/*
	 * row variable has assigned TupleDesc already, so don't be processed here
	 */
	if (rec != NULL)
	{
		PLpgSQL_rec *target = (PLpgSQL_rec *) (cstate->estate->datums[rec->dno]);

		recval_release(target);
		recval_assign_tupdesc(cstate, target, tupdesc, isnull);
	}

	else if (row != NULL && tupdesc != NULL)
	{
		int			td_natts = tupdesc->natts;
		int			fnum;
		int			anum;

		anum = 0;
		for (fnum = 0; fnum < row->nfields; fnum++)
		{
			if (row->varnos[fnum] < 0)
				continue;		/* skip dropped column in row struct */

			while (anum < td_natts && TupleDescAttr(tupdesc, anum)->attisdropped)
				anum++;			/* skip dropped column in tuple */

			if (anum < td_natts)
			{
				Oid	valtype = SPI_gettypeid(tupdesc, anum + 1);
				PLpgSQL_datum *target = cstate->estate->datums[row->varnos[fnum]];

				switch (target->dtype)
				{
					case PLPGSQL_DTYPE_VAR:
						{
							PLpgSQL_var *var = (PLpgSQL_var *) target;

							check_assign_to_target_type(cstate,
												 var->datatype->typoid,
												 var->datatype->atttypmod,
														 valtype,
														 isnull);
						}
						break;

					case PLPGSQL_DTYPE_RECFIELD:
						{
							Oid	expected_typoid;
							int	expected_typmod;

							check_target(cstate, target->dno, &expected_typoid, &expected_typmod);
							check_assign_to_target_type(cstate,
												 expected_typoid,
												 expected_typmod,
														valtype,
														isnull);
						}
						break;
					default:
						;		/* nope */
				}

				anum++;
			}
		}
	}
}

static bool
detect_dependency_walker(Node *node, void *context)
{
	PLpgSQL_checkstate *cstate = (PLpgSQL_checkstate *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		ListCell *lc;

		foreach(lc, query->rtable)
		{
			RangeTblEntry *rt = (RangeTblEntry *) lfirst(lc);

			if (rt->rtekind == RTE_RELATION)
			{
				if (!bms_is_member(rt->relid, cstate->rel_oids))
				{
					tuplestore_put_dependency(cstate->tuple_store,
											 cstate->tupdesc,
											 "RELATION",
											 rt->relid,
											 get_namespace_name(get_rel_namespace(rt->relid)),
											 get_rel_name(rt->relid),
											 NULL);

					cstate->rel_oids = bms_add_member(cstate->rel_oids, rt->relid);
				}
			}
		}

		return query_tree_walker((Query *) node,
								 detect_dependency_walker,
								 context, 0);
	}

	if (IsA(node, FuncExpr) )
	{
		FuncExpr *fexpr = (FuncExpr *) node;

		if (get_func_namespace(fexpr->funcid) != PG_CATALOG_NAMESPACE)
		{
			if (!bms_is_member(fexpr->funcid, cstate->func_oids))
			{
				StringInfoData		str;
				ListCell		   *lc;
				int		i = 0;

				initStringInfo(&str);
				appendStringInfoChar(&str, '(');
				foreach(lc, fexpr->args)
				{
					Node *expr = (Node *) lfirst(lc);

					if (i++ > 0)
						appendStringInfoChar(&str, ',');

					appendStringInfoString(&str, format_type_be(exprType(expr)));
				}
				appendStringInfoChar(&str, ')');

				tuplestore_put_dependency(cstate->tuple_store,
										  cstate->tupdesc,
										  "FUNCTION",
										  fexpr->funcid,
										  get_namespace_name(get_func_namespace(fexpr->funcid)),
										  get_func_name(fexpr->funcid),
										  str.data);

				pfree(str.data);

				cstate->func_oids = bms_add_member(cstate->func_oids, fexpr->funcid);
			}
		}
	}

	return expression_tree_walker(node, detect_dependency_walker, context);
}

static void
detect_dependency(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	Query	   *query;

	query = ExprGetQuery(cstate, expr);
	detect_dependency_walker((Node *) query, cstate);
}


/*
 * Expecting persistent oid of nextval, currval and setval functions.
 * Ensured by regress tests.
 */
#define NEXTVAL_OID		1574
#define CURRVAL_OID		1575
#define SETVAL_OID		1576
#define SETVAL2_OID		1765

typedef struct 
{
	PLpgSQL_checkstate *cstate;
	PLpgSQL_expr *query;
} check_seq_functions_walker_params;

/*
 * When sequence related functions has constant oid parameter, then ensure, so
 * this oid is related to some sequnce object.
 *
 */
static bool
check_seq_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node,
								 check_seq_functions_walker,
								 context, 0);
	}
	if (IsA(node, FuncExpr))
	{
		FuncExpr *fexpr = (FuncExpr *) node;
		int		location = -1;

		switch (fexpr->funcid)
		{
			case NEXTVAL_OID:
			case CURRVAL_OID:
			case SETVAL_OID:
			case SETVAL2_OID:
			{
				Node *first_arg = linitial(fexpr->args);

				location = fexpr->location;

				if (first_arg && IsA(first_arg, Const))
				{
					Const *c = (Const *) first_arg;

					if (c->consttype == REGCLASSOID && !c->constisnull)
					{
						Oid		classid;

						if (c->location != -1)
							location = c->location;

						classid = DatumGetObjectId(c->constvalue);

						if (get_rel_relkind(classid) != RELKIND_SEQUENCE)
						{
							char	message[1024];
							check_seq_functions_walker_params *wp;

							wp = (check_seq_functions_walker_params *) context;

							snprintf(message, sizeof(message), "\"%s\" is not a sequence", get_rel_name(classid));

							put_error(wp->cstate,
										  ERRCODE_WRONG_OBJECT_TYPE, 0,
										  message,
										  NULL, NULL,
										  PLPGSQL_CHECK_ERROR,
										  location,
										  wp->query->query, NULL);
						}
					}
				}
			}
		}
	}

	return expression_tree_walker(node, check_seq_functions_walker, context);
}

/*
 * Returns Query node for expression
 *
 */
static Query *
ExprGetQuery(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query)
{
	CachedPlanSource *plansource = NULL;
	Query *result = NULL;

	if (query->plan != NULL)
	{
		SPIPlanPtr	plan = query->plan;

		if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
			elog(ERROR, "cached plan is not valid plan");

		if (list_length(plan->plancache_list) != 1)
			elog(ERROR, "plan is not single execution plan");

		plansource = (CachedPlanSource *) linitial(plan->plancache_list);

		if (list_length(plansource->query_list) != 1)
			elog(ERROR, "there is not single query");

		result = linitial(plansource->query_list);
	}

	return result;
}

static void
check_seq_functions(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	Query	   *query;
	check_seq_functions_walker_params  wp;

	wp.cstate = cstate;
	wp.query = expr;

	query = ExprGetQuery(cstate, expr);
	check_seq_functions_walker((Node *) query, &wp);
}

/*
 * Try to detect relations in query
 */
static bool
has_rtable_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		bool		has_relation = false;
		ListCell *lc;

		foreach (lc, query->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

			if (rte->rtekind == RTE_RELATION)
			{
				has_relation = true;
				break;
			}
		}

		if (has_relation)
		{
			return true;
		}
		else
			return query_tree_walker(query, has_rtable_walker, context, 0);
	}
	return expression_tree_walker(node, has_rtable_walker, context);
}

/*
 * Returns true, if query use any relation
 */
static bool
has_rtable(Query *query)
{
	return has_rtable_walker((Node *) query, NULL);
}

/*
 * We can detect a volatility of some expressions
 */
static void
collect_volatility(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	/*
	 * Try to detect volatility only when we are not sure about it
	 */
	if (cstate->performance_warnings && cstate->volatility != PROVOLATILE_VOLATILE)
	{
		Query	   *query;

		query = ExprGetQuery(cstate, expr);

		switch (query->commandType)
		{
			case CMD_SELECT:
				{
					if (!query->hasModifyingCTE && !query->hasForUpdate)
					{
						/* there is chance so query will be immutable */
						if (contain_volatile_functions((Node *) query))
							cstate->volatility = PROVOLATILE_VOLATILE;
						else if (!contain_mutable_functions((Node *) query))
						{
							/*
							 * when level is still immutable, check if there
							 * are not reference to tables.
							 */
							if (cstate->volatility == PROVOLATILE_IMMUTABLE)
							{
								if (has_rtable(query))
									cstate->volatility = PROVOLATILE_STABLE;
							}
						}
						else
							cstate->volatility = PROVOLATILE_STABLE;
					}
					else
						cstate->volatility = PROVOLATILE_VOLATILE;
				}
				break;

			default:
				/* these statements are not read only and requires VOLATILE flag */
				cstate->volatility = PROVOLATILE_VOLATILE;
				break;
		}
	}
}

static bool
contain_fishy_cast_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, OpExpr))
	{
		OpExpr *opexpr = (OpExpr *) node;

		if (!opexpr->opretset && opexpr->opresulttype == BOOLOID
				&& list_length(opexpr->args) == 2)
		{
			Node *l1 = linitial(opexpr->args);
			Node *l2 = lsecond(opexpr->args);
			Param *param = NULL;
			FuncExpr *fexpr = NULL;

			if (IsA(l1, Param))
				param = (Param *) l1;
			else if (IsA(l1, FuncExpr))
				fexpr = (FuncExpr *) l1;

			if (IsA(l2, Param))
				param = (Param *) l2;
			else if (IsA(l2, FuncExpr))
				fexpr = (FuncExpr *) l2;

			if (param != NULL && fexpr != NULL)
			{
				if (param->paramkind != PARAM_EXTERN)
					return false;

				if (fexpr->funcformat != COERCE_IMPLICIT_CAST ||
						fexpr->funcretset ||
						list_length(fexpr->args) != 1 ||
						param->paramtype != fexpr->funcresulttype)
					return false;

				if (!IsA(linitial(fexpr->args), Var))
					return false;

				*((Param **) context) = param;

				return true;
			}
		}
	}

	return expression_tree_walker(node, contain_fishy_cast_walker, context);
}

/*
 * Try to identify constraint where variable from one side is implicitly casted to
 * parameter type of second side. It can symptom of parameter wrong type.
 *
 */
static bool
contain_fishy_cast(Node *node, Param **param)
{
	return contain_fishy_cast_walker(node, param);
}

static bool
qual_has_fishy_cast(PlannedStmt *plannedstmt, Plan *plan, Param **param)
{
	ListCell *lc;

	if (plan == NULL)
		return false;

	if (contain_fishy_cast((Node *) plan->qual, param))
		return true;

	if (qual_has_fishy_cast(plannedstmt, innerPlan(plan), param))
		return true;
	if (qual_has_fishy_cast(plannedstmt, outerPlan(plan), param))
		return true;

	foreach(lc, plan->initPlan)
	{
		SubPlan *subplan = (SubPlan *) lfirst(lc);
		Plan *s_plan = exec_subplan_get_plan(plannedstmt, subplan);

		if (qual_has_fishy_cast(plannedstmt, s_plan, param))
			return true;
	}

	return false;
}

/*
 * Common part of some expression based analyzes.
 *
 */
static CachedPlan *
ExprGetPlan(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query, bool *returns_result)
{
	CachedPlanSource *plansource = NULL;
	CachedPlan *cplan;

	if (query->plan != NULL)
	{
		SPIPlanPtr	plan = query->plan;

		if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
			elog(ERROR, "cached plan is not valid plan");

		if (list_length(plan->plancache_list) != 1)
			elog(ERROR, "plan is not single execution plan");

		plansource = (CachedPlanSource *) linitial(plan->plancache_list);

		if (!plansource->resultDesc)
		{
			bool		result_is_optional = false;

#if PG_VERSION_NUM >= 110000

			if (cstate->estate->err_stmt != NULL)
				result_is_optional = cstate->estate->err_stmt->cmd_type == PLPGSQL_STMT_CALL;

#endif

			if (!result_is_optional)
				elog(ERROR, "query returns no result");

			*returns_result = false;
		}
		else
			*returns_result = true;
	}
	else
		elog(ERROR, "there are no plan for query: \"%s\"",
			 query->query);

	/*
	 * When tupdesc is related to unpined record, we will try to check
	 * plan if it is just function call and if it is then we can try to
	 * derive a tupledes from function's description.
	 */
#if PG_VERSION_NUM >= 100000

	cplan = GetCachedPlan(plansource, NULL, true, NULL);

#else

	cplan = GetCachedPlan(plansource, NULL, true);

#endif

	return cplan;
}

/*
 * Returns Const Value from expression if it is possible.
 *
 */
static Const *
ExprGetConst(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query, bool *IsConst)
{
	PlannedStmt *_stmt;
	Plan	   *_plan;
	TargetEntry *tle;
	CachedPlan *cplan;
	Const	   *result = NULL;
	bool		returns_result;

	cplan = ExprGetPlan(cstate, query, &returns_result);
	_stmt = (PlannedStmt *) linitial(cplan->stmt_list);

	if (returns_result && IsA(_stmt, PlannedStmt) &&_stmt->commandType == CMD_SELECT)
	{
		_plan = _stmt->planTree;
		if (IsA(_plan, Result) &&list_length(_plan->targetlist) == 1)
		{
			tle = (TargetEntry *) linitial(_plan->targetlist);
			if (((Node *) tle->expr)->type == T_Const)
				result = (Const *) tle->expr;
		}
	}

	*IsConst = result != NULL;

	ReleaseCachedPlan(cplan, true);

	return result;
}

#if PG_VERSION_NUM >= 110000

/*
 * compatible_tupdescs: detect whether two tupdescs are physically compatible
 *
 * TRUE indicates that a tuple satisfying src_tupdesc can be used directly as
 * a value for a composite variable using dst_tupdesc.
 */
static bool
compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc)
{
	int			i;

	/* Possibly we could allow src_tupdesc to have extra columns? */
	if (dst_tupdesc->natts != src_tupdesc->natts)
		return false;

	for (i = 0; i < dst_tupdesc->natts; i++)
	{
		Form_pg_attribute dattr = TupleDescAttr(dst_tupdesc, i);
		Form_pg_attribute sattr = TupleDescAttr(src_tupdesc, i);

		if (dattr->attisdropped != sattr->attisdropped)
			return false;
		if (!dattr->attisdropped)
		{
			/* Normal columns must match by type and typmod */
			if (dattr->atttypid != sattr->atttypid ||
				(dattr->atttypmod >= 0 &&
				 dattr->atttypmod != sattr->atttypmod))
				return false;
		}
		else
		{
			/* Dropped columns are OK as long as length/alignment match */
			if (dattr->attlen != sattr->attlen ||
				dattr->attalign != sattr->attalign)
				return false;
		}
	}
	return true;
}

/*
 * Try to calculate row target from used INOUT variables
 */
static PLpgSQL_row *
CallExprGetRowTarget(PLpgSQL_checkstate *cstate, PLpgSQL_expr *CallExpr)
{
	Node	   *node;
	FuncExpr   *funcexpr;
	PLpgSQL_row *result = NULL;

	if (CallExpr->plan != NULL)
	{
		PLpgSQL_row *row;
		SPIPlanPtr	plan = CallExpr->plan;
		HeapTuple		tuple;
		List	   *funcargs;
		Oid		   *argtypes;
		char	  **argnames;
		char	   *argmodes;
		ListCell   *lc;
		int			i;
		int			nfields = 0;

		if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
			elog(ERROR, "cached plan is not valid plan");

		if (list_length(plan->plancache_list) != 1)
			elog(ERROR, "plan is not single execution plan");

		/*
		 * Get the original CallStmt
		 */
		node = linitial_node(Query, ((CachedPlanSource *) linitial(plan->plancache_list))->query_list)->utilityStmt;
		if (!IsA(node, CallStmt))
			elog(ERROR, "returned row from not a CallStmt");

		funcexpr = castNode(CallStmt, node)->funcexpr;

		/*
		 * Get the argument modes
		 */
		tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcexpr->funcid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for function %u", funcexpr->funcid);

		/* Extract function arguments, and expand any named-arg notation */
		funcargs = expand_function_arguments(funcexpr->args,
											 funcexpr->funcresulttype,
											 tuple);

		get_func_arg_info(tuple, &argtypes, &argnames, &argmodes);

		ReleaseSysCache(tuple);

		row = palloc0(sizeof(PLpgSQL_row));
		row->dtype = PLPGSQL_DTYPE_ROW;
		row->lineno = 0;
		row->varnos = palloc(sizeof(int) * list_length(funcargs));

		/*
		 * Construct row
		 */
		i = 0;
		foreach(lc, funcargs)
		{
			Node	   *n = lfirst(lc);

			if (argmodes &&
				(argmodes[i] == PROARGMODE_INOUT ||
				 argmodes[i] == PROARGMODE_OUT))
			{
				if (IsA(n, Param))
				{
					Param	   *param = (Param *) n;

					/* paramid is offset by 1 (see make_datum_param()) */
					row->varnos[nfields++] = param->paramid - 1;
				}
				else
				{
					/* report error using parameter name, if available */
					if (argnames && argnames[i] && argnames[i][0])
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("procedure parameter \"%s\" is an output parameter but corresponding argument is not writable",
										argnames[i])));
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("procedure parameter %d is an output parameter but corresponding argument is not writable",
										i + 1)));
				}
			}
			i++;
		}

		row->nfields = nfields;

		/* Don't return empty row variable */
		if (nfields > 0)
		{
			result = row;
		}
		else
		{
			pfree(row->varnos);
			pfree(row);
		}
	}
	else
		elog(ERROR, "there are no plan for query: \"%s\"",
			 CallExpr->query);

	return result;
}

#endif

/*
 * Returns true for entered NULL constant
 *
 */
static bool
is_const_null_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query)
{
	Const	   *c;
	bool		IsConst;

	c = ExprGetConst(cstate, query, &IsConst);

	return IsConst ? c->constisnull : false;
}

/*
 * Returns string for any not null constant.
 * When constant is NULL, then returns NULL.
 *
 */
static char *
ExprGetString(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query, bool *IsConst)
{
	Const	   *c;
	char	   *result = NULL;

	c = ExprGetConst(cstate, query, IsConst);
	if (*IsConst && !c->constisnull)
	{
		Oid		typoutput;
		bool	typisvarlena;

		getTypeOutputInfo(c->consttype, &typoutput, &typisvarlena);
		result = OidOutputFunctionCall(typoutput, c->constvalue);
	}

	return result;
}

/*
 * Returns a tuple descriptor based on existing plan, When error is detected
 * returns null.
 */
static TupleDesc
expr_get_desc(PLpgSQL_checkstate *cstate,
			  PLpgSQL_expr *query,
			  bool use_element_type,
			  bool expand_record,
			  bool is_expression,
			  Oid *first_level_typoid)
{
	TupleDesc	tupdesc = NULL;
	CachedPlanSource *plansource = NULL;

	if (query->plan != NULL)
	{
		SPIPlanPtr	plan = query->plan;

		if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
			elog(ERROR, "cached plan is not valid plan");

		if (list_length(plan->plancache_list) != 1)
			elog(ERROR, "plan is not single execution plan");

		plansource = (CachedPlanSource *) linitial(plan->plancache_list);

		if (!plansource->resultDesc)
		{
			if (is_expression)
				elog(ERROR, "query returns no result");
			else
				return NULL;
		}
		tupdesc = CreateTupleDescCopy(plansource->resultDesc);
	}
	else
		elog(ERROR, "there are no plan for query: \"%s\"",
			 query->query);

	if (is_expression && tupdesc->natts != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("query \"%s\" returned %d columns",
						query->query,
						tupdesc->natts)));

	/*
	 * try to get a element type, when result is a array (used with FOREACH
	 * ARRAY stmt)
	 */
	if (use_element_type)
	{
		Oid			elemtype;
		TupleDesc	elemtupdesc;

		/* result should be a array */
		if (is_expression && tupdesc->natts != 1)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("query \"%s\" returned %d columns",
							query->query,
							tupdesc->natts)));

		/* check the type of the expression - must be an array */
		elemtype = get_element_type(TupleDescAttr(tupdesc, 0)->atttypid);
		if (!OidIsValid(elemtype))
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
				errmsg("FOREACH expression must yield an array, not type %s",
					   format_type_be(TupleDescAttr(tupdesc, 0)->atttypid))));
			FreeTupleDesc(tupdesc);
		}

		if (is_expression && first_level_typoid != NULL)
			*first_level_typoid = elemtype;

		/* when elemtype is not composity, prepare single field tupdesc */
		if (!type_is_rowtype(elemtype))
		{
			TupleDesc rettupdesc;

#if PG_VERSION_NUM >= 120000

			rettupdesc = CreateTemplateTupleDesc(1);

#else

			rettupdesc = CreateTemplateTupleDesc(1, false);

#endif

			TupleDescInitEntry(rettupdesc, 1, "__array_element__", elemtype, -1, 0);

			FreeTupleDesc(tupdesc);
			BlessTupleDesc(rettupdesc);

			tupdesc = rettupdesc;
		}
		else
		{
			elemtupdesc = lookup_rowtype_tupdesc_noerror(elemtype, -1, true);
			if (elemtupdesc != NULL)
			{
				FreeTupleDesc(tupdesc);
				tupdesc = CreateTupleDescCopy(elemtupdesc);
				ReleaseTupleDesc(elemtupdesc);
			}
		}
	}
	else
	{
		if (is_expression && first_level_typoid != NULL)
			*first_level_typoid = TupleDescAttr(tupdesc, 0)->atttypid;
	}

	/*
	 * One spacial case is when record is assigned to composite type, then we
	 * should to unpack composite type.
	 */
	if (tupdesc->tdtypeid == RECORDOID &&
		tupdesc->tdtypmod == -1 &&
		tupdesc->natts == 1 && expand_record)
	{
		TupleDesc	unpack_tupdesc;

		unpack_tupdesc = lookup_rowtype_tupdesc_noerror(TupleDescAttr(tupdesc, 0)->atttypid,
												TupleDescAttr(tupdesc, 0)->atttypmod,
														true);
		if (unpack_tupdesc != NULL)
		{
			FreeTupleDesc(tupdesc);
			tupdesc = CreateTupleDescCopy(unpack_tupdesc);
			ReleaseTupleDesc(unpack_tupdesc);
		}
	}

	/*
	 * There is special case, when returned tupdesc contains only unpined
	 * record: rec := func_with_out_parameters(). IN this case we must to dig
	 * more deep - we have to find oid of function and get their parameters,
	 *
	 * This is support for assign statement recvar :=
	 * func_with_out_parameters(..)
	 *
	 * XXX: Why don't we always do that?
	 */
	if (tupdesc->tdtypeid == RECORDOID &&
		tupdesc->tdtypmod == -1 &&
		tupdesc->natts == 1 &&
		TupleDescAttr(tupdesc, 0)->atttypid == RECORDOID &&
		TupleDescAttr(tupdesc, 0)->atttypmod == -1 &&
		expand_record)
	{
		PlannedStmt *_stmt;
		Plan	   *_plan;
		TargetEntry *tle;
		CachedPlan *cplan;

		/*
		 * When tupdesc is related to unpined record, we will try to check
		 * plan if it is just function call and if it is then we can try to
		 * derive a tupledes from function's description.
		 */
#if PG_VERSION_NUM >= 100000

	cplan = GetCachedPlan(plansource, NULL, true, NULL);

#else

	cplan = GetCachedPlan(plansource, NULL, true);

#endif
		_stmt = (PlannedStmt *) linitial(cplan->stmt_list);

		if (IsA(_stmt, PlannedStmt) &&_stmt->commandType == CMD_SELECT)
		{
			_plan = _stmt->planTree;

			if (IsA(_plan, Result) &&list_length(_plan->targetlist) == 1)
			{
				tle = (TargetEntry *) linitial(_plan->targetlist);

				switch (((Node *) tle->expr)->type)
				{
					case T_FuncExpr:
						{
							FuncExpr   *fn = (FuncExpr *) tle->expr;
							FmgrInfo	flinfo;
							FunctionCallInfoData fcinfo;
							TupleDesc	rd;
							Oid			rt;

							fmgr_info(fn->funcid, &flinfo);
							flinfo.fn_expr = (Node *) fn;
							fcinfo.flinfo = &flinfo;

							get_call_result_type(&fcinfo, &rt, &rd);
							if (rd == NULL)
								ereport(ERROR,
										(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("function does not return composite type, is not possible to identify composite type")));

							FreeTupleDesc(tupdesc);
							BlessTupleDesc(rd);

							tupdesc = rd;
						}
						break;

					case T_RowExpr:
						{
							RowExpr		*row = (RowExpr *) tle->expr;
							ListCell *lc_colname;
							ListCell *lc_arg;
							TupleDesc rettupdesc;
							int			i = 1;

#if PG_VERSION_NUM >= 120000

							rettupdesc = CreateTemplateTupleDesc(list_length(row->args));

#else

							rettupdesc = CreateTemplateTupleDesc(list_length(row->args), false);

#endif

							forboth (lc_colname, row->colnames, lc_arg, row->args)
							{
								Node	*arg = lfirst(lc_arg);
								char	*name = strVal(lfirst(lc_colname));

								TupleDescInitEntry(rettupdesc, i,
												    name,
												    exprType(arg),
												    exprTypmod(arg),
												    0);
								i++;
							}

							FreeTupleDesc(tupdesc);
							BlessTupleDesc(rettupdesc);

							tupdesc = rettupdesc;
						}
						break;

					case T_Const:
						{
							Const *c = (Const *) tle->expr;
						
							if (c->consttype == RECORDOID && c->consttypmod == -1)
							{
								Oid		tupType;
								int32	tupTypmod;

								HeapTupleHeader rec = DatumGetHeapTupleHeader(c->constvalue);
								tupType = HeapTupleHeaderGetTypeId(rec);
								tupTypmod = HeapTupleHeaderGetTypMod(rec);
								tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
							}
							else
								tupdesc = NULL;
						}
						break;

					default:
							/* cannot to take tupdesc */
							tupdesc = NULL;
				}
			}
		}
		ReleaseCachedPlan(cplan, true);
	}
	return tupdesc;
}

/*
 * Raise a error when plan is not read only
 */
static void
prohibit_write_plan(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query)
{
	CachedPlanSource *plansource = NULL;
	SPIPlanPtr	 plan = query->plan;
	CachedPlan	*cplan;
	List		*stmt_list;
	ListCell	*lc;

	if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
		elog(ERROR, "cached plan is not valid plan");

	if (list_length(plan->plancache_list) != 1)
		elog(ERROR, "plan is not single execution plan");

	plansource = (CachedPlanSource *) linitial(plan->plancache_list);

#if PG_VERSION_NUM >= 100000

	cplan = GetCachedPlan(plansource, NULL, true, NULL);

#else

	cplan = GetCachedPlan(plansource, NULL, true);

#endif

	stmt_list = cplan->stmt_list;

	foreach(lc, stmt_list)
	{

#if PG_VERSION_NUM >= 100000

		PlannedStmt *pstmt = (PlannedStmt *) lfirst(lc);

		Assert(IsA(pstmt, PlannedStmt));

#else

		Node *pstmt = (Node *) lfirst(lc);

#endif

		if (!CommandIsReadOnly(pstmt))
		{
			StringInfoData message;

			initStringInfo(&message);
			appendStringInfo(&message,
					"%s is not allowed in a non volatile function",
							CreateCommandTag((Node *) pstmt));

			put_error(cstate,
					  ERRCODE_FEATURE_NOT_SUPPORTED, 0,
					  message.data,
					  NULL,
					  NULL,
					  PLPGSQL_CHECK_ERROR,
					  0, query->query, NULL);

			pfree(message.data);
			message.data = NULL;
		}
	}

	ReleaseCachedPlan(cplan, true);
}

/*
 * Raise a error when plan is a transactional statement
 */
static void
prohibit_transaction_stmt(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query)
{
	CachedPlanSource *plansource = NULL;
	SPIPlanPtr	 plan = query->plan;
	CachedPlan	*cplan;
	List		*stmt_list;
	ListCell	*lc;

	if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
		elog(ERROR, "cached plan is not valid plan");

	if (list_length(plan->plancache_list) != 1)
		elog(ERROR, "plan is not single execution plan");

	plansource = (CachedPlanSource *) linitial(plan->plancache_list);

#if PG_VERSION_NUM >= 100000

	cplan = GetCachedPlan(plansource, NULL, true, NULL);

#else

	cplan = GetCachedPlan(plansource, NULL, true);

#endif

	stmt_list = cplan->stmt_list;

	foreach(lc, stmt_list)
	{
		Node *pstmt = (Node *) lfirst(lc);

#if PG_VERSION_NUM >= 100000

		/* PostgtreSQL 10 can have one level of nesting more */
		if (IsA(pstmt, PlannedStmt))
		{
			PlannedStmt *planstmt = (PlannedStmt *) pstmt;

			if (planstmt->commandType == CMD_UTILITY)
				pstmt = (Node *) planstmt->utilityStmt;
		}

#endif

		if (IsA(pstmt, TransactionStmt))
		{
			put_error(cstate,
					  ERRCODE_FEATURE_NOT_SUPPORTED, 0,
					  "cannot begin/end transactions in PL/pgSQL",
					  NULL,
					  "Use a BEGIN block with an EXCEPTION clause instead.",
					  PLPGSQL_CHECK_ERROR,
					  0, query->query, NULL);
		}
	}

	ReleaseCachedPlan(cplan, true);
}

/*
 * Raise a performance warning when plan hash fishy qual
 */
static void
check_fishy_qual(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query)
{
	CachedPlanSource *plansource = NULL;
	SPIPlanPtr	 plan = query->plan;
	CachedPlan	*cplan;
	List		*stmt_list;
	ListCell	*lc;

	if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
		elog(ERROR, "cached plan is not valid plan");

	if (list_length(plan->plancache_list) != 1)
		elog(ERROR, "plan is not single execution plan");

	plansource = (CachedPlanSource *) linitial(plan->plancache_list);

#if PG_VERSION_NUM >= 100000

	cplan = GetCachedPlan(plansource, NULL, true, NULL);

#else

	cplan = GetCachedPlan(plansource, NULL, true);

#endif

	stmt_list = cplan->stmt_list;

	foreach(lc, stmt_list)
	{
		Param	*param;
		PlannedStmt *pstmt = (PlannedStmt *) lfirst(lc);
		Plan *plan = NULL;

		Assert(IsA(pstmt, PlannedStmt));

		plan = pstmt->planTree;

		if (qual_has_fishy_cast(pstmt, plan, &param))
		{
			put_error(cstate,
					  ERRCODE_DATATYPE_MISMATCH, 0,
					  "implicit cast of attribute caused by different PLpgSQL variable type in WHERE clause",
					  "An index of some attribute cannot be used, when variable, used in predicate, has not right type like a attribute",
					  "Check a variable type - int versus numeric",
					  PLPGSQL_CHECK_WARNING_PERFORMANCE,
					  param->location,
					  query->query, NULL);
		}
	}

	ReleaseCachedPlan(cplan, true);
}

/*
 * returns refname of PLpgSQL_datum. When refname is generated,
 * then return null too, although refname is not null.
 */
static char *
datum_get_refname(PLpgSQL_datum *d)
{
	char	   *refname;
	int			lineno;

	switch (d->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			refname = ((PLpgSQL_var *) d)->refname;
			lineno = ((PLpgSQL_var *) d)->lineno;
			break;

		case PLPGSQL_DTYPE_ROW:
			refname = ((PLpgSQL_row *) d)->refname;
			lineno = ((PLpgSQL_row *) d)->lineno;
			break;

		case PLPGSQL_DTYPE_REC:
			refname = ((PLpgSQL_rec *) d)->refname;
			lineno = ((PLpgSQL_rec *) d)->lineno;
			break;

		default:
			refname = NULL;
			lineno = -1;
	}

	/*
	 * PostgreSQL 12 started use "(unnamed row)" name for internal
	 * variables. Hide this name too (lineno is -1).
	 */
	if (is_internal(refname, lineno))
		return NULL;

	return refname;
}

/****************************************************************************************
 * Output routines
 *
 ****************************************************************************************
 *
 */

#define SET_RESULT_NULL(anum) \
	do { \
		values[(anum)] = (Datum) 0; \
		nulls[(anum)] = true; \
	} while (0)

#define SET_RESULT(anum, value) \
	do { \
		values[(anum)] = (value); \
		nulls[(anum)] = false; \
	} while(0)

#define SET_RESULT_TEXT(anum, str) \
	do { \
		if (str != NULL) \
		{ \
			SET_RESULT((anum), CStringGetTextDatum((str))); \
		} \
		else \
		{ \
			SET_RESULT_NULL(anum); \
		} \
	} while (0)

#define SET_RESULT_INT32(anum, ival)	SET_RESULT((anum), Int32GetDatum((ival)))
#define SET_RESULT_INT64(anum, ival)	SET_RESULT((anum), Int64GetDatum((ival)))
#define SET_RESULT_OID(anum, oid)		SET_RESULT((anum), ObjectIdGetDatum((oid)))
#define SET_RESULT_FLOAT8(anum, fval)	SET_RESULT((anum), Float8GetDatum(fval))

/*
 * error processing switch - ignore warnings when it is necessary,
 * store fields to result tuplestore or raise exception to out
 *
 */
static void
put_error(PLpgSQL_checkstate *cstate,
					  int sqlerrcode,
					  int lineno,
					  const char *message,
					  const char *detail,
					  const char *hint,
					  int level,
					  int position,
					  const char *query,
					  const char *context)
{
	/* In this case we would not to see a errors */
	if (cstate->format == PLPGSQL_SHOW_DEPENDENCY_FORMAT_TABULAR)
		return;

	/* ignore warnings when is not requested */
	if ((level == PLPGSQL_CHECK_WARNING_PERFORMANCE && !cstate->performance_warnings) ||
			    (level == PLPGSQL_CHECK_WARNING_OTHERS && !cstate->other_warnings) ||
			    (level == PLPGSQL_CHECK_WARNING_EXTRA && !cstate->extra_warnings))
		return;

	if (cstate->tuple_store != NULL)
	{
		switch (cstate->format)
		{
			case PLPGSQL_CHECK_FORMAT_TABULAR:
				tuplestore_put_error_tabular(cstate->tuple_store, cstate->tupdesc,
									 cstate->estate, cstate->fn_oid,
									 sqlerrcode, lineno, message, detail,
									 hint, level, position, query, context);
				break;

			case PLPGSQL_CHECK_FORMAT_TEXT:
				tuplestore_put_error_text(cstate->tuple_store, cstate->tupdesc,
									 cstate->estate, cstate->fn_oid,
									 sqlerrcode, lineno, message, detail,
									 hint, level, position, query, context);
				break;

			case PLPGSQL_CHECK_FORMAT_XML:
				format_error_xml(cstate->sinfo, cstate->estate,
									 sqlerrcode, lineno, message, detail,
									 hint, level, position, query, context);
				break;

			case PLPGSQL_CHECK_FORMAT_JSON:
				format_error_json(cstate->sinfo, cstate->estate,
									 sqlerrcode, lineno, message, detail,
									 hint, level, position, query, context);
			break;
		}
	}
	else
	{
		int elevel;

		/*
		 * when a passive mode is active and fatal_errors is false, then
		 * raise warning everytime.
		 */
		if (!cstate->is_active_mode && !cstate->fatal_errors)
			elevel = WARNING;
		else
			elevel = level == PLPGSQL_CHECK_ERROR ? ERROR : WARNING;

		/* use error fields as parameters of postgres exception */
		ereport(elevel,
				(sqlerrcode ? errcode(sqlerrcode) : 0,
				 errmsg_internal("%s", message),
				 (detail != NULL) ? errdetail_internal("%s", detail) : 0,
				 (hint != NULL) ? errhint("%s", hint) : 0,
				 (query != NULL) ? internalerrquery(query) : 0,
				 (position != 0) ? internalerrposition(position) : 0,
				 (context != NULL) ? errcontext("%s", context) : 0));
	}
}

static const char *
error_level_str(int level)
{
	switch (level)
	{
		case PLPGSQL_CHECK_ERROR:
			return "error";
		case PLPGSQL_CHECK_WARNING_OTHERS:
			return "warning";
		case PLPGSQL_CHECK_WARNING_EXTRA:
			return "warning extra";
		case PLPGSQL_CHECK_WARNING_PERFORMANCE:
			return "performance";
		default:
			return "???";
	}
}

/*
 * store dependency fields to result tuplestore
 *
 */
static void
tuplestore_put_dependency(Tuplestorestate *tuple_store,
						  TupleDesc tupdesc,
						  char *type,
						  Oid oid,
						  char *schema,
						  char *name,
						  char *params)
{
	Datum	values[Natts_dependency];
	bool	nulls[Natts_dependency];

	SET_RESULT_TEXT(Anum_dependency_type, type);
	SET_RESULT_OID(Anum_dependency_oid, oid);
	SET_RESULT_TEXT(Anum_dependency_schema, schema);
	SET_RESULT_TEXT(Anum_dependency_name, name);
	SET_RESULT_TEXT(Anum_dependency_params, params);

	tuplestore_putvalues(tuple_store, tupdesc, values, nulls);
}

/*
 * store error fields to result tuplestore
 *
 */
static void
tuplestore_put_error_tabular(Tuplestorestate *tuple_store, TupleDesc tupdesc,
						  PLpgSQL_execstate *estate,
								 Oid fn_oid,
								 int sqlerrcode,
								 int lineno,
								 const char *message,
								 const char *detail,
								 const char *hint,
								 int level,
								 int position,
								 const char *query,
								 const char *context)
{
	Datum	values[Natts_result];
	bool	nulls[Natts_result];

	Assert(message != NULL);

	SET_RESULT_OID(Anum_result_functionid, fn_oid);

	/* lineno should be valid */
	if (estate != NULL && estate->err_stmt != NULL && estate->err_stmt->lineno > 0)
	{
		/* use lineno based on err_stmt */
		SET_RESULT_INT32(Anum_result_lineno, estate->err_stmt->lineno);
		SET_RESULT_TEXT(Anum_result_statement, plpgsql_stmt_typename(estate->err_stmt));
	}
	else if (strncmp(message, UNUSED_VARIABLE_TEXT, UNUSED_VARIABLE_TEXT_CHECK_LENGTH) == 0)
	{
		SET_RESULT_INT32(Anum_result_lineno, lineno);
		SET_RESULT_TEXT(Anum_result_statement, "DECLARE");
	}
	else if (strncmp(message, NEVER_READ_VARIABLE_TEXT, NEVER_READ_VARIABLE_TEXT_CHECK_LENGTH) == 0)
	{
		SET_RESULT_INT32(Anum_result_lineno, lineno);
		SET_RESULT_TEXT(Anum_result_statement, "DECLARE");
	}

	else
	{
		SET_RESULT_NULL(Anum_result_lineno);
		SET_RESULT_NULL(Anum_result_statement);
	} 

	SET_RESULT_TEXT(Anum_result_sqlstate, unpack_sql_state(sqlerrcode));
	SET_RESULT_TEXT(Anum_result_message, message);
	SET_RESULT_TEXT(Anum_result_detail, detail);
	SET_RESULT_TEXT(Anum_result_hint, hint);
	SET_RESULT_TEXT(Anum_result_level, error_level_str(level));

	if (position != 0)
		SET_RESULT_INT32(Anum_result_position, position);
	else
		SET_RESULT_NULL(Anum_result_position);

	SET_RESULT_TEXT(Anum_result_query, query);
	SET_RESULT_TEXT(Anum_result_context, context);

	tuplestore_putvalues(tuple_store, tupdesc, values, nulls);
}


/*
 * collects errors and warnings in plain text format
 */
static void
tuplestore_put_error_text(Tuplestorestate *tuple_store, TupleDesc tupdesc,
						  PLpgSQL_execstate *estate,
								 Oid fn_oid,
								 int sqlerrcode,
								 int lineno,
								 const char *message,
								 const char *detail,
								 const char *hint,
								 int level,
								 int position,
								 const char *query,
								 const char *context)
{
	StringInfoData  sinfo;
	const char *level_str = error_level_str(level);

	Assert(message != NULL);

	initStringInfo(&sinfo);

	/* lineno should be valid for actual statements */
	if (estate != NULL && estate->err_stmt != NULL && estate->err_stmt->lineno > 0)
		appendStringInfo(&sinfo, "%s:%s:%d:%s:%s",
				 level_str,
				 unpack_sql_state(sqlerrcode),
				 estate->err_stmt->lineno,
			    plpgsql_stmt_typename(estate->err_stmt),
				 message);
	else if (strncmp(message, UNUSED_VARIABLE_TEXT, UNUSED_VARIABLE_TEXT_CHECK_LENGTH) == 0)
	{
		appendStringInfo(&sinfo, "%s:%s:%d:%s:%s",
				 level_str,
				 unpack_sql_state(sqlerrcode),
				 lineno,
				 "DECLARE",
				 message);
	}
	else if (strncmp(message, NEVER_READ_VARIABLE_TEXT, NEVER_READ_VARIABLE_TEXT_CHECK_LENGTH) == 0)
	{
		appendStringInfo(&sinfo, "%s:%s:%d:%s:%s",
				 level_str,
				 unpack_sql_state(sqlerrcode),
				 lineno,
				 "DECLARE",
				 message);
	}
	else
	{
		appendStringInfo(&sinfo, "%s:%s:%s",
				 level_str,
				 unpack_sql_state(sqlerrcode),
				 message);
	}

	tuplestore_put_text_line(tuple_store, tupdesc, sinfo.data, sinfo.len);
	resetStringInfo(&sinfo);

	if (query != NULL) 
	{
		char           *query_line;	/* pointer to beginning of current line */
		int             line_caret_pos;
		bool            is_first_line = true;
		char           *_query = pstrdup(query);
		char           *ptr;

		ptr = _query;
		query_line = ptr;
		line_caret_pos = position;

		while (*ptr != '\0')
		{
			/* search end of lines and replace '\n' by '\0' */
			if (*ptr == '\n')
			{
				*ptr = '\0';
				if (is_first_line)
				{
					appendStringInfo(&sinfo, "Query: %s", query_line);
					is_first_line = false;
				} else
					appendStringInfo(&sinfo, "       %s", query_line);

				tuplestore_put_text_line(tuple_store, tupdesc, sinfo.data, sinfo.len);
				resetStringInfo(&sinfo);

				if (line_caret_pos > 0 && position == 0)
				{
					appendStringInfo(&sinfo, "--     %*s",
						       line_caret_pos, "^");

					tuplestore_put_text_line(tuple_store, tupdesc, sinfo.data, sinfo.len);
					resetStringInfo(&sinfo);

					line_caret_pos = 0;
				}
				/* store caret position offset for next line */

				if (position > 1)
					line_caret_pos = position - 1;

				/* go to next line */
				query_line = ptr + 1;
			}
			ptr += pg_mblen(ptr);

			if (position > 0)
				position--;
		}

		/* flush last line */
		if (query_line != NULL)
		{
			if (is_first_line)
				appendStringInfo(&sinfo, "Query: %s", query_line);
			else
				appendStringInfo(&sinfo, "       %s", query_line);

			tuplestore_put_text_line(tuple_store, tupdesc, sinfo.data, sinfo.len);
			resetStringInfo(&sinfo);

			if (line_caret_pos > 0 && position == 0)
			{
				appendStringInfo(&sinfo, "--     %*s",
						 line_caret_pos, "^");
				tuplestore_put_text_line(tuple_store, tupdesc, sinfo.data, sinfo.len);
				resetStringInfo(&sinfo);
			}
		}

		pfree(_query);
	}

	if (detail != NULL)
	{
		appendStringInfo(&sinfo, "Detail: %s", detail);
		tuplestore_put_text_line(tuple_store, tupdesc, sinfo.data, sinfo.len);
		resetStringInfo(&sinfo);
	}

	if (hint != NULL)
	{
		appendStringInfo(&sinfo, "Hint: %s", hint);
		tuplestore_put_text_line(tuple_store, tupdesc, sinfo.data, sinfo.len);
		resetStringInfo(&sinfo);
	}

	if (context != NULL) 
	{
		appendStringInfo(&sinfo, "Context: %s", context);
		tuplestore_put_text_line(tuple_store, tupdesc, sinfo.data, sinfo.len);
		resetStringInfo(&sinfo);
	}

	pfree(sinfo.data);
}

/*
 * format_error_xml formats and collects a identifided issues
 */
static void
format_error_xml(StringInfo str,
						  PLpgSQL_execstate *estate,
								 int sqlerrcode,
								 int lineno,
								 const char *message,
								 const char *detail,
								 const char *hint,
								 int level,
								 int position,
								 const char *query,
								 const char *context)
{
	const char *level_str = error_level_str(level);

	Assert(message != NULL);

	/* flush tag */
	appendStringInfoString(str, "  <Issue>\n");

	appendStringInfo(str, "    <Level>%s</Level>\n", level_str);
	appendStringInfo(str, "    <Sqlstate>%s</Sqlstate>\n",
						 unpack_sql_state(sqlerrcode));
	appendStringInfo(str, "    <Message>%s</Message>\n",
							 escape_xml(message));
	if (estate != NULL && estate->err_stmt != NULL)
		appendStringInfo(str, "    <Stmt lineno=\"%d\">%s</Stmt>\n",
				 estate->err_stmt->lineno,
			   plpgsql_stmt_typename(estate->err_stmt));

	else if (strcmp(message, "unused declared variable") == 0)
		appendStringInfo(str, "    <Stmt lineno=\"%d\">DECLARE</Stmt>\n",
				 lineno);

	if (hint != NULL)
		appendStringInfo(str, "    <Hint>%s</Hint>\n",
								 escape_xml(hint));
	if (detail != NULL)
		appendStringInfo(str, "    <Detail>%s</Detail>\n",
								 escape_xml(detail));
	if (query != NULL)
		appendStringInfo(str, "    <Query position=\"%d\">%s</Query>\n",
							 position, escape_xml(query));

	if (context != NULL)
		appendStringInfo(str, "    <Context>%s</Context>\n",
							 escape_xml(context));

	/* flush closing tag */
	appendStringInfoString(str, "  </Issue>\n");
}

/*
* format_error_json formats and collects a identifided issues
*/
static void
format_error_json(StringInfo str,
	PLpgSQL_execstate *estate,
	int sqlerrcode,
	int lineno,
	const char *message,
	const char *detail,
	const char *hint,
	int level,
	int position,
	const char *query,
	const char *context)
{
	const char *level_str = error_level_str(level);
	StringInfoData sinfo; /*Holds escaped json*/

	Assert(message != NULL);

	initStringInfo(&sinfo);

	/* flush tag */
	appendStringInfoString(str, "  {\n");
	appendStringInfo(str, "    \"level\":\"%s\",\n", level_str);
		
	escape_json(&sinfo, message);
	appendStringInfo(str, "    \"message\":%s,\n", sinfo.data);
	if (estate != NULL && estate->err_stmt != NULL)
		appendStringInfo(str, "    \"statement\":{\n\"lineNumber\":\"%d\",\n\"text\":\"%s\"\n},\n",
			estate->err_stmt->lineno,
			plpgsql_stmt_typename(estate->err_stmt));

	else if (strcmp(message, "unused declared variable") == 0)
		appendStringInfo(str, "    \"statement\":{\n\"lineNumber\":\"%d\",\n\"text\":\"DECLARE\"\n},",
			lineno);

	if (hint != NULL) {
		resetStringInfo(&sinfo);
		escape_json(&sinfo, hint);
		appendStringInfo(str, "    \"hint\":%s,\n", sinfo.data);
	}
	if (detail != NULL) {
		resetStringInfo(&sinfo);
		escape_json(&sinfo, detail);
		appendStringInfo(str, "    \"detail\":%s,\n", sinfo.data);
	}
	if (query != NULL) {
		resetStringInfo(&sinfo);
		escape_json(&sinfo, query);
		appendStringInfo(str, "    \"query\":{\n\"position\":\"%d\",\n\"text\":%s\n},\n", position, sinfo.data);
	}

	if (context != NULL) {
		resetStringInfo(&sinfo);
		escape_json(&sinfo, context);
		appendStringInfo(str, "    \"context\":%s,\n", sinfo.data);
	}

	/* placing this property last as to avoid a trailing comma*/
	appendStringInfo(str, "    \"sqlState\":\"%s\"\n",	unpack_sql_state(sqlerrcode));

	/* flush closing tag. Needs comman jus in case there is more than one issue. Comma removed in epilog */
	appendStringInfoString(str, "  },");
}

/*
 * store edata
 */
static void
put_error_edata(PLpgSQL_checkstate *cstate,
					ErrorData *edata)
{
	put_error(cstate,
				  edata->sqlerrcode,
				  edata->lineno,
				  edata->message,
				  edata->detail,
				  edata->hint,
				  PLPGSQL_CHECK_ERROR,
				  edata->internalpos,
				  edata->internalquery,
				  edata->context);
}

/*
 * Append text line (StringInfo) to one column tuple store
 *
 */
static void
tuplestore_put_text_line(Tuplestorestate *tuple_store, TupleDesc tupdesc,
								    const char *message, int len)
{
	Datum           value;
	bool            isnull = false;
	HeapTuple       tuple;

	if (len >= 0)
		value = PointerGetDatum(cstring_to_text_with_len(message, len));
	else
		value = PointerGetDatum(cstring_to_text(message));

	tuple = heap_form_tuple(tupdesc, &value, &isnull);
	tuplestore_puttuple(tuple_store, tuple);
}

/*
 * routines for beginning and finishing function checking
 *
 * it is used primary for XML & JSON format - create almost left
 * and almost right tag per function
 *
 */
static void
check_function_prolog(PLpgSQL_checkstate *cstate)
{
	/* XML format requires StringInfo buffer */
	if (cstate->format == PLPGSQL_CHECK_FORMAT_XML)
	{
		if (cstate->sinfo != NULL)
			resetStringInfo(cstate->sinfo);
		else
			cstate->sinfo = makeStringInfo();

		/* create a initial tag */
		appendStringInfo(cstate->sinfo, "<Function oid=\"%d\">\n", cstate->fn_oid);
	}
	else if (cstate->format == PLPGSQL_CHECK_FORMAT_JSON) {
		if (cstate->sinfo != NULL)
			resetStringInfo(cstate->sinfo);
		else
			cstate->sinfo = makeStringInfo();

		/* create a initial tag */
		appendStringInfo(cstate->sinfo, "{ \"function\":\"%d\",\n\"issues\":[\n", cstate->fn_oid);
	}
}

static void
check_function_epilog(PLpgSQL_checkstate *cstate)
{
	if (cstate->format == PLPGSQL_CHECK_FORMAT_XML)
	{
		appendStringInfoString(cstate->sinfo, "</Function>");

		tuplestore_put_text_line(cstate->tuple_store, cstate->tupdesc,
					    cstate->sinfo->data, cstate->sinfo->len);
	}
	else if (cstate->format == PLPGSQL_CHECK_FORMAT_JSON)
	{
		if (cstate->sinfo->len > 1 && cstate->sinfo->data[cstate->sinfo->len -1] == ',') {
			cstate->sinfo->data[cstate->sinfo->len - 1] = '\n';
		}
		appendStringInfoString(cstate->sinfo, "\n]\n}");

		tuplestore_put_text_line(cstate->tuple_store, cstate->tupdesc,
			cstate->sinfo->data, cstate->sinfo->len);
	}
}

/****************************************************************************************
 * A maintaining of hash table of checked functions
 *
 ****************************************************************************************
 *
 */

/*
 * We cannot to attach to DELETE event - so we don't need implement delete here.
 */

/* exported so we can call it from plpgsql_check_init() */
static void
plpgsql_check_HashTableInit(void)
{
	HASHCTL		ctl;

	/* don't allow double-initialization */
	Assert(plpgsql_check_HashTable == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(PLpgSQL_func_hashkey);
	ctl.entrysize = sizeof(plpgsql_check_HashEnt);
	ctl.hash = tag_hash;
	plpgsql_check_HashTable = hash_create("plpgsql_check function cache",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_FUNCTION);
}

static bool
is_checked(PLpgSQL_function *func)
{
	plpgsql_check_HashEnt *hentry;

	hentry = (plpgsql_check_HashEnt *) hash_search(plpgsql_check_HashTable,
											 (void *) func->fn_hashkey,
											 HASH_FIND,
											 NULL);

	if (hentry != NULL && hentry->fn_xmin == func->fn_xmin &&
			  ItemPointerEquals(&hentry->fn_tid, &func->fn_tid))
		return hentry->is_checked;

	return false;
}

static void
mark_as_checked(PLpgSQL_function *func)
{
	plpgsql_check_HashEnt *hentry;
	bool		found;

	/* don't try to mark anonymous code blocks */
	if (func->fn_oid != InvalidOid)
	{
		hentry = (plpgsql_check_HashEnt *) hash_search(plpgsql_check_HashTable,
												 (void *) func->fn_hashkey,
												 HASH_ENTER,
												 &found);

		hentry->fn_xmin = func->fn_xmin;
		hentry->fn_tid = func->fn_tid;

		hentry->is_checked = true;
	}
}

/*
 * plpgsql_show_dependency_tb
 *
 * Prepare tuplestore and start check function in mode dependency detection
 *
 */
Datum
plpgsql_show_dependency_tb(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	Oid			relid = PG_GETARG_OID(1);
	TupleDesc	tupdesc;
	HeapTuple	procTuple;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	PLpgSQL_trigtype trigtype;
	ErrorContextCallback *prev_errorcontext;

	/* check to see if caller supports us returning a tuplestore */
	SetReturningFunctionCheck(rsinfo);

	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);

	trigtype = get_trigtype(procTuple);
	precheck_conditions(procTuple, trigtype, relid);

	/* need to build tuplestore in query context */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
	tupstore = tuplestore_begin_heap(false, false, work_mem);
	MemoryContextSwitchTo(oldcontext);

	prev_errorcontext = error_context_stack;

	/* Envelope outer plpgsql function is not interesting */
	error_context_stack = NULL;

	check_plpgsql_function(procTuple, relid, trigtype,
							   tupdesc, tupstore,
							   PLPGSQL_SHOW_DEPENDENCY_FORMAT_TABULAR,
								   false, false, false, false);
	error_context_stack = prev_errorcontext;

	ReleaseSysCache(procTuple);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	return (Datum) 0;
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
	ctl.hash = tag_hash;
	profiler_HashTable = hash_create("plpgsql_check function profiler local cache",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
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
	ctl.hash = tag_hash;
	profiler_chunks_HashTable = hash_create("plpgsql_check function profiler local chunks",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

static void
update_persistent_profile(profiler_info *pinfo, PLpgSQL_function *func)
{
	profiler_profile *profile = pinfo->profile;
	profiler_hashkey hk;
	profiler_stmt_chunk *chunk;
	profiler_stmt_chunk *first_chunk = NULL;
	bool		found;
	int			i;
	int			stmt_counter = 0;
	HTAB	   *chunks;
	bool		shared_chunks;
	bool		exclusive_lock = false;
	volatile bool unlock_mutex = false;

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

	profiler_init_hashkey(&hk, func);

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
		exclusive_lock = true;

		chunk = (profiler_stmt_chunk *) hash_search(chunks,
												 (void *) &hk,
												 HASH_ENTER,
												 &found);
	}

	if (!found)
	{
		int		i;
		int		stmt_counter;

		/* first shared chunk is prepared already. local chunk should be done */
		if (shared_chunks)
		{
			/* for first chunk we need to initialize mutex */
			SpinLockInit(&chunk->mutex);
			stmt_counter = 0;
		}
		else
			stmt_counter = -1;

		/* we should to enter empty chunks first */
		for (i = 0; i < profile->nstatements; i++)
		{
			profiler_stmt_reduced *prstmt;
			profiler_stmt *pstmt = &pinfo->stmts[i];

			hk.chunk_num = 0;

			if (stmt_counter == -1 || stmt_counter >= STATEMENTS_PER_CHUNK)
			{
				hk.chunk_num += 1;

				chunk = (profiler_stmt_chunk *) hash_search(chunks,
															 (void *) &hk,
															 HASH_ENTER,
															 &found);

				if (found)
					elog(ERROR, "broken consistency of plpgsql_check profiler chunks");

				stmt_counter = 0;
			}

			prstmt = &chunk->stmts[stmt_counter++];

			prstmt->lineno = pstmt->lineno;
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

	/* if we have not exclusive lock, we should to lock first chunk */
	PG_TRY();
	{
		if (shared_chunks && !exclusive_lock)
		{
			first_chunk = chunk;
			SpinLockAcquire(&first_chunk->mutex);
			unlock_mutex = true;
		}

		/* there is a profiler chunk already */
		for (i = 0; i < profile->nstatements; i++)
		{
			profiler_stmt_reduced *prstmt;
			profiler_stmt *pstmt = &pinfo->stmts[i];

			if (stmt_counter >= STATEMENTS_PER_CHUNK)
			{
				hk.chunk_num += 1;

				chunk = (profiler_stmt_chunk *) hash_search(chunks,
															 (void *) &hk,
															 HASH_FIND,
															 &found);

				if (!found)
					elog(ERROR, "broken consistency of plpgsql_check profiler chunks");

				stmt_counter = 0;
			}

			prstmt = &chunk->stmts[stmt_counter++];

			if (prstmt->lineno != pstmt->lineno)
				elog(ERROR, "broken consistency of plpgsql_check profiler chunks");

			if (prstmt->us_max < pstmt->us_max)
				prstmt->us_max = pstmt->us_max;

			prstmt->us_total += pstmt->us_total;
			prstmt->rows += pstmt->rows;
			prstmt->exec_count += pstmt->exec_count;
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
 * PLpgSQL statements has not unique id. We can assign some unique id
 * that can be used for statements counters. Fast access to this id
 * is implemented via map structure. It is a array of lists structure.
 */
static void
profiler_update_map(profiler_profile *profile, PLpgSQL_stmt *stmt)
{
	int		lineno = stmt->lineno;
	profiler_map_entry *pme;

	if (lineno > profile->stmts_map_max_lineno)
	{
		int		lines;
		int		i;

		/* calculate new size of map */
		for (lines = profile->stmts_map_max_lineno; stmt->lineno < lines;)
			if (lines < 10000)
				lines *= 2;
			else
				lines += 10000;

		profile->stmts_map = realloc(profile->stmts_map,
									 lines * sizeof(profiler_map_entry));

		for (i = profile->stmts_map_max_lineno; i < lines; i++)
			profile->stmts_map[i].stmt = NULL;

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
}

/*
 * Returns statement id assigned to plpgsql statement. Should be
 * fast, because lineno is usually unique.
 */
static int
profiler_get_stmtid(profiler_profile *profile, PLpgSQL_stmt *stmt)
{
	int		lineno = stmt->lineno;
	profiler_map_entry *pme;

	if (lineno > profile->stmts_map_max_lineno)
		elog(ERROR, "broken statement map - too high lineno");

	pme = &profile->stmts_map[lineno];

	/* pme->stmt should not be null */
	if (!pme->stmt)
		elog(ERROR, "broken statement map - broken format");

	while (pme && pme->stmt != stmt)
		pme = pme->next;

	/* we should to find statement */
	if (!pme)
		elog(ERROR, "broken statement map - cannot to find statement");

	return pme->stmtid;
}

static void
profiler_touch_stmts(profiler_info *pinfo,
					 List *stmts,
					 bool generate_map,
					 bool finalize_profile,
					 int64 *nested_us_total)
{
	ListCell *lc;

	*nested_us_total = 0;

	foreach(lc, stmts)
	{
		int64		us_total = 0;

		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *) lfirst(lc);

		profiler_touch_stmt(pinfo,
							stmt,
							generate_map,
							finalize_profile,
							&us_total);

		if (finalize_profile)
			*nested_us_total += us_total;
	}
}

/*
 * This function should to iterate over all plpgsql commands to:
 *   count statements and build statement -> uniq id map,
 *   collect counted metrics.
 */
static void
profiler_touch_stmt(profiler_info *pinfo,
					PLpgSQL_stmt *stmt,
					bool generate_map,
					bool finalize_profile,
					int64 *nested_us_total)
{
	int64		us_total = 0;
	profiler_profile *profile = pinfo->profile;
	profiler_stmt *pstmt = NULL;

	if (generate_map)
		profiler_update_map(profile, stmt);

	if (finalize_profile)
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
									 generate_map,
									 finalize_profile,
									 &us_total);

				if (finalize_profile)
					*nested_us_total += us_total;

				if (stmt_block->exceptions)
				{
					ListCell *lc;

					foreach(lc, stmt_block->exceptions->exc_list)
					{
						profiler_touch_stmts(pinfo,
											 ((PLpgSQL_exception *) lfirst(lc))->action,
											 generate_map,
											 finalize_profile,
											 &us_total);

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
				ListCell *lc;

				profiler_touch_stmts(pinfo,
									 stmt_if->then_body,
									 generate_map,
									 finalize_profile,
									 &us_total);

				if (finalize_profile)
					*nested_us_total += us_total;

				foreach(lc, stmt_if->elsif_list)
				{
					profiler_touch_stmts(pinfo,
										 ((PLpgSQL_if_elsif *) lfirst(lc))->stmts,
										 generate_map,
										 finalize_profile,
										 &us_total);

					if (finalize_profile)
						*nested_us_total += us_total;
				}

				profiler_touch_stmts(pinfo,
									 stmt_if->else_body,
									 generate_map,
									 finalize_profile,
									 &us_total);

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

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;
				ListCell *lc;

				foreach(lc, stmt_case->case_when_list)
				{
					profiler_touch_stmts(pinfo,
										 ((PLpgSQL_case_when *) lfirst(lc))->stmts,
										 generate_map,
										 finalize_profile,
										 &us_total);

					if (finalize_profile)
						*nested_us_total += us_total;

				}

				profiler_touch_stmts(pinfo,
									 stmt_case->else_stmts,
									 generate_map,
									 finalize_profile,
									 &us_total);

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

		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_FORS:
		case PLPGSQL_STMT_FORC:
		case PLPGSQL_STMT_DYNFORS:
		case PLPGSQL_STMT_FOREACH_A:
			{
				List   *stmts;

				switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
				{
					case PLPGSQL_STMT_LOOP:
						stmts = ((PLpgSQL_stmt_while *) stmt)->body;
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
								 generate_map,
								 finalize_profile,
								 &us_total);

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
 * Try to search profile pattern for function. Creates profile pattern when
 * it doesn't exists.
 */
static void
profiler_func_init(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
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

		pinfo = palloc0(sizeof(profiler_info));
		pinfo->profile = profile;

		if (!found)
		{
			MemoryContext oldcxt;

			profile->nstatements = 0;
			profile->stmts_map_max_lineno = 200;

			oldcxt = MemoryContextSwitchTo(profiler_mcxt);
			profile->stmts_map = palloc0(profile->stmts_map_max_lineno * sizeof(profiler_map_entry));

			profiler_touch_stmt(pinfo, (PLpgSQL_stmt *) func->action, true, false, NULL);

			/* entry statements is not visible for plugin functions */
			profile->entry_stmt = (PLpgSQL_stmt *) func->action;

			MemoryContextSwitchTo(oldcxt);
		}

		pinfo->stmts = palloc0(profile->nstatements * sizeof(profiler_stmt));

		INSTR_TIME_SET_CURRENT(pinfo->start_time);

		estate->plugin_info = pinfo;
	}
}

static void
profiler_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	if (plpgsql_check_profiler &&
		estate->plugin_info &&
		func->fn_oid != InvalidOid)
	{
		profiler_info *pinfo = (profiler_info *) estate->plugin_info;
		profiler_profile *profile = pinfo->profile;
		int		entry_stmtid = profiler_get_stmtid(profile, profile->entry_stmt);
		instr_time		end_time;
		uint64			elapsed;
		int64			nested_us_total;

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
						   false,
						   true,
						   &nested_us_total);

		update_persistent_profile(pinfo, func);

		pfree(pinfo->stmts);
		pfree(pinfo);
	}
}

static void
profiler_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	if (plpgsql_check_profiler &&
		estate->plugin_info &&
		estate->func->fn_oid != InvalidOid)
	{
		profiler_info *pinfo = (profiler_info *) estate->plugin_info;
		profiler_profile *profile  = pinfo->profile;
		int stmtid = profiler_get_stmtid(profile, stmt);
		profiler_stmt *pstmt = &pinfo->stmts[stmtid];

		INSTR_TIME_SET_CURRENT(pstmt->start_time);
	}
}

static void
profiler_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	if (plpgsql_check_profiler && 
		estate->plugin_info && 
		estate->func->fn_oid != InvalidOid)
	{
		profiler_info *pinfo = (profiler_info *) estate->plugin_info;
		profiler_profile *profile  = pinfo->profile;
		int stmtid = profiler_get_stmtid(profile, stmt);
		profiler_stmt *pstmt = &pinfo->stmts[stmtid];
		instr_time		end_time;
		uint64			elapsed;
		instr_time		end_time2;

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

static void
tuplestore_put_profile(Tuplestorestate *tuple_store,
					   TupleDesc tupdesc,
					   int lineno,
					   int stmt_lineno,
					   int cmds_on_row,
					   int exec_count,
					   int64 us_total,
					   Datum max_time_array,
					   Datum processed_rows_array,
					   char *source_row)
{
	Datum	values[Natts_profiler];
	bool	nulls[Natts_profiler];

	SET_RESULT_NULL(Anum_profiler_stmt_lineno);
	SET_RESULT_NULL(Anum_profiler_exec_count);
	SET_RESULT_NULL(Anum_profiler_total_time);
	SET_RESULT_NULL(Anum_profiler_avg_time);
	SET_RESULT_NULL(Anum_profiler_max_time);
	SET_RESULT_NULL(Anum_profiler_processed_rows);
	SET_RESULT_NULL(Anum_profiler_source);
	SET_RESULT_NULL(Anum_profiler_cmds_on_row);

	SET_RESULT_INT32(Anum_profiler_lineno, lineno);
	SET_RESULT_TEXT(Anum_profiler_source, source_row);

	if (stmt_lineno > 0)
	{
		SET_RESULT_INT32(Anum_profiler_stmt_lineno, stmt_lineno);
		SET_RESULT_INT32(Anum_profiler_cmds_on_row, cmds_on_row);
		SET_RESULT_INT64(Anum_profiler_exec_count, exec_count);
		SET_RESULT_FLOAT8(Anum_profiler_total_time, us_total / 1000.0);
		SET_RESULT_FLOAT8(Anum_profiler_avg_time, ceil(((float8) us_total) / exec_count) / 1000.0);
		SET_RESULT(Anum_profiler_max_time, max_time_array);
		SET_RESULT(Anum_profiler_processed_rows, processed_rows_array);
	}

	tuplestore_putvalues(tuple_store, tupdesc, values, nulls);
}

static void
SetReturningFunctionCheck(ReturnSetInfo *rsinfo)
{
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));
}

Datum
plpgsql_profiler_function_tb(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	profiler_hashkey hk;
	HeapTuple	procTuple;
	bool found;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	TupleDesc	tupdesc;
	char	   *prosrc;
	Datum		prosrcdatum;
	bool		isnull;
	int			lineno = 1;
	int			current_statement = 0;
	profiler_stmt_chunk *chunk = NULL;
	profiler_stmt_chunk *first_chunk = NULL;
	HTAB	   *chunks;
	bool		shared_chunks;
	volatile bool		unlock_mutex = false;

	/* check to see if caller supports us returning a tuplestore */
	SetReturningFunctionCheck(rsinfo);

	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);

	prosrcdatum = SysCacheGetAttr(PROCOID, procTuple,
								  Anum_pg_proc_prosrc, &isnull);
	if (isnull)
		elog(ERROR, "null prosrc");

	prosrc = TextDatumGetCString(prosrcdatum);

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
		LWLockAcquire(profiler_ss->lock, LW_SHARED);
		chunks = shared_profiler_chunks_HashTable;
		shared_chunks = true;
	}
	else
	{
		chunks = profiler_chunks_HashTable;
		shared_chunks = false;
	}

	chunk = (profiler_stmt_chunk *) hash_search(chunks,
											 (void *) &hk,
											 HASH_FIND,
											 &found);


	/* need to build tuplestore in query context */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
	tupstore = tuplestore_begin_heap(false, false, work_mem);
	MemoryContextSwitchTo(oldcontext);

	PG_TRY();
	{
		if (shared_chunks && chunk)
		{
			first_chunk = chunk;
			SpinLockAcquire(&first_chunk->mutex);
			unlock_mutex = true;
		}

		while (*prosrc)
		{
			char	   *lineend = prosrc;
			char	   *linebeg = prosrc;
			int			stmt_lineno = -1;
			int64		us_total = 0;
			int64		exec_count = 0;
			Datum		max_time_array = (Datum) 0;
			Datum		processed_rows_array = (Datum) 0;
			int			cmds_on_row = 0;

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
				while (chunk->stmts[current_statement].lineno < lineno)
				{
					current_statement += 1;

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
				}

				if (chunk && chunk->stmts[current_statement].lineno == lineno)
				{
					ArrayBuildState *max_time_abs = NULL;
					ArrayBuildState *processed_rows_abs = NULL;

#if PG_VERSION_NUM >= 90500

					max_time_abs = initArrayResult(FLOAT8OID, CurrentMemoryContext, true);
					processed_rows_abs = initArrayResult(INT8OID, CurrentMemoryContext, true);

#endif

					stmt_lineno = lineno;

					/* try to collect all statements on the line */
					while (chunk->stmts[current_statement].lineno == lineno)
					{
						profiler_stmt_reduced *prstmt;

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

						if (!chunk)
							break;

						prstmt = &chunk->stmts[current_statement];

						us_total += prstmt->us_total;
						exec_count += prstmt->exec_count;

						cmds_on_row += 1;

						max_time_abs = accumArrayResult(max_time_abs,
														Float8GetDatum(prstmt->us_max / 1000.0), false,
														FLOAT8OID,
														CurrentMemoryContext);

						processed_rows_abs = accumArrayResult(processed_rows_abs,
															 Int64GetDatum(prstmt->rows), false,
															 INT8OID,
															 CurrentMemoryContext);

						current_statement += 1;
					}

					max_time_array = makeArrayResult(max_time_abs, CurrentMemoryContext);
					processed_rows_array = makeArrayResult(processed_rows_abs, CurrentMemoryContext);
				}
			}

			tuplestore_put_profile(tupstore, tupdesc,
								   lineno,
								   stmt_lineno,
								   cmds_on_row,
								   exec_count,
								   us_total,
								   max_time_array,
								   processed_rows_array,
								   linebeg);

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

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	return (Datum) 0;
}

/*
 * clean all chunks used by profiler
 */
Datum
plpgsql_profiler_reset_all(PG_FUNCTION_ARGS)
{
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
		profiler_init_hash_tables();

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
