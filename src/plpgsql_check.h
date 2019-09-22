#include "postgres.h"

#include "plpgsql.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "access/tupdesc.h"
#include "storage/ipc.h"

enum
{
	PLPGSQL_CHECK_ERROR,
	PLPGSQL_CHECK_WARNING_OTHERS,
	PLPGSQL_CHECK_WARNING_EXTRA,					/* check shadowed variables */
	PLPGSQL_CHECK_WARNING_PERFORMANCE,				/* invisible cast check */
	PLPGSQL_CHECK_WARNING_SECURITY					/* sql injection check */
};

enum
{
	PLPGSQL_CHECK_FORMAT_ELOG,
	PLPGSQL_CHECK_FORMAT_TEXT,
	PLPGSQL_CHECK_FORMAT_TABULAR,
	PLPGSQL_CHECK_FORMAT_XML,
	PLPGSQL_CHECK_FORMAT_JSON,
	PLPGSQL_SHOW_DEPENDENCY_FORMAT_TABULAR,
	PLPGSQL_SHOW_PROFILE_TABULAR,
	PLPGSQL_SHOW_PROFILE_STATEMENTS_TABULAR
};

enum
{
	PLPGSQL_CHECK_MODE_DISABLED,		/* all functionality is disabled */
	PLPGSQL_CHECK_MODE_BY_FUNCTION,		/* checking is allowed via CHECK function only (default) */
	PLPGSQL_CHECK_MODE_FRESH_START,		/* check only when function is called first time */
	PLPGSQL_CHECK_MODE_EVERY_START		/* check on every start */
};

enum
{
	PLPGSQL_CHECK_CLOSED,
	PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS,
	PLPGSQL_CHECK_POSSIBLY_CLOSED,
	PLPGSQL_CHECK_UNCLOSED,
	PLPGSQL_CHECK_UNKNOWN
};

typedef struct PLpgSQL_stmt_stack_item
{
	PLpgSQL_stmt	   *stmt;
	char			   *label;
	struct PLpgSQL_stmt_stack_item *outer;
} PLpgSQL_stmt_stack_item;

typedef struct plpgsql_check_result_info
{
	int			format;						/* produced / expected format */
	Tuplestorestate	*tuple_store;			/* target tuple store */
	TupleDesc	tupdesc;					/* target tuple store tuple descriptor */
	StringInfo	sinfo;						/* buffer for multi line one value output formats */
	bool		init_tag;					/* true, when init tag should be created */
} plpgsql_check_result_info;

typedef struct plpgsql_check_info
{
	HeapTuple	proctuple;
	bool		is_procedure;
	Oid			fn_oid;
	Oid			rettype;
	char		volatility;
	Oid			relid;
	PLpgSQL_trigtype trigtype;
	char	   *src;
	bool		fatal_errors;
	bool		other_warnings;
	bool		performance_warnings;
	bool		extra_warnings;
	bool		security_warnings;
	bool		show_profile;
	char	   *oldtable;
	char	   *newtable;
} plpgsql_check_info;

typedef struct PLpgSQL_checkstate
{
	List	    *argnames;					/* function arg names */
	char		decl_volatility;			/* declared function volatility */
	char		volatility;					/* detected function volatility */
	bool		has_execute_stmt;			/* detected dynamic SQL, disable volatility check */
	bool		skip_volatility_check;		/* don't do this test for trigger */
	PLpgSQL_execstate	   *estate;			/* check state is estate extension */
	MemoryContext			check_cxt;
	List	   *exprs;						/* list of all expression created by checker */
	bool		is_active_mode;				/* true, when checking is started by plpgsql_check_function */
	Bitmapset  *used_variables;				/* track which variables have been used; bit per varno */
	Bitmapset  *modif_variables;			/* track which variables had been changed; bit per varno */
	PLpgSQL_stmt_stack_item *top_stmt_stack;	/* list of known labels + related command */
	bool		found_return_query;			/* true, when code contains RETURN query */
	Bitmapset	   *func_oids;				/* list of used (and displayed) functions */
	Bitmapset	   *rel_oids;				/* list of used (and displayed) relations */
	bool		fake_rtd;					/* true when functions returns record */
	plpgsql_check_result_info *result_info;
	plpgsql_check_info *cinfo;
	Bitmapset	   *safe_variables;			/* track which variables are safe against sql injection */
	Bitmapset	   *out_variables;			/* what variables are used as OUT variables */
	bool			stop_check;				/* true after error when fatal_errors option is active */
	bool			allow_mp;				/* true, when multiple plans in plancache are allowed */
	bool			has_mp;					/* true, when multiple plan was used */
} PLpgSQL_checkstate;


/*
 * function from assign.c
 */
extern void plpgsql_check_record_variable_usage(PLpgSQL_checkstate *cstate, int dno, bool write);
extern void plpgsql_check_row_or_rec(PLpgSQL_checkstate *cstate, PLpgSQL_row *row, PLpgSQL_rec *rec);
extern void plpgsql_check_target(PLpgSQL_checkstate *cstate, int varno, Oid *expected_typoid, int *expected_typmod);
extern void plpgsql_check_assign_to_target_type(PLpgSQL_checkstate *cstate,
	Oid target_typoid, int32 target_typmod, Oid value_typoid, bool isnull);
extern void plpgsql_check_assign_tupdesc_dno(PLpgSQL_checkstate *cstate, int varno, TupleDesc tupdesc, bool isnull);
extern void plpgsql_check_assign_tupdesc_row_or_rec(PLpgSQL_checkstate *cstate,
	PLpgSQL_row *row, PLpgSQL_rec *rec, TupleDesc tupdesc, bool isnull);
extern void plpgsql_check_recval_assign_tupdesc(PLpgSQL_checkstate *cstate, PLpgSQL_rec *rec, TupleDesc tupdesc, bool is_null);
extern void plpgsql_check_recval_init(PLpgSQL_rec *rec);
extern void plpgsql_check_recval_release(PLpgSQL_rec *rec);

/*
 * functions from format.c
 */
extern int plpgsql_check_format_num(char *format_str);
extern void plpgsql_check_init_ri(plpgsql_check_result_info *ri, int format, ReturnSetInfo *rsinfo);
extern void plpgsql_check_finalize_ri(plpgsql_check_result_info *ri);
extern void plpgsql_check_put_error(PLpgSQL_checkstate *cstate, int sqlerrcode, int lineno,
	const char *message, const char *detail, const char *hint, int level, int position, const char *query, const char *context);
extern void plpgsql_check_put_error_edata(PLpgSQL_checkstate *cstate, ErrorData *edata);
extern void plpgsql_check_put_dependency(plpgsql_check_result_info *ri, char *type, Oid oid, char *schema, char *name, char *params);
extern void plpgsql_check_put_profile(plpgsql_check_result_info *ri, int lineno, int stmt_lineno,
	int cmds_on_row, int exec_count, int64 us_total, Datum max_time_array, Datum processed_rows_array, char *source_row);
extern void plpgsql_check_put_profile_statement(plpgsql_check_result_info *ri, int stmtid, int parent_stmtid, const char *parent_note, int block_num, int lineno,
	int64 exec_stmts, double total_time, double max_time, int64 processed_rows, char *stmtname);

/*
 * function from catalog.c
 */
extern void plpgsql_check_get_function_info(HeapTuple procTuple, Oid *rettype, char *volatility, PLpgSQL_trigtype *trigtype, bool *is_procedure);
extern void plpgsql_check_precheck_conditions(plpgsql_check_info *cinfo);
extern char * plpgsql_check_get_src(HeapTuple procTuple);

/*
 * functions from tablefunc.c
 */

/*
 * functions from profiler.c
 */
extern void plpgsql_check_profiler_shmem_startup(void);
extern void plpgsql_check_profiler_show_profile(plpgsql_check_result_info *ri, plpgsql_check_info *cinfo);

/*
 * functions from check_function.c
 */
extern void plpgsql_check_function_internal(plpgsql_check_result_info *ri, plpgsql_check_info *cinfo);
extern void plpgsql_check_on_func_beg(PLpgSQL_execstate * estate, PLpgSQL_function * func);
extern void plpgsql_check_HashTableInit(void);
extern bool plpgsql_check_is_checked(PLpgSQL_function *func);
extern void plpgsql_check_mark_as_checked(PLpgSQL_function *func);
extern void plpgsql_check_setup_fcinfo(HeapTuple procTuple, FmgrInfo *flinfo, FunctionCallInfo fcinfo,
	ReturnSetInfo *rsinfo, TriggerData *trigdata, Oid relid, EventTriggerData *etrigdata, Oid funcoid,
	Oid rettype, PLpgSQL_trigtype trigtype, Trigger *tg_trigger, bool *fake_rtd,
	char *oldtable, char *newtable);


extern bool plpgsql_check_other_warnings;
extern bool plpgsql_check_extra_warnings;
extern bool plpgsql_check_performance_warnings;
extern bool plpgsql_check_fatal_errors;
extern int plpgsql_check_mode;

/*
 * functions from expr_walk.c
 */
extern void plpgsql_check_detect_dependency(PLpgSQL_checkstate *cstate, Query *query);
extern bool plpgsql_check_has_rtable(Query *query);
extern bool plpgsql_check_qual_has_fishy_cast(PlannedStmt *plannedstmt, Plan *plan, Param **param);
extern void plpgsql_check_funcexpr(PLpgSQL_checkstate *cstate, Query *query, char *query_str);
extern bool plpgsql_check_is_sql_injection_vulnerable(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, Node *node, int *location);

/*
 * functions from check_expr.c
 */
extern char *plpgsql_check_expr_get_string(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool *isnull);
extern void plpgsql_check_expr_with_scalar_type(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, Oid expected_typoid, bool required);
extern void plpgsql_check_returned_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool is_expression);
extern void plpgsql_check_expr_as_rvalue(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
	PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow, int targetdno, bool use_element_type, bool is_expression);
extern void plpgsql_check_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
extern void plpgsql_check_assignment_with_possible_slices(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
	PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow, int targetdno, bool use_element_type);
extern void plpgsql_check_expr_as_sqlstmt_nodata(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
extern void plpgsql_check_expr_as_sqlstmt_data(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
extern bool plpgsql_check_expr_as_sqlstmt(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
extern void plpgsql_check_assignment(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
	PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow, int targetdno);
extern void plpgsql_check_expr_generic(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
extern void plpgsql_check_expr_generic_with_parser_setup(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
	ParserSetupHook parser_setup, void *arg);

extern Node *plpgsql_check_expr_get_node(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool force_plan_checks);
extern char *plpgsql_check_const_to_string(Const *c);
extern CachedPlanSource *plpgsql_check_get_plan_source(PLpgSQL_checkstate *cstate, SPIPlanPtr plan);

#if PG_VERSION_NUM >= 110000

extern void plpgsql_check_assignment_to_variable(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
	PLpgSQL_variable *targetvar, int targetdno);

#endif

/*
 * functions from report.c
 */
extern char * plpgsql_check_datum_get_refname(PLpgSQL_datum *d);
extern void plpgsql_check_report_unused_variables(PLpgSQL_checkstate *cstate);
extern void plpgsql_check_report_too_high_volatility(PLpgSQL_checkstate *cstate);
extern bool is_internal_variable(PLpgSQL_variable *var);

/*
 * functions from stmtwalk.c
 */
extern bool plpgsql_check_is_reserved_keyword(char *name);
extern void plpgsql_check_stmt(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt, int *closing, List **exceptions);

/*
 * functions from typdesc.c
 */
extern TupleDesc plpgsql_check_expr_get_desc(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query,
	bool use_element_type, bool expand_record, bool is_expression, Oid *first_level_typoid);;

#if PG_VERSION_NUM >= 110000

extern PLpgSQL_row * plpgsql_check_CallExprGetRowTarget(PLpgSQL_checkstate *cstate, PLpgSQL_expr *CallExpr);

#endif

/*
 * functions from profiler.c
 */
extern Size plpgsql_check_shmem_size(void);
extern void plpgsql_check_profiler_init_hash_tables(void);

extern void plpgsql_check_profiler_func_init(PLpgSQL_execstate *estate, PLpgSQL_function *func);
extern void plpgsql_check_profiler_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func);
extern void plpgsql_check_profiler_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);
extern void plpgsql_check_profiler_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

extern void plpgsql_check_profiler_show_profile(plpgsql_check_result_info *ri, plpgsql_check_info *cinfo);
extern void plpgsql_check_profiler_show_profile_statements(plpgsql_check_result_info *ri, plpgsql_check_info *cinfo);

extern bool plpgsql_check_profiler;

/*
 * functions from plpgsql_check.c
 */

extern shmem_startup_hook_type prev_shmem_startup_hook;

#define NEVER_READ_VARIABLE_TEXT		"never read variable \"%s\""
#define NEVER_READ_VARIABLE_TEXT_CHECK_LENGTH		19
#define UNUSED_PARAMETER_TEXT			"unused parameter \"%s\""
#define NEVER_READ_PARAMETER_TEXT		"parameter \"%s\" is never read"
#define UNMODIFIED_VARIABLE_TEXT		"unmodified OUT variable \"%s\""
#define OUT_COMPOSITE_IS_NOT_SINGLE_TEXT	"composite OUT variable \"%s\" is not single argument"
#define UNUSED_VARIABLE_TEXT			"unused variable \"%s\""
#define UNUSED_VARIABLE_TEXT_CHECK_LENGTH	15
#define NEVER_READ_VARIABLE_TEXT		"never read variable \"%s\""
#define NEVER_READ_VARIABLE_TEXT_CHECK_LENGTH		19
#define UNUSED_PARAMETER_TEXT			"unused parameter \"%s\""
#define NEVER_READ_PARAMETER_TEXT		"parameter \"%s\" is never read"
#define UNMODIFIED_VARIABLE_TEXT		"unmodified OUT variable \"%s\""
#define OUT_COMPOSITE_IS_NOT_SINGLE_TEXT	"composite OUT variable \"%s\" is not single argument"
#define UNSAFE_EXECUTE					"the expression used by EXECUTE command is possibly sql injection vulnerable"

#ifndef TupleDescAttr
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

#if PG_VERSION_NUM >= 110000

#define recvar_tuple(rec)		(rec->erh ? expanded_record_get_tuple(rec->erh) : NULL)
#define recvar_tupdesc(rec)		(rec->erh ? expanded_record_fetch_tupdesc(rec->erh) : NULL)

#define is_procedure(estate)		((estate)->func && (estate)->func->fn_rettype == InvalidOid)

#else

#define recvar_tuple(rec)		(rec->tup)
#define recvar_tupdesc(rec)		(rec->tupdesc)

#define is_procedure(estate)	(false)

#endif


#if PG_VERSION_NUM >= 100000

#define PLPGSQL_STMT_TYPES

#else

#define PLPGSQL_STMT_TYPES		(enum PLpgSQL_stmt_types)

#endif

#define FUNCS_PER_USER		128 /* initial table size */
