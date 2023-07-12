#include "postgres.h"

#include "plpgsql.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "access/tupdesc.h"
#include "storage/ipc.h"

typedef uint64 pc_queryid;
#define NOQUERYID				(UINT64CONST(0))

#if PG_VERSION_NUM < 150000
#define parse_analyze_fixedparams	parse_analyze
#endif

enum
{
	PLPGSQL_CHECK_ERROR,
	PLPGSQL_CHECK_WARNING_OTHERS,
	PLPGSQL_CHECK_WARNING_EXTRA,					/* check shadowed variables */
	PLPGSQL_CHECK_WARNING_PERFORMANCE,				/* invisible cast check */
	PLPGSQL_CHECK_WARNING_SECURITY,					/* sql injection check */
	PLPGSQL_CHECK_WARNING_COMPATIBILITY				/* obsolete setting of cursor's or refcursor's variable */
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
	PLPGSQL_SHOW_PROFILE_STATEMENTS_TABULAR,
	PLPGSQL_SHOW_PROFILE_FUNCTIONS_ALL_TABULAR
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

typedef enum
{
	PLPGSQL_CHECK_STMT_WALKER_PREPARE_PROFILE,
	PLPGSQL_CHECK_STMT_WALKER_COUNT_EXEC_TIME,
	PLPGSQL_CHECK_STMT_WALKER_PREPARE_RESULT,
	PLPGSQL_CHECK_STMT_WALKER_COLLECT_COVERAGE
} profiler_stmt_walker_mode;

typedef struct PLpgSQL_stmt_stack_item
{
	PLpgSQL_stmt	   *stmt;
	char			   *label;
	struct PLpgSQL_stmt_stack_item *outer;
	bool				is_exception_handler;
} PLpgSQL_stmt_stack_item;

typedef struct plpgsql_check_result_info
{
	int			format;						/* produced / expected format */
	Tuplestorestate	*tuple_store;			/* target tuple store */
	TupleDesc	tupdesc;					/* target tuple store tuple descriptor */
	MemoryContext			query_ctx;		/* memory context for string operations */
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
	Oid			anyelementoid;
	Oid			anyenumoid;
	Oid			anyrangeoid;
	Oid			anycompatibleoid;
	Oid			anycompatiblerangeoid;
	PLpgSQL_trigtype trigtype;
	char	   *src;
	bool		fatal_errors;
	bool		other_warnings;
	bool		performance_warnings;
	bool		extra_warnings;
	bool		security_warnings;
	bool		compatibility_warnings;
	bool		show_profile;

	bool		all_warnings;
	bool		without_warnings;

	char	   *oldtable;
	char	   *newtable;

	bool		incomment_options_usage_warning;
} plpgsql_check_info;

typedef struct
{
	unsigned int disable_check : 1;
	unsigned int disable_tracer : 1;		/* has not any effect - it's runtime */
	unsigned int disable_other_warnings : 1;
	unsigned int disable_performance_warnings : 1;
	unsigned int disable_extra_warnings : 1;
	unsigned int disable_security_warnings : 1;
	unsigned int disable_compatibility_warnings : 1;
} plpgsql_check_pragma_vector;

#define CI_MAGIC		2023042922

typedef struct PLpgSQL_checkstate
{
	int			ci_magic;

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
	bool		found_return_dyn_query;		/* true, when code contains RETURN QUERY EXECUTE */
	Bitmapset	   *func_oids;				/* list of used (and displayed) functions */
	Bitmapset	   *rel_oids;				/* list of used (and displayed) relations */
	bool		fake_rtd;					/* true when functions returns record */
	plpgsql_check_result_info *result_info;
	plpgsql_check_info *cinfo;
	Bitmapset	   *safe_variables;			/* track which variables are safe against sql injection */
	Bitmapset	   *out_variables;			/* what variables are used as OUT variables */
	Bitmapset	   *protected_variables;	/* what variables should be assigned internal only */
	Bitmapset	   *auto_variables;			/* variables initialized, used by runtime */
	Bitmapset	   *typed_variables;		/* record variables with assigned type by pragma TYPE */
	bool			stop_check;				/* true after error when fatal_errors option is active */
	bool			allow_mp;				/* true, when multiple plans in plancache are allowed */
	bool			has_mp;					/* true, when multiple plan was used */
	bool			was_pragma;				/* true, when last expression was a plpgsql_check pragma */
	plpgsql_check_pragma_vector pragma_vector;
	Oid			pragma_foid;				/* oid of plpgsql_check pragma function */
} PLpgSQL_checkstate;

typedef struct
{
	int		statements;
	int		branches;
	int		executed_statements;
	int		executed_branches;
} coverage_state;

typedef struct
{
	unsigned long int run_id;
	int		level;
} tracer_info;

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
extern void plpgsql_check_is_assignable(PLpgSQL_execstate *estate, int dno);

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
extern void plpgsql_check_put_profile(plpgsql_check_result_info *ri, Datum queryids_array, int lineno, int stmt_lineno,
	int cmds_on_row, int64 exec_count, int64 exec_count_err, int64 us_total, Datum max_time_array, Datum processed_rows_array, char *source_row);
extern void plpgsql_check_put_profile_statement(plpgsql_check_result_info *ri, pc_queryid queryid, int stmtid, int parent_stmtid, const char *parent_note, int block_num, int lineno,
	int64 exec_stmts, int64 exec_count_err, double total_time, double max_time, int64 processed_rows, char *stmtname);
extern void plpgsql_check_put_profiler_functions_all_tb(plpgsql_check_result_info *ri, Oid funcoid, int64 exec_count, int64 exec_count_err,
	double total_time, double avg_time, double stddev_time, double min_time, double max_time);

/*
 * function from catalog.c
 */
extern bool plpgsql_check_is_eventtriggeroid(Oid typoid);
extern void plpgsql_check_get_function_info(plpgsql_check_info *cinfo);
extern void plpgsql_check_precheck_conditions(plpgsql_check_info *cinfo);
extern char *plpgsql_check_get_src(HeapTuple procTuple);
extern Oid plpgsql_check_pragma_func_oid(void);
extern bool plpgsql_check_is_plpgsql_function(Oid foid);
extern Oid plpgsql_check_get_op_namespace(Oid opno);
extern char *get_extension_version(Oid ext_oid);


/*
 * functions from tablefunc.c
 */
extern void plpgsql_check_info_init(plpgsql_check_info *cinfo, Oid fn_oid);
extern void plpgsql_check_set_all_warnings(plpgsql_check_info *cinfo);
extern void plpgsql_check_set_without_warnings(plpgsql_check_info *cinfo);

/*
 * functions from check_function.c
 */
extern void plpgsql_check_function_internal(plpgsql_check_result_info *ri, plpgsql_check_info *cinfo);
extern void plpgsql_check_on_func_beg(PLpgSQL_execstate * estate, PLpgSQL_function * func);
extern void plpgsql_check_HashTableInit(void);
extern bool plpgsql_check_is_checked(PLpgSQL_function *func);
extern void plpgsql_check_mark_as_checked(PLpgSQL_function *func);
extern void plpgsql_check_setup_fcinfo(plpgsql_check_info *cinfo, FmgrInfo *flinfo, FunctionCallInfo fcinfo,
	ReturnSetInfo *rsinfo, TriggerData *trigdata, EventTriggerData *etrigdata, Trigger *tg_trigger, bool *fake_rtd);

extern bool plpgsql_check_other_warnings;
extern bool plpgsql_check_extra_warnings;
extern bool plpgsql_check_performance_warnings;
extern bool plpgsql_check_compatibility_warnings;
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
extern bool plpgsql_check_contain_volatile_functions(Node *clause, PLpgSQL_checkstate *cstate);
extern bool plpgsql_check_contain_mutable_functions(Node *clause, PLpgSQL_checkstate *cstate);
extern bool plpgsql_check_vardno_is_used_for_reading(Node *node, int dno);

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

extern void plpgsql_check_assignment_to_variable(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
	PLpgSQL_variable *targetvar, int targetdno);

/*
 * functions from report.c
 */
extern char * plpgsql_check_datum_get_refname(PLpgSQL_datum *d);
extern void plpgsql_check_report_unused_variables(PLpgSQL_checkstate *cstate);
extern void plpgsql_check_report_too_high_volatility(PLpgSQL_checkstate *cstate);
extern bool is_internal_variable(PLpgSQL_checkstate *cstate, PLpgSQL_variable *var);

/*
 * functions from stmtwalk.c
 */
extern bool plpgsql_check_is_reserved_keyword(char *name);
extern void plpgsql_check_stmt(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt, int *closing, List **exceptions);

/*
 * functions from typdesc.c
 */
extern TupleDesc plpgsql_check_expr_get_desc(PLpgSQL_checkstate *cstate, PLpgSQL_expr *query,
	bool use_element_type, bool expand_record, bool is_expression, Oid *first_level_typoid);
extern void plpgsql_check_recvar_info(PLpgSQL_rec *rec, Oid *typoid, int32 *typmod);
extern PLpgSQL_row * plpgsql_check_CallExprGetRowTarget(PLpgSQL_checkstate *cstate, PLpgSQL_expr *CallExpr);

/*
 * functions from parser.c
 */
extern Oid plpgsql_check_parse_name_or_signature(char *name_or_signature);
extern bool plpgsql_check_pragma_type(PLpgSQL_checkstate *cstate, const char *str, PLpgSQL_nsitem *ns, int lineno);
extern bool plpgsql_check_pragma_table(PLpgSQL_checkstate *cstate, const char *str, int lineno);
extern bool plpgsql_check_pragma_sequence(PLpgSQL_checkstate *cstate, const char *str, int lineno);
extern void plpgsql_check_search_comment_options(plpgsql_check_info *cinfo);
extern char *plpgsql_check_process_echo_string(char *str, plpgsql_check_info *cinfo);

/*
 * functions from profiler.c
 */
extern bool plpgsql_check_profiler;
extern int plpgsql_check_profiler_max_shared_chunks;

extern needs_fmgr_hook_type		plpgsql_check_next_needs_fmgr_hook;
extern fmgr_hook_type			plpgsql_check_next_fmgr_hook;

#if PG_VERSION_NUM >= 150000
extern void plpgsql_check_profiler_shmem_request(void);
#endif
extern void plpgsql_check_profiler_shmem_startup(void);

extern Size plpgsql_check_shmem_size(void);
extern void plpgsql_check_profiler_init_hash_tables(void);

extern void plpgsql_check_iterate_over_profile(plpgsql_check_info *cinfo, profiler_stmt_walker_mode mode,
   plpgsql_check_result_info *ri, coverage_state *cs);

extern void plpgsql_check_profiler_show_profile(plpgsql_check_result_info *ri, plpgsql_check_info *cinfo);
extern void plpgsql_check_profiler_iterate_over_all_profiles(plpgsql_check_result_info *ri);

extern void plpgsql_check_init_trace_info(PLpgSQL_execstate *estate);
extern bool plpgsql_check_get_trace_info(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, PLpgSQL_execstate **outer_estate, int *frame_num, int *level, instr_time *start_time);

extern void plpgsql_check_get_trace_stmt_info(PLpgSQL_execstate *estate, int stmt_id, instr_time **start_time);
extern bool *plpgsql_check_get_disable_tracer_on_stack(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

extern void plpgsql_check_profiler_init(void);

/*
 * functions and variables from tracer.c
 */
extern bool plpgsql_check_enable_tracer;
extern bool plpgsql_check_tracer;
extern bool plpgsql_check_trace_assert;
extern bool plpgsql_check_tracer_test_mode;

extern int plpgsql_check_tracer_variable_max_length;
extern int plpgsql_check_tracer_errlevel;

extern PGErrorVerbosity plpgsql_check_tracer_verbosity;
extern PGErrorVerbosity plpgsql_check_trace_assert_verbosity;

extern bool plpgsql_check_regress_test_mode;

extern void plpgsql_check_tracer_on_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func);
extern void plpgsql_check_tracer_on_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func);
extern void plpgsql_check_tracer_on_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);
extern void plpgsql_check_tracer_on_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);
extern void plpgsql_check_trace_assert_on_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

extern void plpgsql_check_set_stmt_group_number(PLpgSQL_stmt *stmt, int *group_numbers, int *parent_group_numbers, int sgn, int *cgn, int psgn);

/*
 * variables from pragma.c
 */
extern void plpgsql_check_pragma_apply(PLpgSQL_checkstate *cstate, char *pragma_str, PLpgSQL_nsitem *ns, int lineno);

extern plpgsql_check_pragma_vector plpgsql_check_runtime_pragma_vector;
extern bool plpgsql_check_runtime_pragma_vector_changed;

/*
 * functions from pldbgapi2
 */
typedef struct plpgsql_check_plugin2
{
	/* Function pointers set up by the plugin */
	void		(*func_setup2) (PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info);
	void		(*func_beg2) (PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info);
	void		(*func_end2) (PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info, bool is_aborted);
	void		(*stmt_beg2) (PLpgSQL_execstate *estate, PLpgSQL_function *func, PLpgSQL_stmt *stmt, void **plugin2_info);
	void		(*stmt_end2) (PLpgSQL_execstate *estate, PLpgSQL_function *func, PLpgSQL_stmt *stmt, void **plugin2_info, bool is_aborted);

	/* Function pointers set by PL/pgSQL itself */
	void		(*error_callback) (void *arg);
	void		(*assign_expr) (PLpgSQL_execstate *estate,
								PLpgSQL_datum *target,
								PLpgSQL_expr *expr);

	void		(*assign_value) (PLpgSQL_execstate *estate,
								 PLpgSQL_datum *target,
								 Datum value, bool isNull,
								 Oid valtype, int32 valtypmod);
	void		(*eval_datum) (PLpgSQL_execstate *estate, PLpgSQL_datum *datum,
							   Oid *typeId, int32 *typetypmod,
							   Datum *value, bool *isnull);
	Datum		(*cast_value) (PLpgSQL_execstate *estate,
							   Datum value, bool *isnull,
							   Oid valtype, int32 valtypmod,
							   Oid reqtype, int32 reqtypmod);
} plpgsql_check_plugin2;

extern void plpgsql_check_register_pldbgapi2_plugin(plpgsql_check_plugin2 *plugin2);
extern void plpgsql_check_init_pldbgapi2(void);

#if PG_VERSION_NUM < 150000

extern void plpgsql_check_finish_pldbgapi2(void);

#endif

/*
 * functions from plpgsql_check.c
 */

#if PG_VERSION_NUM >= 150000
extern shmem_request_hook_type prev_shmem_request_hook;
#endif
extern shmem_startup_hook_type prev_shmem_startup_hook;

extern PLpgSQL_plugin **plpgsql_check_plugin_var_ptr;

extern void plpgsql_check_check_ext_version(Oid fn_oid);
extern void plpgsql_check_passive_check_init(void);

/*
 * Links to function in plpgsql module
 */
typedef PLpgSQL_type *(*plpgsql_check__build_datatype_t) (Oid typeOid, int32 typmod, Oid collation, TypeName *origtypname);

extern plpgsql_check__build_datatype_t plpgsql_check__build_datatype_p;
typedef PLpgSQL_function *(*plpgsql_check__compile_t) (FunctionCallInfo fcinfo, bool forValidator);
extern plpgsql_check__compile_t plpgsql_check__compile_p;
typedef void (*plpgsql_check__parser_setup_t) (struct ParseState *pstate, PLpgSQL_expr *expr);
extern plpgsql_check__parser_setup_t plpgsql_check__parser_setup_p;
typedef const char *(*plpgsql_check__stmt_typename_t) (PLpgSQL_stmt *stmt);
extern plpgsql_check__stmt_typename_t plpgsql_check__stmt_typename_p;
typedef Oid (*plpgsql_check__exec_get_datum_type_t) (PLpgSQL_execstate *estate, PLpgSQL_datum *datum);
extern plpgsql_check__exec_get_datum_type_t plpgsql_check__exec_get_datum_type_p;
typedef int (*plpgsql_check__recognize_err_condition_t) (const char *condname, bool allow_sqlstate);
extern plpgsql_check__recognize_err_condition_t plpgsql_check__recognize_err_condition_p;

typedef PLpgSQL_nsitem *(*plpgsql_check__ns_lookup_t) (PLpgSQL_nsitem *ns_cur, bool localmode,
													  const char *name1, const char *name2, const char *name3,
													  int *names_used);
extern plpgsql_check__ns_lookup_t plpgsql_check__ns_lookup_p;

#define NEVER_READ_VARIABLE_TEXT		"never read variable \"%s\""
#define NEVER_READ_VARIABLE_TEXT_CHECK_LENGTH		19
#define UNUSED_PARAMETER_TEXT			"unused parameter \"%s\""
#define NEVER_READ_PARAMETER_TEXT		"parameter \"%s\" is never read"
#define UNMODIFIED_VARIABLE_TEXT		"unmodified OUT variable \"%s\""
#define OUT_COMPOSITE_IS_NOT_SINGLE_TEXT	"composite OUT variable \"%s\" is not single argument"
#define UNUSED_VARIABLE_TEXT			"unused variable \"%s\""
#define UNUSED_VARIABLE_TEXT_CHECK_LENGTH	15
#define MAYBE_UNMODIFIED_VARIABLE_TEXT	"OUT variable \"%s\" is maybe unmodified"

/*
 * Expecting persistent oid of nextval, currval and setval functions.
 * Ensured by regress tests.
 */
#define NEXTVAL_OID		1574
#define CURRVAL_OID		1575
#define SETVAL_OID		1576
#define SETVAL2_OID		1765
#define FORMAT_0PARAM_OID	3540
#define FORMAT_NPARAM_OID	3539

#ifndef TupleDescAttr
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

#define recvar_tuple(rec)		(rec->erh ? expanded_record_get_tuple(rec->erh) : NULL)
#define recvar_tupdesc(rec)		(rec->erh ? expanded_record_fetch_tupdesc(rec->erh) : NULL)

#define FUNCS_PER_USER		128 /* initial table size */

#ifdef _MSC_VER

#define strcasecmp _stricmp
#define strncasecmp _strnicmp

#endif
