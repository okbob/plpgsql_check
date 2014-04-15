/*-------------------------------------------------------------------------
 *
 * plpgsql_check.c
 *
 *			  extended checks for plpgsql functions
 *
 * by Pavel Stehule 2013
 *
 *-------------------------------------------------------------------------
 *
 * Notes:
 *
 * 1) We have to clean all plans inside expressions created in checking time, because we
 *    use a fake expression context (?? and fake fcinfo ??), and we don't try to use a fast
 *    query plans - so we don't wont to use plans from check time in productyion - We 
 *    cannot remove compiled function due missing access to function cache hash. 
 *    Possible robust solution is a copying every expression before check and using a copy. 
 *    All copies will be destroyed by dropping related memory context.
 *
 * 2) We have to have workable environment for plpgsql_parser_setup function
 *
 * 3) We need a own edition of signatures and oids as protection agains repeated check.
 *
 */

#include "plpgsql.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/spi_priv.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/rel.h"
#include "utils/xml.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * Interface
 *
 */
void _PG_init(void);

Datum plpgsql_check_function_tb(PG_FUNCTION_ARGS);
Datum plpgsql_check_function(PG_FUNCTION_ARGS);

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

enum
{
	PLPGSQL_CHECK_ERROR,
	PLPGSQL_CHECK_WARNING_OTHERS,
	PLPGSQL_CHECK_WARNING_PERFORMANCE
};

enum
{
	PLPGSQL_CHECK_FORMAT_ELOG,
	PLPGSQL_CHECK_FORMAT_TEXT,
	PLPGSQL_CHECK_FORMAT_TABULAR,
	PLPGSQL_CHECK_FORMAT_XML
};

enum
{
	PLPGSQL_CHECK_MODE_DISABLED,		/* all functionality is disabled */
	PLPGSQL_CHECK_MODE_BY_FUNCTION,	/* checking is allowed via CHECK function only (default) */
	PLPGSQL_CHECK_MODE_FRESH_START,	/* check only when function is called first time */
	PLPGSQL_CHECK_MODE_EVERY_START	/* check on every start */
};

typedef struct PLpgSQL_checkstate
{
	Oid		fn_oid;		/* oid of checked function */
	List	 	*argnames;	/* function arg names */
	PLpgSQL_execstate *estate;	/* check state is estate extension */
	Tuplestorestate *tuple_store;	/* result target */
	TupleDesc	tupdesc;	/* result description */
	bool	    fatal_errors;	/* stop on first error */
	bool	    performance_warnings;	/* show performace warnings */
	bool	    other_warnings;	/* show other warnings */
	int	 	   format;	/* output format */
	StringInfo	   sinfo;	/* aux. stringInfo used for result string concat */
	MemoryContext		   check_cxt;
	List			   *exprs;	/* list of all expression created by checker */
	bool		is_active_mode;	/* true, when checking is started by plpgsql_check_function */
	Bitmapset *used_variables; /* track which variables have been used; bit per varno */
}	PLpgSQL_checkstate;


static void assign_tupdesc_dno(PLpgSQL_checkstate *cstate, int varno, TupleDesc tupdesc);
static void assign_tupdesc_row_or_rec(PLpgSQL_checkstate *cstate,
						  PLpgSQL_row *row, PLpgSQL_rec *rec,
						  TupleDesc tupdesc);
static void check_assignment(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
				 PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
				 int targetdno);
static void check_element_assignment(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
						 PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
						 int targetdno);
static void check_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
static void check_expr_as_rvalue(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
					  PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
				   int targetdno, bool use_element_type, bool is_expression);
static void check_function_epilog(PLpgSQL_checkstate *cstate);
static void check_function_prolog(PLpgSQL_checkstate *cstate);
static void check_on_func_beg(PLpgSQL_execstate * estate, PLpgSQL_function * func);
static void check_plpgsql_function(HeapTuple procTuple, Oid relid, PLpgSQL_trigtype trigtype,
					   TupleDesc tupdesc,
					   Tuplestorestate *tupstore,
							    int format,
									  bool fatal_errors,
									  bool other_warnings,
									  bool performance_warnings);
static void check_row_or_rec(PLpgSQL_checkstate *cstate, PLpgSQL_row *row, PLpgSQL_rec *rec);
static void check_stmt(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt);
static void check_stmts(PLpgSQL_checkstate *cstate, List *stmts);
static void check_target(PLpgSQL_checkstate *cstate, int varno);
static PLpgSQL_datum *copy_plpgsql_datum(PLpgSQL_datum *datum);
static char *datum_get_refname(PLpgSQL_datum *d);
static TupleDesc expr_get_desc(PLpgSQL_checkstate *cstate,
							  PLpgSQL_expr *query,
										  bool use_element_type,
										  bool expand_record,
										  bool is_expression);
static void format_error_xml(StringInfo str,
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
							 Oid fn_oid, TupleDesc tupdesc, Tuplestorestate *tupstore,
							 bool fatal_errors, bool other_warnings, bool perform_warnings,
												    int format,
												    bool is_active_mode);
static void setup_fake_fcinfo(FmgrInfo *flinfo,
						 FunctionCallInfoData *fcinfo,
										 TriggerData *trigdata,
										 EventTriggerData *etrigdata,
										 Oid funcoid,
										 PLpgSQL_trigtype trigtype);
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
static void record_variable_usage(PLpgSQL_checkstate *cstate, int dno);


static bool plpgsql_check_other_warnings = false;
static bool plpgsql_check_performance_warnings = false;
static bool plpgsql_check_fatal_errors = true;
static int plpgsql_check_mode = PLPGSQL_CHECK_MODE_BY_FUNCTION;

static PLpgSQL_plugin plugin_funcs = { NULL, check_on_func_beg, NULL, NULL, NULL};

static const struct config_enum_entry plpgsql_check_mode_options[] = {
	{"disabled", PLPGSQL_CHECK_MODE_DISABLED, false},
	{"by_function", PLPGSQL_CHECK_MODE_BY_FUNCTION, false},
	{"fresh_start", PLPGSQL_CHECK_MODE_FRESH_START, false},
	{"every_start", PLPGSQL_CHECK_MODE_EVERY_START, false},
	{NULL, 0, false}
};

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

	plpgsql_check_HashTableInit();

	inited = true;
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

		/*
		 * don't allow repeated execution on checked function
		 * when it is not requsted. 
		 */
		if (plpgsql_check_mode == PLPGSQL_CHECK_MODE_FRESH_START &&
			is_checked(func))
		{
			elog(NOTICE, "function \"%s\" was checked already",
							    func->fn_signature);
			return;
		}

		mark_as_checked(func);

		setup_cstate(&cstate, func->fn_oid, NULL, NULL,
							    plpgsql_check_fatal_errors,
							    plpgsql_check_other_warnings,
							    plpgsql_check_performance_warnings,
							    PLPGSQL_CHECK_FORMAT_ELOG,
							    false);

		/* use real estate */
		cstate.estate = estate;

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

				saved_records[i].tup = rec->tup;
				saved_records[i].tupdesc = rec->tupdesc;
				saved_records[i].freetup = rec->freetup;
				saved_records[i].freetupdesc = rec->freetupdesc;

				/* don't release a original tupdesc and original tup */
				rec->freetup = false;
				rec->freetupdesc = false;
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
			check_stmt(&cstate, (PLpgSQL_stmt *) func->action);
			report_unused_variables(&cstate);
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

				if (rec->freetupdesc)
					FreeTupleDesc(rec->tupdesc);

				rec->tup = saved_records[i].tup;
				rec->tupdesc = saved_records[i].tupdesc;
				rec->freetup = saved_records[i].freetup;
				rec->freetupdesc = saved_records[i].freetupdesc;
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
PG_FUNCTION_INFO_V1(plpgsql_check_function);

Datum
plpgsql_check_function(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	Oid			relid = PG_GETARG_OID(1);
	char			*format_str = text_to_cstring(PG_GETARG_TEXT_PP(2));
	bool			fatal_errors = PG_GETARG_BOOL(3);
	bool			other_warnings = PG_GETARG_BOOL(4);
	bool			performance_warnings = PG_GETARG_BOOL(5);
	TupleDesc	tupdesc;
	HeapTuple	procTuple;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	PLpgSQL_trigtype trigtype;
	char *format_lower_str;
	int format;

	format_lower_str = lowerstr(format_str);
	if (strcmp(format_lower_str, "text") == 0)
		format = PLPGSQL_CHECK_FORMAT_TEXT;
	else if (strcmp(format_lower_str, "xml") == 0)
		format = PLPGSQL_CHECK_FORMAT_XML;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognize format: \"%s\"",
									 format_lower_str),
				 errhint("Only \"text\" and \"xml\" formats are supported.")));

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

	check_plpgsql_function(procTuple, relid, trigtype,
							   tupdesc, tupstore,
							   format,
								   fatal_errors, other_warnings, performance_warnings);

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
PG_FUNCTION_INFO_V1(plpgsql_check_function_tb);

Datum
plpgsql_check_function_tb(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	Oid			relid = PG_GETARG_OID(1);
	bool			fatal_errors = PG_GETARG_BOOL(2);
	bool			other_warnings = PG_GETARG_BOOL(3);
	bool			performance_warnings = PG_GETARG_BOOL(4);
	TupleDesc	tupdesc;
	HeapTuple	procTuple;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	PLpgSQL_trigtype trigtype;

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

	check_plpgsql_function(procTuple, relid, trigtype,
							   tupdesc, tupstore,
							   PLPGSQL_CHECK_FORMAT_TABULAR,
								   fatal_errors, other_warnings, performance_warnings);

	ReleaseSysCache(procTuple);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	return (Datum) 0;
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
	funcname = format_procedure(HeapTupleGetOid(procTuple));

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
									  bool performance_warnings)
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
	int			rc;
	ResourceOwner oldowner;
	PLpgSQL_execstate *cur_estate = NULL;
	MemoryContext old_cxt;
	PLpgSQL_execstate estate;

	funcoid = HeapTupleGetOid(procTuple);

	/*
	 * Connect to SPI manager
	 */
	if ((rc = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));

	setup_fake_fcinfo(&flinfo, &fake_fcinfo, &trigdata, &etrigdata,
									  funcoid, trigtype);

	setup_cstate(&cstate, funcoid, tupdesc, tupstore,
							    fatal_errors, other_warnings, performance_warnings,
										    format,
										    true);

	old_cxt = MemoryContextSwitchTo(cstate.check_cxt);

	check_function_prolog(&cstate);

	if (OidIsValid(relid))
		trigdata.tg_relation = relation_open(relid, AccessShareLock);

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

	/*
	 * Make local execution copies of all the datums
	 */
	for (i = 0; i < cstate->estate->ndatums; i++)
		cstate->estate->datums[i] = copy_plpgsql_datum(func->datums[i]);

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
	check_stmt(cstate, (PLpgSQL_stmt *) func->action);
	report_unused_variables(cstate);
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

	/*
	 * Make local execution copies of all the datums
	 */
	for (i = 0; i < cstate->estate->ndatums; i++)
		cstate->estate->datums[i] = copy_plpgsql_datum(func->datums[i]);

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
		rec_new = (PLpgSQL_rec *) (cstate->estate->datums[func->new_varno]);
		rec_new->freetup = false;
		rec_new->freetupdesc = false;
		assign_tupdesc_row_or_rec(cstate, NULL, rec_new, trigdata->tg_relation->rd_att);

		rec_old = (PLpgSQL_rec *) (cstate->estate->datums[func->old_varno]);
		rec_old->freetup = false;
		rec_old->freetupdesc = false;
		assign_tupdesc_row_or_rec(cstate, NULL, rec_old, trigdata->tg_relation->rd_att);

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
	}
	else if (IsA(tdata, EventTriggerData))
	{
		init_datum_dno(cstate, func->tg_event_varno);
		init_datum_dno(cstate, func->tg_tag_varno);
	}
	else
		elog(ERROR, "unexpected environment");

	/*
	 * Now check the toplevel block of statements
	 */
	check_stmt(cstate, (PLpgSQL_stmt *) func->action);
	report_unused_variables(cstate);
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
 * Set up a fake fcinfo with just enough info to satisfy plpgsql_compile().
 *
 * There should be a different real argtypes for polymorphic params.
 *
 */
static void
setup_fake_fcinfo(FmgrInfo *flinfo,
						  FunctionCallInfoData *fcinfo,
						  TriggerData *trigdata,
						  EventTriggerData *etrigdata,
						  Oid funcoid,
						  PLpgSQL_trigtype trigtype)
{
	/* clean structures */
	MemSet(fcinfo, 0, sizeof(FunctionCallInfoData));
	MemSet(flinfo, 0, sizeof(FmgrInfo));

	fcinfo->flinfo = flinfo;
	flinfo->fn_oid = funcoid;
	flinfo->fn_mcxt = CurrentMemoryContext;

	if (trigtype == PLPGSQL_DML_TRIGGER)
	{
		Assert(trigdata != NULL);

		MemSet(trigdata, 0, sizeof(trigdata));
		trigdata->type = T_TriggerData;
		fcinfo->context = (Node *) trigdata;
	}
	else if (trigtype == PLPGSQL_EVENT_TRIGGER)
	{
		MemSet(etrigdata, 0, sizeof(etrigdata));
		etrigdata->type = T_EventTriggerData;
		fcinfo->context = (Node *) etrigdata;
	}
}

/*
 * prepare PLpgSQL_checkstate structure
 *
 */
static void
setup_cstate(PLpgSQL_checkstate *cstate,
			 Oid fn_oid,
			 TupleDesc tupdesc,
			 Tuplestorestate *tupstore,
			 bool fatal_errors,
			 bool other_warnings,
			 bool performance_warnings,
			 int format,
			 bool is_active_mode)
{
	cstate->fn_oid = fn_oid;
	cstate->estate = NULL;
	cstate->tupdesc = tupdesc;
	cstate->tuple_store = tupstore;
	cstate->fatal_errors = fatal_errors;
	cstate->other_warnings = other_warnings;
	cstate->performance_warnings = performance_warnings;
	cstate->argnames = NIL;
	cstate->exprs = NIL;
	cstate->used_variables = NULL;

	cstate->format = format;
	cstate->is_active_mode = is_active_mode;

	cstate->sinfo = NULL;

	cstate->check_cxt = AllocSetContextCreate(CurrentMemoryContext,
										 "plpgsql_check temporary cxt",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);
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

	estate->rettupdesc = NULL;
	estate->exitlabel = NULL;
	estate->cur_error = NULL;

	estate->tuple_store = NULL;
	if (rsi)
	{
		estate->tuple_store_cxt = rsi->econtext->ecxt_per_query_memory;
		estate->tuple_store_owner = CurrentResourceOwner;
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
	estate->eval_lastoid = InvalidOid;
	estate->eval_econtext = NULL;
	estate->cur_expr = NULL;

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
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) cstate->estate->datums[dno];

				var->value = (Datum) 0;
				var->isnull = true;
				var->freeval = false;
			}
			break;

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
copy_plpgsql_datum(PLpgSQL_datum *datum)
{
	PLpgSQL_datum *result;

	switch (datum->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
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
				/* Ensure the value is null (possibly not needed?) */
				new->tup = NULL;
				new->tupdesc = NULL;
				new->freetup = false;
				new->freetupdesc = false;

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
check_stmt(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt)
{
	TupleDesc	tupdesc = NULL;
	PLpgSQL_function *func;
	ListCell   *l;
	ResourceOwner oldowner;
	MemoryContext oldCxt = CurrentMemoryContext;

	if (stmt == NULL)
		return;

	cstate->estate->err_stmt = stmt;
	func = cstate->estate->func;

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		switch ((enum PLpgSQL_stmt_types) stmt->cmd_type)
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
						}
					}

					check_stmts(cstate, stmt_block->body);

					if (stmt_block->exceptions)
					{
						foreach(l, stmt_block->exceptions->exc_list)
						{
							check_stmts(cstate, ((PLpgSQL_exception *) lfirst(l))->action);
						}

						/*
						 * Mark the hidden variables SQLSTATE and SQLERRM used
						 * even if they actually weren't.  Not using them
						 * should practically never be a sign of a problem, so
						 * there's no point in annoying the user.
						 */
						record_variable_usage(cstate, stmt_block->exceptions->sqlstate_varno);
						record_variable_usage(cstate, stmt_block->exceptions->sqlerrm_varno);
					}
				}
				break;

			case PLPGSQL_STMT_ASSIGN:
				{
					PLpgSQL_stmt_assign *stmt_assign = (PLpgSQL_stmt_assign *) stmt;

					check_target(cstate, stmt_assign->varno);

					/* prepare plan if desn't exist yet */
					check_assignment(cstate, stmt_assign->expr, NULL, NULL,
									 stmt_assign->varno);

					/*
					 * XXX: i thínk I lost some args to prepare_expr here
					 */
				}
				break;

			case PLPGSQL_STMT_IF:
				{
					PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;
					ListCell   *l;

					check_expr(cstate, stmt_if->cond);
					check_stmts(cstate, stmt_if->then_body);
					foreach(l, stmt_if->elsif_list)
					{
						PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(l);

						check_expr(cstate, elif->cond);
						check_stmts(cstate, elif->stmts);
					}

					check_stmts(cstate, stmt_if->else_body);
				}
				break;

			case PLPGSQL_STMT_CASE:
				{
					PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;
					Oid			result_oid;

					if (stmt_case->t_expr != NULL)
					{
						PLpgSQL_var *t_var = (PLpgSQL_var *) cstate->estate->datums[stmt_case->t_varno];

						/*
						 * we need to set hidden variable type
						 */
						prepare_expr(cstate, stmt_case->t_expr, 0);
						tupdesc = expr_get_desc(cstate,
												stmt_case->t_expr,
												false,	/* no element type */
												true,	/* expand record */
												true);	/* is expression */
						result_oid = tupdesc->attrs[0]->atttypid;

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
						check_stmts(cstate, cwt->stmts);
					}

					check_stmts(cstate, stmt_case->else_stmts);
				}
				break;

			case PLPGSQL_STMT_LOOP:
				check_stmts(cstate, ((PLpgSQL_stmt_loop *) stmt)->body);
				break;

			case PLPGSQL_STMT_WHILE:
				{
					PLpgSQL_stmt_while *stmt_while = (PLpgSQL_stmt_while *) stmt;

					check_expr(cstate, stmt_while->cond);
					check_stmts(cstate, stmt_while->body);
				}
				break;

			case PLPGSQL_STMT_FORI:
				{
					PLpgSQL_stmt_fori *stmt_fori = (PLpgSQL_stmt_fori *) stmt;

					check_expr(cstate, stmt_fori->lower);
					check_expr(cstate, stmt_fori->upper);
					check_expr(cstate, stmt_fori->step);
					check_stmts(cstate, stmt_fori->body);
				}
				break;

			case PLPGSQL_STMT_FORS:
				{
					PLpgSQL_stmt_fors *stmt_fors = (PLpgSQL_stmt_fors *) stmt;

					check_row_or_rec(cstate, stmt_fors->row, stmt_fors->rec);

					/* we need to set hidden variable type */
					check_assignment(cstate, stmt_fors->query,
									 stmt_fors->rec, stmt_fors->row, -1);

					check_stmts(cstate, stmt_fors->body);
				}
				break;

			case PLPGSQL_STMT_FORC:
				{
					PLpgSQL_stmt_forc *stmt_forc = (PLpgSQL_stmt_forc *) stmt;
					PLpgSQL_var *var = (PLpgSQL_var *) func->datums[stmt_forc->curvar];

					check_row_or_rec(cstate, stmt_forc->row, stmt_forc->rec);

					check_expr(cstate, stmt_forc->argquery);

					if (var->cursor_explicit_expr != NULL)
						check_assignment(cstate, var->cursor_explicit_expr,
										 stmt_forc->rec, stmt_forc->row, -1);

					check_stmts(cstate, stmt_forc->body);
				}
				break;

			case PLPGSQL_STMT_DYNFORS:
				{
					PLpgSQL_stmt_dynfors *stmt_dynfors = (PLpgSQL_stmt_dynfors *) stmt;

					if (stmt_dynfors->rec != NULL)
					{
						put_error(cstate,
									  0, 0,
								"cannot determinate a result of dynamic SQL",
									  "Cannot to contine in check.",
					  "Don't use dynamic SQL and record type together, when you would check function.",
									  PLPGSQL_CHECK_WARNING_OTHERS,
									  0, NULL, NULL);

						/*
						 * don't continue in checking. Behave should be
						 * indeterministic.
						 */
						break;
					}
					check_expr(cstate, stmt_dynfors->query);

					foreach(l, stmt_dynfors->params)
					{
						check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}

					check_stmts(cstate, stmt_dynfors->body);
				}
				break;

			case PLPGSQL_STMT_FOREACH_A:
				{
					PLpgSQL_stmt_foreach_a *stmt_foreach_a = (PLpgSQL_stmt_foreach_a *) stmt;

					check_target(cstate, stmt_foreach_a->varno);

					check_element_assignment(cstate, stmt_foreach_a->expr, NULL, NULL, stmt_foreach_a->varno);

					check_stmts(cstate, stmt_foreach_a->body);
				}
				break;

			case PLPGSQL_STMT_EXIT:
				check_expr(cstate, ((PLpgSQL_stmt_exit *) stmt)->cond);
				break;

			case PLPGSQL_STMT_PERFORM:
				check_expr(cstate, ((PLpgSQL_stmt_perform *) stmt)->expr);
				break;

			case PLPGSQL_STMT_RETURN:
				check_expr(cstate, ((PLpgSQL_stmt_return *) stmt)->expr);
				break;

			case PLPGSQL_STMT_RETURN_NEXT:
				check_expr(cstate, ((PLpgSQL_stmt_return_next *) stmt)->expr);
				break;

			case PLPGSQL_STMT_RETURN_QUERY:
				{
					PLpgSQL_stmt_return_query *stmt_rq = (PLpgSQL_stmt_return_query *) stmt;

					check_expr(cstate, stmt_rq->dynquery);

					check_expr(cstate, stmt_rq->query);

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

					foreach(l, stmt_raise->params)
					{
						check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}

					foreach(l, stmt_raise->options)
					{
						PLpgSQL_raise_option *opt = (PLpgSQL_raise_option *) lfirst(l);

						check_expr(cstate, opt->expr);
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
				}
				break;

			case PLPGSQL_STMT_EXECSQL:
				{
					PLpgSQL_stmt_execsql *stmt_execsql = (PLpgSQL_stmt_execsql *) stmt;

					if (stmt_execsql->into)
					{
						check_row_or_rec(cstate, stmt_execsql->row, stmt_execsql->rec);
						check_assignment(cstate, stmt_execsql->sqlstmt,
								   stmt_execsql->rec, stmt_execsql->row, -1);
					}
					else
					{
						/* only statement */
						check_expr(cstate, stmt_execsql->sqlstmt);
					}
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
						check_row_or_rec(cstate, stmt_dynexecute->row, stmt_dynexecute->rec);

						if (stmt_dynexecute->rec != NULL)
						{
							put_error(cstate,
										  0, 0,
								"cannot determinate a result of dynamic SQL",
										  "Cannot to contine in check.",
						  "Don't use dynamic SQL and record type together, when you would check function.",
										  PLPGSQL_CHECK_WARNING_OTHERS,
										  0, NULL, NULL);

							/*
							 * don't continue in checking. Behave should be
							 * indeterministic.
							 */
							break;
						}
					}
				}
				break;

			case PLPGSQL_STMT_OPEN:
				{
					PLpgSQL_stmt_open *stmt_open = (PLpgSQL_stmt_open *) stmt;
					PLpgSQL_var *var = (PLpgSQL_var *) func->datums[stmt_open->curvar];

					if (var->cursor_explicit_expr)
						check_expr(cstate, var->cursor_explicit_expr);

					check_expr(cstate, stmt_open->query);
					check_expr(cstate, stmt_open->argquery);
					check_expr(cstate, stmt_open->dynquery);
					foreach(l, stmt_open->params)
					{
						check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}
				}
				break;

			case PLPGSQL_STMT_GETDIAG:
				{
					PLpgSQL_stmt_getdiag *stmt_getdiag = (PLpgSQL_stmt_getdiag *) stmt;
					ListCell   *lc;

					foreach(lc, stmt_getdiag->diag_items)
					{
						PLpgSQL_diag_item *diag_item = (PLpgSQL_diag_item *) lfirst(lc);

						check_target(cstate, diag_item->target);
					}
				}
				break;

			case PLPGSQL_STMT_FETCH:
				{
					PLpgSQL_stmt_fetch *stmt_fetch = (PLpgSQL_stmt_fetch *) stmt;
					PLpgSQL_var *var = (PLpgSQL_var *) (cstate->estate->datums[stmt_fetch->curvar]);

					check_row_or_rec(cstate, stmt_fetch->row, stmt_fetch->rec);

					if (var != NULL && var->cursor_explicit_expr != NULL)
						check_assignment(cstate, var->cursor_explicit_expr,
									   stmt_fetch->rec, stmt_fetch->row, -1);
				}
				break;

			case PLPGSQL_STMT_CLOSE:
				break;

			default:
				elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
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
 * Ensure check for all statements in list
 *
 */
static void
check_stmts(PLpgSQL_checkstate *cstate, List *stmts)
{
	ListCell   *lc;

	foreach(lc, stmts)
	{
		check_stmt(cstate, (PLpgSQL_stmt *) lfirst(lc));
	}
}

/*
 * Verify a expression
 *
 */
static void
check_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	if (expr)
		check_expr_as_rvalue(cstate, expr, NULL, NULL, -1, false, false);
}

static void
record_variable_usage(PLpgSQL_checkstate *cstate, int dno)
{
	if (dno >= 0)
		cstate->used_variables = bms_add_member(cstate->used_variables, dno);
}

/*
 * Reports all unused variables explicitly DECLAREd by the user.  Ignores IN
 * and OUT variables and special variables created by PL/PgSQL.
 */
static void
report_unused_variables(PLpgSQL_checkstate *cstate)
{
	int i;
	PLpgSQL_execstate *estate = cstate->estate;

	for (i = 0; i < estate->ndatums; i++)
	{
		int dtype = estate->datums[i]->dtype;
		PLpgSQL_variable *var;

		if (dtype != PLPGSQL_DTYPE_VAR &&
			dtype != PLPGSQL_DTYPE_ROW &&
			dtype != PLPGSQL_DTYPE_REC)
			continue;

		/* skip special internal variables */
		var = (PLpgSQL_variable *) estate->datums[i];
		if (var->lineno < 1)
			continue;
		/* skip internal vars created for INTO lists  */
		if (dtype == PLPGSQL_DTYPE_ROW &&
			((PLpgSQL_row *) var)->rowtupdesc == NULL)
			continue;

		if (!bms_is_member(i, cstate->used_variables))
		{
			StringInfoData ctx;
			initStringInfo(&ctx);
			appendStringInfo(&ctx, "variable %s declared on line %d", var->refname, var->lineno);
			put_error(cstate,
					  0, 0,
					  "unused declared variable",
					  NULL,
					  NULL,
					  PLPGSQL_CHECK_WARNING_OTHERS,
					  0, NULL, ctx.data);
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

/*
 * Verify a element
 *
 */
static void
check_element_assignment(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
						 PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
						 int targetdno)
{
	bool		is_expression = (targetrec == NULL && targetrow == NULL);

	check_expr_as_rvalue(cstate, expr, targetrec, targetrow, targetdno, true,
						  is_expression);
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

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		prepare_expr(cstate, expr, 0);
		/* record all variables used by the query */
		cstate->used_variables = bms_add_members(cstate->used_variables, expr->paramnos);

		tupdesc = expr_get_desc(cstate, expr, use_element_type, true, is_expression);
		if (tupdesc)
		{
			if (targetrow != NULL || targetrec != NULL)
				assign_tupdesc_row_or_rec(cstate, targetrow, targetrec, tupdesc);
			if (targetdno != -1)
				assign_tupdesc_dno(cstate, targetdno, tupdesc);

			if (targetrow)
			{
				if (targetrow->nfields > tupdesc->natts)
					put_error(cstate,
								  0, 0,
								  "too few attributies for target variables",
								  "There are more target variables than output columns in query.",
						  "Check target variables in SELECT INTO statement.",
								  PLPGSQL_CHECK_WARNING_OTHERS,
								  0, NULL, NULL);
				else if (targetrow->nfields < tupdesc->natts)
					put_error(cstate,
								  0, 0,
								  "too many attributies for target variables",
								  "There are less target variables than output columns in query.",
						   "Check target variables in SELECT INTO statement",
								  PLPGSQL_CHECK_WARNING_OTHERS,
								  0, NULL, NULL);
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

			check_target(cstate, row->varnos[fnum]);
		}
	}
	else if (rec != NULL)
	{
		/*
		 * There are no checks done on records currently; just record that the
		 * variable is not unused.
		 */
		record_variable_usage(cstate, rec->dno);
	}
}

/*
 * Verify lvalue It doesn't repeat a checks that are done. Checks a subscript
 * expressions, verify a validity of record's fields.
 */
static void
check_target(PLpgSQL_checkstate *cstate, int varno)
{
	PLpgSQL_datum *target = cstate->estate->datums[varno];

	record_variable_usage(cstate, varno);

	switch (target->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
		case PLPGSQL_DTYPE_REC:
			/* nothing to check */
			break;

		case PLPGSQL_DTYPE_ROW:
			check_row_or_rec(cstate, (PLpgSQL_row *) target, NULL);
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
				if (!HeapTupleIsValid(rec->tup))
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("record \"%s\" is not assigned to tuple structure",
						   rec->refname)));

				/*
				 * Get the number of the records field to change and the
				 * number of attributes in the tuple.  Note: disallow system
				 * column names because the code below won't cope.
				 */
				fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
				if (fno <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("record \"%s\" has no field \"%s\"",
									rec->refname, recfield->fieldname)));
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
				arraytypeid = exec_get_datum_type(cstate->estate, target);
				arraytypeid = getBaseType(arraytypeid);

				arrayelemtypeid = get_element_type(arraytypeid);

				if (!OidIsValid(arrayelemtypeid))
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("subscripted object is not an array")));
			}
			break;
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

	if (expr->plan != NULL)
		return;					/* already checked */

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

/*
 * Assign a tuple descriptor to variable specified by dno
 */
static void
assign_tupdesc_dno(PLpgSQL_checkstate *cstate, int varno, TupleDesc tupdesc)
{
	PLpgSQL_datum *target = cstate->estate->datums[varno];

	/* check assign without IO casts */
	if (target->dtype == PLPGSQL_DTYPE_VAR)
	{
		PLpgSQL_var *var = (PLpgSQL_var *) target;

		if (type_is_rowtype(tupdesc->attrs[0]->atttypid))
			put_error(cstate,
						  ERRCODE_DATATYPE_MISMATCH, 0,
						  "cannot assign composite value to a scalar variable",
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_ERROR,
						  0, NULL, NULL);

		else if (var->datatype->typoid != tupdesc->attrs[0]->atttypid)
		{
			StringInfoData	str;

			initStringInfo(&str);
			appendStringInfo(&str, "assign \"%s\" value to \"%s\" variable",
										format_type_be(tupdesc->attrs[0]->atttypid),
										format_type_be(var->datatype->typoid));

			put_error(cstate,
						  ERRCODE_DATATYPE_MISMATCH, 0,
						  "target variable has different type then expression result",
						  str.data,
						  "Hidden casting can be a performance issue.",
						  PLPGSQL_CHECK_WARNING_PERFORMANCE,
						  0, NULL, NULL);

			pfree(str.data);
		}
	}
	else if (target->dtype == PLPGSQL_DTYPE_REC)
		assign_tupdesc_row_or_rec(cstate, NULL, (PLpgSQL_rec *) target, tupdesc);
}

/*
 * We have to assign TupleDesc to all used record variables step by step. We
 * would to use a exec routines for query preprocessing, so we must to create
 * a typed NULL value, and this value is assigned to record variable.
 */
static void
assign_tupdesc_row_or_rec(PLpgSQL_checkstate *cstate,
						  PLpgSQL_row *row, PLpgSQL_rec *rec,
						  TupleDesc tupdesc)
{
	bool	   *nulls;
	HeapTuple	tup;

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

		if (target->freetup)
			heap_freetuple(target->tup);

		if (rec->freetupdesc)
			FreeTupleDesc(target->tupdesc);

		/* initialize rec by NULLs */
		nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
		memset(nulls, true, tupdesc->natts * sizeof(bool));

		target->tupdesc = CreateTupleDescCopy(tupdesc);
		target->freetupdesc = true;

		tup = heap_form_tuple(tupdesc, NULL, nulls);
		if (HeapTupleIsValid(tup))
		{
			target->tup = tup;
			target->freetup = true;
		}
		else
			elog(ERROR, "cannot to build valid composite value");
	}
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
			  bool is_expression)
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
		elemtype = get_element_type(tupdesc->attrs[0]->atttypid);
		if (!OidIsValid(elemtype))
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
				errmsg("FOREACH expression must yield an array, not type %s",
					   format_type_be(tupdesc->attrs[0]->atttypid))));
			FreeTupleDesc(tupdesc);
		}
		/* we can't know typmod now */
		elemtupdesc = lookup_rowtype_tupdesc_noerror(elemtype, -1, true);
		if (elemtupdesc != NULL)
		{
			FreeTupleDesc(tupdesc);
			tupdesc = CreateTupleDescCopy(elemtupdesc);
			ReleaseTupleDesc(elemtupdesc);
		}
		else
			/* XXX: should be a warning? */
			ereport(ERROR,
					(errmsg("cannot to identify real type for record type variable")));
	}
	if (is_expression && tupdesc->natts != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("query \"%s\" returned %d columns",
						query->query,
						tupdesc->natts)));

	/*
	 * One spacial case is when record is assigned to composite type, then we
	 * should to unpack composite type.
	 */
	if (tupdesc->tdtypeid == RECORDOID &&
		tupdesc->tdtypmod == -1 &&
		tupdesc->natts == 1 && expand_record)
	{
		TupleDesc	unpack_tupdesc;

		unpack_tupdesc = lookup_rowtype_tupdesc_noerror(tupdesc->attrs[0]->atttypid,
												tupdesc->attrs[0]->atttypmod,
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
		tupdesc->attrs[0]->atttypid == RECORDOID &&
		tupdesc->attrs[0]->atttypmod == -1 &&
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
		cplan = GetCachedPlan(plansource, NULL, true);
		_stmt = (PlannedStmt *) linitial(cplan->stmt_list);

		if (IsA(_stmt, PlannedStmt) &&_stmt->commandType == CMD_SELECT)
		{
			_plan = _stmt->planTree;
			if (IsA(_plan, Result) &&list_length(_plan->targetlist) == 1)
			{
				tle = (TargetEntry *) linitial(_plan->targetlist);
				if (((Node *) tle->expr)->type == T_FuncExpr)
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
			}
		}
		ReleaseCachedPlan(cplan, true);
	}
	return tupdesc;
}


/*
 * returns refname of PLpgSQL_datum
 */
static char *
datum_get_refname(PLpgSQL_datum *d)
{
	switch (d->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			return ((PLpgSQL_var *) d)->refname;

		case PLPGSQL_DTYPE_ROW:
			return ((PLpgSQL_row *) d)->refname;

		case PLPGSQL_DTYPE_REC:
			return ((PLpgSQL_rec *) d)->refname;

		default:
			return NULL;
	}
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
#define SET_RESULT_OID(anum, oid)	SET_RESULT((anum), ObjectIdGetDatum((oid)))

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
	/* ignore warnings when is not requested */
	if ((level == PLPGSQL_CHECK_WARNING_PERFORMANCE && !cstate->performance_warnings) ||
			    (level == PLPGSQL_CHECK_WARNING_OTHERS && !cstate->other_warnings))
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

	if (estate != NULL && estate->err_stmt != NULL)
	{
		/* use lineno based on err_stmt */
		SET_RESULT_INT32(Anum_result_lineno, estate->err_stmt->lineno);
		SET_RESULT_TEXT(Anum_result_statement, plpgsql_stmt_typename(estate->err_stmt));
	}
	else
	{
		/* lineno is based on edata */
		SET_RESULT_NULL(Anum_result_lineno);
		SET_RESULT_NULL(Anum_result_statement);
	} 

	SET_RESULT_TEXT(Anum_result_sqlstate, unpack_sql_state(sqlerrcode));
	SET_RESULT_TEXT(Anum_result_message, message);
	SET_RESULT_TEXT(Anum_result_detail, detail);
	SET_RESULT_TEXT(Anum_result_hint, hint);

	switch (level)
	{
		case PLPGSQL_CHECK_ERROR:
			SET_RESULT_TEXT(Anum_result_level, "error");
			break;
		case PLPGSQL_CHECK_WARNING_OTHERS:
			SET_RESULT_TEXT(Anum_result_level, "warning");
			break;
		case PLPGSQL_CHECK_WARNING_PERFORMANCE:
			SET_RESULT_TEXT(Anum_result_level, "performance");
			break;
	}

	SET_RESULT_INT32(Anum_result_position, position);
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
	const char *level_str;
	bool			use_sql_lineno = false;

	Assert(message != NULL);

	initStringInfo(&sinfo);

	switch (level)
	{
		case PLPGSQL_CHECK_ERROR:
			level_str = "error";
			break;
		case PLPGSQL_CHECK_WARNING_OTHERS:
			level_str = "warning";
			break;
		case PLPGSQL_CHECK_WARNING_PERFORMANCE:
			level_str = "performance";
			break;
	}

	if (estate != NULL && estate->err_stmt != NULL)
		appendStringInfo(&sinfo, "%s:%s:%d:%s:%s",
				 level_str,
				 unpack_sql_state(sqlerrcode),
				 estate->err_stmt->lineno,
			    plpgsql_stmt_typename(estate->err_stmt),
				 message);
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
	const char *level_str;

	Assert(message != NULL);

	switch (level)
	{
		case PLPGSQL_CHECK_ERROR:
			level_str = "error";
			break;
		case PLPGSQL_CHECK_WARNING_OTHERS:
			level_str = "warning";
			break;
		case PLPGSQL_CHECK_WARNING_PERFORMANCE:
			level_str = "performance";
			break;
	}

	/* flush tag */
	appendStringInfoString(str, "  <Issue>\n");

	appendStringInfo(str, "    <Level>%s</level>\n", level_str);
	appendStringInfo(str, "    <Sqlstate>%s</Sqlstate>\n",
						 unpack_sql_state(sqlerrcode));
	appendStringInfo(str, "    <Message>%s</Message>\n",
							 escape_xml(message));
	if (estate->err_stmt != NULL)
		appendStringInfo(str, "    <Stmt lineno=\"%d\">%s</Stmt>\n",
				 estate->err_stmt->lineno,
			   plpgsql_stmt_typename(estate->err_stmt));
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
 * it is used primary for XML format - create almost left and almost right tag per function
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
}

/****************************************************************************************
 * A maintaining of hash table of checked funtcions
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

	hentry = (plpgsql_check_HashEnt *) hash_search(plpgsql_check_HashTable,
											 (void *) func->fn_hashkey,
											 HASH_ENTER,
											 &found);

	hentry->fn_xmin = func->fn_xmin;
	hentry->fn_tid = func->fn_tid;

	hentry->is_checked = true;
}
