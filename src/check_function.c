/*-------------------------------------------------------------------------
 *
 * check_function.c
 *
 *			  workhorse functionality of this extension - expression
 *			  and query validator
 *
 * by Pavel Stehule 2013-2023
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "access/heapam.h"

static HTAB *plpgsql_check_HashTable = NULL;

bool plpgsql_check_other_warnings = false;
bool plpgsql_check_extra_warnings = false;
bool plpgsql_check_performance_warnings = false;
bool plpgsql_check_compatibility_warnings = false;
bool plpgsql_check_fatal_errors = true;
bool plpgsql_check_constants_tracing = true;
int plpgsql_check_mode = PLPGSQL_CHECK_MODE_BY_FUNCTION;

/* ----------
 * Hash table for checked functions
 * ----------
 */

typedef struct plpgsql_hashent
{
	PLpgSQL_func_hashkey key;
	TransactionId fn_xmin;
	ItemPointerData fn_tid;
	bool is_checked;
} plpgsql_check_HashEnt;


static void function_check(PLpgSQL_function *func, PLpgSQL_checkstate *cstate);
static void trigger_check(PLpgSQL_function *func, Node *tdata, PLpgSQL_checkstate *cstate);
static void release_exprs(List *exprs);
static int load_configuration(HeapTuple procTuple, bool *reload_config);
static void init_datum_dno(PLpgSQL_checkstate *cstate, int dno, bool is_auto, bool is_protected);
static PLpgSQL_datum * copy_plpgsql_datum(PLpgSQL_checkstate *cstate, PLpgSQL_datum *datum);
static void setup_estate(PLpgSQL_execstate *estate, PLpgSQL_function *func, ReturnSetInfo *rsi, plpgsql_check_info *cinfo);
static void setup_cstate(PLpgSQL_checkstate *cstate, plpgsql_check_result_info *result_info,
	plpgsql_check_info *cinfo, bool is_active_mode, bool fake_rtd);

static void passive_check_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info);

static plpgsql_check_plugin2 check_plugin2 = { NULL,
											   passive_check_func_beg, NULL, NULL,
											   NULL, NULL, NULL,
											   NULL, NULL, NULL, NULL, NULL };

/*
 * Prepare list of OUT variables for later report
 */
static void
collect_out_variables(PLpgSQL_function *func, PLpgSQL_checkstate *cstate)
{
	cstate->out_variables = NULL;

	if (func->out_param_varno != -1)
	{
		int		varno = func->out_param_varno;
		PLpgSQL_variable *var = (PLpgSQL_variable *) func->datums[varno];

		if (var->dtype == PLPGSQL_DTYPE_ROW && is_internal_variable(cstate, var))
		{
			/* this function has more OUT parameters */
			PLpgSQL_row *row = (PLpgSQL_row*) var;
			int		fnum;

			for (fnum = 0; fnum < row->nfields; fnum++)
				cstate->out_variables = bms_add_member(cstate->out_variables, row->varnos[fnum]);
		}
		else
			cstate->out_variables = bms_add_member(cstate->out_variables, varno);
	}
}

/*
 * Returns true, when routine should be closed by RETURN statement
 *
 */
static bool
return_is_required(plpgsql_check_info *cinfo)
{
	if (cinfo->is_procedure)
		return false;

	if (cinfo->rettype == VOIDOID)
		return false;

	return true;
}

/*
 * own implementation - active mode
 *
 */
void
plpgsql_check_function_internal(plpgsql_check_result_info *ri,
								plpgsql_check_info *cinfo)
{
	PLpgSQL_checkstate cstate;
	PLpgSQL_function *volatile function = NULL;
	bool		reload_config;
	LOCAL_FCINFO(fake_fcinfo, 0);

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
	bool		fake_rtd;

	/*
	 * Connect to SPI manager
	 */
	if ((rc = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));

	plpgsql_check_setup_fcinfo(cinfo,
							   &flinfo,
							   fake_fcinfo,
							   &rsinfo,
							   &trigdata,
							   &etrigdata,
								&tg_trigger,
								&fake_rtd);

	setup_cstate(&cstate, ri, cinfo, true, fake_rtd);

	old_cxt = MemoryContextSwitchTo(cstate.check_cxt);

	/*
	 * Copy argument names for later check, only when other warnings are required.
	 * Argument names are used for check parameter versus local variable collision.
	 */
	if (cinfo->other_warnings)
	{
		int		numargs;
		Oid		   *argtypes;
		char	  **argnames;
		char	   *argmodes;

		numargs = get_func_arg_info(cinfo->proctuple,
							&argtypes, &argnames, &argmodes);

		if (argnames != NULL)
		{
			int			i;

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
		int			save_nestlevel = 0;

		BeginInternalSubTransaction(NULL);
		MemoryContextSwitchTo(cstate.check_cxt);

		save_nestlevel = load_configuration(cinfo->proctuple, &reload_config);

		/* have to wait for this decision to loaded configuration */
		if (plpgsql_check_mode != PLPGSQL_CHECK_MODE_DISABLED)
		{
			/* Get a compiled function */
			function = plpgsql_check__compile_p(fake_fcinfo, false);

			collect_out_variables(function, &cstate);

			/* Must save and restore prior value of cur_estate */
			cur_estate = function->cur_estate;

			/* recheck trigtype */

			Assert(function->fn_is_trigger == cinfo->trigtype);

			setup_estate(&estate, function, (ReturnSetInfo *) fake_fcinfo->resultinfo, cinfo);
			cstate.estate = &estate;

			/*
			 * Mark the function as busy, ensure higher than zero usage. There is no
			 * reason for protection function against delete, but I afraid of asserts.
			 */
			function->use_count++;

			/* Create a fake runtime environment and process check */
			switch (cinfo->trigtype)
			{
				case PLPGSQL_DML_TRIGGER:
					trigger_check(function, (Node *) &trigdata, &cstate);
					break;

				case PLPGSQL_EVENT_TRIGGER:
					trigger_check(function, (Node *) &etrigdata, &cstate);
					break;

				case PLPGSQL_NOT_TRIGGER:
					function_check(function, &cstate);
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

		if (OidIsValid(cinfo->relid))
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

		if (OidIsValid(cinfo->relid))
			relation_close(trigdata.tg_relation, AccessShareLock);

		if (function)
		{
			function->cur_estate = cur_estate;
			function->use_count--;
			release_exprs(cstate.exprs);
		}

		plpgsql_check_put_error_edata(&cstate, edata);

		/* reconnect spi */
		SPI_restore_connection();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(old_cxt);
	MemoryContextDelete(cstate.check_cxt);

	/*
	 * Disconnect from SPI manager
	 */
	if ((rc = SPI_finish()) != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));
}

/*
 * plpgsql_check_on_func_beg - passive mode
 *
 *      callback function - called by plgsql executor, when function is started
 *      and local variables are initialized.
 *
 */
static void
passive_check_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info)
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
		plpgsql_check_result_info ri;
		plpgsql_check_info cinfo;
		PLpgSQL_checkstate cstate;

		/*
		 * don't allow repeated execution on checked function
		 * when it is not requsted. 
		 */
		if (plpgsql_check_mode == PLPGSQL_CHECK_MODE_FRESH_START &&
			plpgsql_check_is_checked(func))
			return;

		plpgsql_check_mark_as_checked(func);

		memset(&ri, 0, sizeof(ri));

		plpgsql_check_info_init(&cinfo, func->fn_oid);

		if (OidIsValid(func->fn_oid))
		{
			cinfo.proctuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->fn_oid));
			if (!HeapTupleIsValid(cinfo.proctuple))
				elog(ERROR, "cache lookup failed for function %u", func->fn_oid);

			plpgsql_check_get_function_info(&cinfo);

			ReleaseSysCache(cinfo.proctuple);
			cinfo.proctuple = NULL;

			cinfo.fn_oid = func->fn_oid;
		}
		else
			cinfo.volatility = PROVOLATILE_VOLATILE;

		cinfo.fatal_errors = plpgsql_check_fatal_errors,
		cinfo.other_warnings = plpgsql_check_other_warnings,
		cinfo.performance_warnings = plpgsql_check_performance_warnings,
		cinfo.extra_warnings = plpgsql_check_extra_warnings,
		cinfo.compatibility_warnings = plpgsql_check_compatibility_warnings;
		cinfo.constants_tracing = plpgsql_check_constants_tracing;

		ri.format = PLPGSQL_CHECK_FORMAT_ELOG;

		setup_cstate(&cstate, &ri, &cinfo, false, false);

		collect_out_variables(func, &cstate);

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

				memcpy(&saved_records[i], rec, sizeof(PLpgSQL_rec));

				if (rec->erh)
				{
					/* work with dummy copy */
					rec->erh = make_expanded_record_from_exprecord(rec->erh, cstate.check_cxt);
				}

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
			plpgsql_check_stmt(&cstate, (PLpgSQL_stmt *) func->action, &closing, &exceptions);

			estate->err_stmt = NULL;

			if (!cstate.stop_check)
			{
				if (closing != PLPGSQL_CHECK_CLOSED && closing != PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS &&
					return_is_required(cstate.cinfo))
					plpgsql_check_put_error(&cstate,
									  ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT, 0,
									  "control reached end of function without RETURN",
									  NULL,
									  NULL,
									  closing == PLPGSQL_CHECK_UNCLOSED ?
											PLPGSQL_CHECK_ERROR : PLPGSQL_CHECK_WARNING_EXTRA,
									  0, NULL, NULL);

				plpgsql_check_report_unused_variables(&cstate);
				plpgsql_check_report_too_high_volatility(&cstate);
			}
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

				memcpy(rec, &saved_records[i], sizeof(PLpgSQL_rec));
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

void
plpgsql_check_passive_check_init(void)
{
	plpgsql_check_register_pldbgapi2_plugin(&check_plugin2);
}

/*
 * Check function - it prepare variables and starts a prepare plan walker
 *
 */
static void
function_check(PLpgSQL_function *func, PLpgSQL_checkstate *cstate)
{
	int			i;
	int closing = PLPGSQL_CHECK_UNCLOSED;
	List	   *exceptions;
	ListCell   *lc;

	/*
	 * Make local execution copies of all the datums
	 */
	for (i = 0; i < cstate->estate->ndatums; i++)
		cstate->estate->datums[i] = copy_plpgsql_datum(cstate, func->datums[i]);

	init_datum_dno(cstate, cstate->estate->found_varno, true, true);

	/*
	 * check function's parameters to not be reserved keywords
	 */
	foreach(lc, cstate->argnames)
	{
		char	   *argname = (char *) lfirst(lc);

		if (plpgsql_check_is_reserved_keyword(argname))
		{
			StringInfoData str;

			initStringInfo(&str);
			appendStringInfo(&str, "name of parameter \"%s\" is reserved keyword",
						 argname);

			plpgsql_check_put_error(cstate,
						  0, 0,
						  str.data,
						  "The reserved keyword was used as parameter name.",
						  NULL,
						  PLPGSQL_CHECK_WARNING_OTHERS,
						  0, NULL, NULL);
			pfree(str.data);
		}
	}

	/*
	 * Store the actual call argument values (fake) into the appropriate
	 * variables
	 */
	for (i = 0; i < func->fn_nargs; i++)
	{
		init_datum_dno(cstate, func->fn_argvarnos[i], false, false);
	}

	/*
	 * Now check the toplevel block of statements
	 */
	plpgsql_check_stmt(cstate, (PLpgSQL_stmt *) func->action, &closing, &exceptions);

	/* clean state values - next errors are not related to any command */
	cstate->estate->err_stmt = NULL;

	if (!cstate->stop_check)
	{
		if (closing != PLPGSQL_CHECK_CLOSED && closing != PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS &&
			return_is_required(cstate->cinfo))
			plpgsql_check_put_error(cstate,
							  ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT, 0,
							  "control reached end of function without RETURN",
							  NULL,
							  NULL,
							  closing == PLPGSQL_CHECK_UNCLOSED ? PLPGSQL_CHECK_ERROR : PLPGSQL_CHECK_WARNING_EXTRA,
							  0, NULL, NULL);

		plpgsql_check_report_unused_variables(cstate);
		plpgsql_check_report_too_high_volatility(cstate);
	}
}

/*
 * Check trigger - prepare fake environments for testing trigger
 *
 */
static void
trigger_check(PLpgSQL_function *func, Node *tdata, PLpgSQL_checkstate *cstate)
{
	int			i;
	int closing = PLPGSQL_CHECK_UNCLOSED;
	List	   *exceptions;

	/*
	 * Make local execution copies of all the datums
	 */
	for (i = 0; i < cstate->estate->ndatums; i++)
		cstate->estate->datums[i] = copy_plpgsql_datum(cstate, func->datums[i]);

	init_datum_dno(cstate, cstate->estate->found_varno, true, true);

	if (IsA(tdata, TriggerData))
	{
		PLpgSQL_rec *rec_new,
				   *rec_old;

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

		/*
		 * find all PROMISE VARIABLES and initit their
		 */
		for (i = 0; i < func->ndatums; i++)
		{
			PLpgSQL_datum *datum = func->datums[i];

			if (datum->dtype == PLPGSQL_DTYPE_PROMISE)
				init_datum_dno(cstate, datum->dno, true, datum->dno != func->new_varno && datum->dno != func->old_varno);
		}

		rec_new = (PLpgSQL_rec *) (cstate->estate->datums[func->new_varno]);
		plpgsql_check_recval_assign_tupdesc(cstate, rec_new, trigdata->tg_relation->rd_att, false);
		rec_old = (PLpgSQL_rec *) (cstate->estate->datums[func->old_varno]);
		plpgsql_check_recval_assign_tupdesc(cstate, rec_old, trigdata->tg_relation->rd_att, false);
	}
	else if (IsA(tdata, EventTriggerData))
	{

		/* do nothing */
	}
	else
		elog(ERROR, "unexpected environment");

	/*
	 * Now check the toplevel block of statements
	 */
	plpgsql_check_stmt(cstate, (PLpgSQL_stmt *) func->action, &closing, &exceptions);

	/* clean state values - next errors are not related to any command */
	cstate->estate->err_stmt = NULL;

	if (!cstate->stop_check)
	{
		if (closing != PLPGSQL_CHECK_CLOSED && closing != PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS &&
			return_is_required(cstate->cinfo))
			plpgsql_check_put_error(cstate,
							  ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT, 0,
							  "control reached end of function without RETURN",
							  NULL,
							  NULL,
							  closing == PLPGSQL_CHECK_UNCLOSED ? PLPGSQL_CHECK_ERROR : PLPGSQL_CHECK_WARNING_EXTRA,
							  0, NULL, NULL);

		plpgsql_check_report_unused_variables(cstate);
		plpgsql_check_report_too_high_volatility(cstate);
	}
}

/*
 * Returns true when some fields is polymorphics
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
 * Replaces polymorphic types by real type
 */
static Oid
replace_polymorphic_type(plpgsql_check_info *cinfo,
								Oid typ,
								Oid anyelement_array_oid,
								bool is_array_anyelement,
								Oid anycompatible_array_oid,
								bool is_array_anycompatible,
								bool is_variadic)
{
	/* quite compiler warnings */
	(void) anycompatible_array_oid;
	(void) is_array_anycompatible;

	if (OidIsValid(typ) && IsPolymorphicType(typ))
	{
		switch (typ)
		{
			case ANYELEMENTOID:
				typ = is_variadic ? anyelement_array_oid : cinfo->anyelementoid;
				break;

			case ANYNONARRAYOID:
				if (is_array_anyelement)
					elog(ERROR, "anyelement type is a array (expected nonarray)");
				typ = is_variadic ? anyelement_array_oid : cinfo->anyelementoid;
				break;

			case ANYENUMOID:	/* XXX dubious */
				if (!OidIsValid(cinfo->anyenumoid))
					elog(ERROR, "anyenumtype option should be specified (anyenum type is used)");
				if (!type_is_enum(cinfo->anyenumoid))
					elog(ERROR, "type specified by anyenumtype option is not enum");
				typ = cinfo->anyenumoid;
			break;

			case ANYARRAYOID:
				typ = anyelement_array_oid;
				break;

			case ANYRANGEOID:
				typ = is_variadic ? get_array_type(cinfo->anyrangeoid) : cinfo->anyrangeoid;
				break;

#if PG_VERSION_NUM >= 130000

			case ANYCOMPATIBLEOID:
				typ = is_variadic ? anycompatible_array_oid : cinfo->anycompatibleoid;
				break;

			case ANYCOMPATIBLENONARRAYOID:
				if (is_array_anycompatible)
					elog(ERROR, "anycompatible type is a array (expected nonarray)");
				typ = is_variadic ? anycompatible_array_oid : cinfo->anycompatibleoid;
				break;

			case ANYCOMPATIBLEARRAYOID:
				typ = anycompatible_array_oid;
				break;

			case ANYCOMPATIBLERANGEOID:
				typ = is_variadic ? get_array_type(cinfo->anycompatiblerangeoid) : cinfo->anycompatiblerangeoid;
				break;

#endif

			default:
				/* fallback */
				typ = is_variadic ? INT4ARRAYOID : INT4OID;
		}
	}

	return typ;
}

/*
 * Set up a fake fcinfo with just enough info to satisfy plpgsql_compile().
 *
 * There should be a different real argtypes for polymorphic params.
 *
 * When output fake_rtd is true, then we should to not compare result fields,
 * because we know nothing about expected result.
 */
void
plpgsql_check_setup_fcinfo(plpgsql_check_info *cinfo,
						  FmgrInfo *flinfo,
						  FunctionCallInfo fcinfo,
						  ReturnSetInfo *rsinfo,
						  TriggerData *trigdata,
						  EventTriggerData *etrigdata,
						  Trigger *tg_trigger,
						  bool *fake_rtd)
{
	TupleDesc resultTupleDesc;
	int		nargs;
	Oid	   *argtypes;
	char  **argnames;
	char   *argmodes;
	Oid		rettype;
	bool	found_polymorphic = false;

	*fake_rtd = false;

	/* clean structures */
	MemSet(fcinfo, 0, SizeForFunctionCallInfo(0));

	MemSet(flinfo, 0, sizeof(FmgrInfo));
	MemSet(rsinfo, 0, sizeof(ReturnSetInfo));

	fcinfo->flinfo = flinfo;
	flinfo->fn_oid = cinfo->fn_oid;
	flinfo->fn_mcxt = CurrentMemoryContext;

	rettype = cinfo->rettype;

	if (cinfo->trigtype == PLPGSQL_DML_TRIGGER)
	{
		Assert(trigdata != NULL);

		MemSet(trigdata, 0, sizeof(TriggerData));
		MemSet(tg_trigger, 0, sizeof(Trigger));

		trigdata->type = T_TriggerData;
		trigdata->tg_trigger = tg_trigger;

		fcinfo->context = (Node *) trigdata;

		if (OidIsValid(cinfo->relid))
			trigdata->tg_relation = relation_open(cinfo->relid, AccessShareLock);
	}
	else if (cinfo->trigtype == PLPGSQL_EVENT_TRIGGER)
	{
		MemSet(etrigdata, 0, sizeof(etrigdata));
		etrigdata->type = T_EventTriggerData;
		fcinfo->context = (Node *) etrigdata;
	}

	/* prepare call expression - used for polymorphic arguments */
	nargs = get_func_arg_info(cinfo->proctuple,
							  &argtypes,
							  &argnames,
							  &argmodes);

	if (nargs > 0)
	{
		int		i;

		for (i = 0; i < nargs; i++)
		{
			Oid		argtype = InvalidOid;

			if (argmodes)
			{
				if (argmodes[i] == FUNC_PARAM_IN ||
					argmodes[i] == FUNC_PARAM_INOUT ||
					argmodes[i] == FUNC_PARAM_VARIADIC)
				argtype = argtypes[i];
			}
			else
				argtype = argtypes[i];

			if (OidIsValid(argtype) && IsPolymorphicType(argtype))
			{
				found_polymorphic = true;
				break;
			}
		}

		if (found_polymorphic)
		{
			List	   *args = NIL;
			Oid			anyelement_array_oid;
			Oid			anyelement_base_oid;
			bool		is_array_anyelement;
			Oid			anycompatible_array_oid;
			Oid			anycompatible_base_oid;
			bool		is_array_anycompatible;

			anyelement_array_oid = get_array_type(cinfo->anyelementoid);
			anyelement_base_oid = getBaseType(cinfo->anyelementoid);
			is_array_anyelement = OidIsValid(get_element_type(anyelement_base_oid));

#if PG_VERSION_NUM >= 130000

			anycompatible_array_oid = get_array_type(cinfo->anycompatibleoid);
			anycompatible_base_oid = getBaseType(cinfo->anycompatibleoid);
			is_array_anycompatible = OidIsValid(get_element_type(anycompatible_base_oid));

#else

			anycompatible_array_oid = InvalidOid;
			anycompatible_base_oid = InvalidOid;
			is_array_anycompatible = false;

			(void) anycompatible_base_oid;

#endif

			/*
			 * when polymorphic types are used, then we need to build fake fn_expr,
			 * to be in plpgsql_resolve_polymorphic_argtypes happy.
			 */
			for (i = 0; i < nargs; i++)
			{
				bool	is_variadic = false;
				Oid		argtype = InvalidOid;

				if (argmodes)
				{
					if (argmodes[i] == FUNC_PARAM_IN ||
						argmodes[i] == FUNC_PARAM_INOUT ||
						argmodes[i] == FUNC_PARAM_VARIADIC)
					{
						argtype = argtypes[i];
						if (argmodes[i] == FUNC_PARAM_VARIADIC)
							is_variadic = true;
					}
				}
				else
					argtype = argtypes[i];

				if (OidIsValid(argtype))
				{
					argtype = replace_polymorphic_type(cinfo,
													   argtype,
													   anyelement_array_oid,
													   is_array_anyelement,
													   anycompatible_array_oid,
													   is_array_anycompatible,
													   is_variadic);

					args = lappend(args,
								   makeNullConst(argtype, -1, InvalidOid));
				}
			}

			rettype =  replace_polymorphic_type(cinfo,
												rettype,
												anyelement_array_oid,
												is_array_anyelement,
												anycompatible_array_oid,
												is_array_anycompatible,
												false);

			fcinfo->flinfo->fn_expr = (Node *) makeFuncExpr(cinfo->fn_oid,
															rettype,
															args,
															InvalidOid,
															InvalidOid,
															COERCE_EXPLICIT_CALL);
		}
	}

	if (argtypes)
		pfree(argtypes);
	if (argnames)
		pfree(argnames);
	if (argmodes)
		pfree(argmodes);

	/* 
	 * prepare ReturnSetInfo
	 *
	 * necessary for RETURN NEXT and RETURN QUERY
	 *
	 */
	resultTupleDesc = build_function_result_tupdesc_t(cinfo->proctuple);
	if (resultTupleDesc)
	{
		/* we cannot to solve polymorphic params now */
		if (is_polymorphic_tupdesc(resultTupleDesc))
		{
			FreeTupleDesc(resultTupleDesc);
			resultTupleDesc = NULL;
		}
	}
	else if (cinfo->rettype == TRIGGEROID

#if PG_VERSION_NUM < 130000

			|| cinfo->rettype == OPAQUEOID

#endif

			)
	{
		/* trigger - return value should be ROW or RECORD based on relid */
		if (trigdata->tg_relation)
			resultTupleDesc = CreateTupleDescCopy(trigdata->tg_relation->rd_att);
	}
	else if (!IsPolymorphicType(cinfo->rettype))
	{
		if (get_typtype(cinfo->rettype) == TYPTYPE_COMPOSITE)
			resultTupleDesc = lookup_rowtype_tupdesc_copy(cinfo->rettype, -1);
		else
		{
			*fake_rtd = cinfo->rettype == RECORDOID;

			resultTupleDesc = CreateTemplateTupleDesc(1);

			TupleDescInitEntry(resultTupleDesc,
							    (AttrNumber) 1, "__result__",
							    cinfo->rettype, -1, 0);
			resultTupleDesc = BlessTupleDesc(resultTupleDesc);
		}
	}
	else
	{
		if (IsPolymorphicType(cinfo->rettype))
		{
			/*
			 * ensure replacament of polymorphic rettype, but this
			 * error is checked in validation stage, so this case
			 * should not be possible.
			 */
			if (IsPolymorphicType(rettype))
				elog(ERROR, "return type is still polymorphic");
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

/* ----------
 * Initialize a plpgsql fake execution state
 * ----------
 */
static void
setup_estate(PLpgSQL_execstate *estate,
					 PLpgSQL_function *func,
					 ReturnSetInfo *rsi,
					 plpgsql_check_info *cinfo)
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

	estate->eval_econtext = makeNode(ExprContext);
	estate->eval_econtext->ecxt_per_tuple_memory = AllocSetContextCreate(CurrentMemoryContext,
													"ExprContext",
													ALLOCSET_DEFAULT_SIZES);
	estate->datum_context = CurrentMemoryContext;

	estate->exitlabel = NULL;
	estate->cur_error = NULL;

	estate->tuple_store = NULL;
	if (rsi)
	{
		estate->tuple_store_cxt = rsi->econtext->ecxt_per_query_memory;
		estate->tuple_store_owner = CurrentResourceOwner;

		estate->tuple_store_desc = rsi->expectedDesc;
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

	if (cinfo->oldtable)
	{
		EphemeralNamedRelation enr = palloc(sizeof(EphemeralNamedRelationData));
		int rc PG_USED_FOR_ASSERTS_ONLY;

		enr->md.name = cinfo->oldtable;
		enr->md.reliddesc = cinfo->relid;
		enr->md.tupdesc = NULL;
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = 0;
		enr->reldata = NULL;

		rc = SPI_register_relation(enr);
		Assert(rc >= 0);
	}

	if (cinfo->newtable)
	{
		EphemeralNamedRelation enr = palloc(sizeof(EphemeralNamedRelationData));
		int rc PG_USED_FOR_ASSERTS_ONLY;

		enr->md.name = cinfo->newtable;
		enr->md.reliddesc = cinfo->relid;
		enr->md.tupdesc = NULL;
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = 0;
		enr->reldata = NULL;

		rc = SPI_register_relation(enr);
		Assert(rc >= 0);
	}

	estate->err_stmt = NULL;
	estate->err_text = NULL;

	estate->plugin_info = NULL;
}

/*
 * prepare PLpgSQL_checkstate structure
 *
 */
static void
setup_cstate(PLpgSQL_checkstate *cstate,
			 plpgsql_check_result_info *result_info,
			 plpgsql_check_info *cinfo,
			 bool is_active_mode,
			 bool fake_rtd)
{
	cstate->ci_magic = CI_MAGIC;

	cstate->decl_volatility = cinfo->volatility;
	cstate->has_execute_stmt = false;
	cstate->volatility = PROVOLATILE_IMMUTABLE;
	cstate->skip_volatility_check = (cinfo->rettype == TRIGGEROID ||

#if PG_VERSION_NUM < 130000

									 cinfo->rettype == OPAQUEOID ||

#endif

									 plpgsql_check_is_eventtriggeroid(cinfo->rettype) ||
									 cinfo->is_procedure);
	cstate->estate = NULL;
	cstate->result_info = result_info;
	cstate->cinfo = cinfo;
	cstate->argnames = NIL;
	cstate->exprs = NIL;
	cstate->used_variables = NULL;
	cstate->modif_variables = NULL;
	cstate->out_variables = NULL;
	cstate->top_stmt_stack = NULL;

	cstate->is_active_mode = is_active_mode;

	cstate->func_oids = NULL;
	cstate->rel_oids = NULL;

	cstate->check_cxt = AllocSetContextCreate(CurrentMemoryContext,
										 "plpgsql_check temporary cxt",
										   ALLOCSET_DEFAULT_SIZES);

	cstate->found_return_query = false;
	cstate->found_return_dyn_query = false;

	cstate->fake_rtd = fake_rtd;

	cstate->safe_variables = NULL;
	cstate->protected_variables = NULL;
	cstate->auto_variables = NULL;
	cstate->typed_variables = NULL;

	cstate->stop_check = false;
	cstate->allow_mp = false;

	cstate->pragma_vector.disable_check = false;
	cstate->pragma_vector.disable_other_warnings = false;
	cstate->pragma_vector.disable_performance_warnings = false;
	cstate->pragma_vector.disable_extra_warnings = false;
	cstate->pragma_vector.disable_security_warnings = false;
	cstate->pragma_vector.disable_compatibility_warnings = false;
	cstate->pragma_vector.disable_constants_tracing = false;

	/* try to find oid of plpgsql_check pragma function */
	cstate->pragma_foid = plpgsql_check_pragma_func_oid();

	/* for simple string constants tracing */
	cstate->strconstvars = NULL;
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

/*
 * Initialize plpgsql datum to NULL. This routine is used only for function
 * and trigger parameters so it should not support all dtypes.
 *
 */
static void
init_datum_dno(PLpgSQL_checkstate *cstate, int dno, bool is_auto, bool is_protected)
{
	switch (cstate->estate->datums[dno]->dtype)
	{
		case PLPGSQL_DTYPE_PROMISE:
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) cstate->estate->datums[dno];

				var->value = (Datum) 0;
				var->isnull = true;
				var->freeval = false;
			}
			break;

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) cstate->estate->datums[dno];

				plpgsql_check_recval_init(rec);
				plpgsql_check_recval_assign_tupdesc(cstate, rec, NULL, false);
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

					init_datum_dno(cstate, row->varnos[fnum], is_auto, is_protected);
				}
			}
			break;

		default:
			elog(ERROR, "unexpected dtype: %d", cstate->estate->datums[dno]->dtype);
	}

	if (is_protected)
		cstate->protected_variables = bms_add_member(cstate->protected_variables, dno);
	if (is_auto)
		cstate->auto_variables = bms_add_member(cstate->auto_variables, dno);
}

/*
 * initializing local execution variables
 *
 */
static PLpgSQL_datum *
copy_plpgsql_datum(PLpgSQL_checkstate *cstate, PLpgSQL_datum *datum)
{
	PLpgSQL_datum *result;

	switch (datum->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
		case PLPGSQL_DTYPE_PROMISE:
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
				plpgsql_check_recval_init(new);
				plpgsql_check_recval_assign_tupdesc(cstate, new, NULL, false);

				result = (PLpgSQL_datum *) new;
			}
			break;

		case PLPGSQL_DTYPE_ROW:
		case PLPGSQL_DTYPE_RECFIELD:

#if PG_VERSION_NUM < 140000

		case PLPGSQL_DTYPE_ARRAYELEM:

#endif

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

void
plpgsql_check_HashTableInit(void)
{
	HASHCTL		ctl;

	/* don't allow double-initialization */
	Assert(plpgsql_check_HashTable == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(PLpgSQL_func_hashkey);
	ctl.entrysize = sizeof(plpgsql_check_HashEnt);
	plpgsql_check_HashTable = hash_create("plpgsql_check function cache",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_BLOBS);
}

/*
 * Returns true, when function is marked as checked already
 *
 */
bool
plpgsql_check_is_checked(PLpgSQL_function *func)
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

/*
 * Protect function agains repeated checking
 *
 */
void
plpgsql_check_mark_as_checked(PLpgSQL_function *func)
{
	/* don't try to mark anonymous code blocks */
	if (func->fn_oid != InvalidOid)
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
}
