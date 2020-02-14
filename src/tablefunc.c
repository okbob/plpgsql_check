/*-------------------------------------------------------------------------
 *
 * tablefunc.c
 *
 *			  top functions - display results in table format
 *
 * by Pavel Stehule 2013-2019
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "utils/builtins.h"
#include "utils/syscache.h"

static void SetReturningFunctionCheck(ReturnSetInfo *rsinfo);

PG_FUNCTION_INFO_V1(plpgsql_check_function);
PG_FUNCTION_INFO_V1(plpgsql_check_function_tb);
PG_FUNCTION_INFO_V1(plpgsql_show_dependency_tb);
PG_FUNCTION_INFO_V1(plpgsql_profiler_function_tb);
PG_FUNCTION_INFO_V1(plpgsql_profiler_function_statements_tb);
PG_FUNCTION_INFO_V1(plpgsql_check_function_name);
PG_FUNCTION_INFO_V1(plpgsql_check_function_tb_name);
PG_FUNCTION_INFO_V1(plpgsql_show_dependency_tb_name);
PG_FUNCTION_INFO_V1(plpgsql_profiler_function_tb_name);
PG_FUNCTION_INFO_V1(plpgsql_profiler_function_statements_tb_name);

/*
 * Validate function result description
 *
 */
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

void
plpgsql_check_info_init(plpgsql_check_info *cinfo, Oid fn_oid)
{
	memset(cinfo, 0, sizeof(*cinfo));

	cinfo->fn_oid = fn_oid;
}

/*
 * plpgsql_check_function
 *
 * Extended check with formatted text output
 *
 */
static Datum
check_function_internal(Oid fnoid, FunctionCallInfo fcinfo)
{
	plpgsql_check_info		cinfo;
	plpgsql_check_result_info ri;
	ReturnSetInfo *rsinfo;
	ErrorContextCallback *prev_errorcontext;
	int	format;

	if (PG_NARGS() != 10)
		elog(ERROR, "unexpected number of parameters, you should to update extension");

	/* check to see if caller supports us returning a tuplestore */
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	SetReturningFunctionCheck(rsinfo);

	if (PG_ARGISNULL(1))
		elog(ERROR, "the second argument should not be null");
	if (PG_ARGISNULL(2))
		elog(ERROR, "the third argument should not be null");
	if (PG_ARGISNULL(3))
		elog(ERROR, "the fourth argument should not be null");
	if (PG_ARGISNULL(4))
		elog(ERROR, "the fifth argument should not be null");
	if (PG_ARGISNULL(5))
		elog(ERROR, "the sixth argument should not be null");
	if (PG_ARGISNULL(6))
		elog(ERROR, "the seventh argument should not be null");
	if (PG_ARGISNULL(7))
		elog(ERROR, "the eighth argument should not be null");

	format = plpgsql_check_format_num(text_to_cstring(PG_GETARG_TEXT_PP(2)));

	plpgsql_check_info_init(&cinfo, fnoid);

	cinfo.relid = PG_GETARG_OID(1);
	cinfo.fatal_errors = PG_GETARG_BOOL(3);
	cinfo.other_warnings = PG_GETARG_BOOL(4);
	cinfo.performance_warnings = PG_GETARG_BOOL(5);
	cinfo.extra_warnings = PG_GETARG_BOOL(6);
	cinfo.security_warnings = PG_GETARG_BOOL(7);

	if (PG_ARGISNULL(8))
		cinfo.oldtable = NULL;
	else
		cinfo.oldtable = NameStr(*(PG_GETARG_NAME(8)));

	if (PG_ARGISNULL(9))
		cinfo.newtable = NULL;
	else
		cinfo.newtable = NameStr(*(PG_GETARG_NAME(9)));

	if ((cinfo.oldtable || cinfo.newtable) && !OidIsValid(cinfo.relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing description of oldtable or newtable"),
				 errhint("Parameter relid is a empty.")));

	cinfo.proctuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(cinfo.fn_oid));
	if (!HeapTupleIsValid(cinfo.proctuple))
		elog(ERROR, "cache lookup failed for function %u", cinfo.fn_oid);

	plpgsql_check_get_function_info(cinfo.proctuple,
									&cinfo.rettype,
									&cinfo.volatility,
									&cinfo.trigtype,
									&cinfo.is_procedure);

	plpgsql_check_precheck_conditions(&cinfo);

	/* Envelope outer plpgsql function is not interesting */
	prev_errorcontext = error_context_stack;
	error_context_stack = NULL;

	plpgsql_check_init_ri(&ri, format, rsinfo);

	plpgsql_check_function_internal(&ri, &cinfo);

	plpgsql_check_finalize_ri(&ri);

	error_context_stack = prev_errorcontext;

	ReleaseSysCache(cinfo.proctuple);

	return (Datum) 0;
}

/*
 * plpgsql_check_function_tb
 *
 * It ensure a detailed validation and returns result as multicolumn table
 *
 */
static Datum
check_function_tb_internal(Oid fnoid, FunctionCallInfo fcinfo)
{
	plpgsql_check_info		cinfo;
	plpgsql_check_result_info ri;
	ReturnSetInfo *rsinfo;
	ErrorContextCallback *prev_errorcontext;

	if (PG_NARGS() != 9)
		elog(ERROR, "unexpected number of parameters, you should to update extension");

	/* check to see if caller supports us returning a tuplestore */
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	SetReturningFunctionCheck(rsinfo);

	if (PG_ARGISNULL(1))
		elog(ERROR, "the second argument should not be null");
	if (PG_ARGISNULL(2))
		elog(ERROR, "the third argument should not be null");
	if (PG_ARGISNULL(3))
		elog(ERROR, "the fourth argument should not be null");
	if (PG_ARGISNULL(4))
		elog(ERROR, "the fifth argument should not be null");
	if (PG_ARGISNULL(5))
		elog(ERROR, "the sixth argument should not be null");
	if (PG_ARGISNULL(6))
		elog(ERROR, "the seventh argument should not be null");

	plpgsql_check_info_init(&cinfo, fnoid);

	cinfo.relid = PG_GETARG_OID(1);
	cinfo.fatal_errors = PG_GETARG_BOOL(2);
	cinfo.other_warnings = PG_GETARG_BOOL(3);
	cinfo.performance_warnings = PG_GETARG_BOOL(4);
	cinfo.extra_warnings = PG_GETARG_BOOL(5);
	cinfo.security_warnings = PG_GETARG_BOOL(6);

	if (PG_ARGISNULL(7))
		cinfo.oldtable = NULL;
	else
		cinfo.oldtable = NameStr(*(PG_GETARG_NAME(7)));

	if (PG_ARGISNULL(8))
		cinfo.newtable = NULL;
	else
		cinfo.newtable = NameStr(*(PG_GETARG_NAME(8)));

	if ((cinfo.oldtable || cinfo.newtable) && !OidIsValid(cinfo.relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing description of oldtable or newtable"),
				 errhint("Parameter relid is a empty.")));

	cinfo.proctuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(cinfo.fn_oid));
	if (!HeapTupleIsValid(cinfo.proctuple))
		elog(ERROR, "cache lookup failed for function %u", cinfo.fn_oid);

	plpgsql_check_get_function_info(cinfo.proctuple,
									&cinfo.rettype,
									&cinfo.volatility,
									&cinfo.trigtype,
									&cinfo.is_procedure);

	plpgsql_check_precheck_conditions(&cinfo);

	/* Envelope outer plpgsql function is not interesting */
	prev_errorcontext = error_context_stack;
	error_context_stack = NULL;

	plpgsql_check_init_ri(&ri, PLPGSQL_CHECK_FORMAT_TABULAR, rsinfo);

	plpgsql_check_function_internal(&ri, &cinfo);

	plpgsql_check_finalize_ri(&ri);

	error_context_stack = prev_errorcontext;

	ReleaseSysCache(cinfo.proctuple);

	return (Datum) 0;
}

/*
 * plpgsql_show_dependency_tb
 *
 * Prepare tuplestore and start check function in mode dependency detection
 *
 */
static Datum
show_dependency_tb_internal(Oid fnoid, FunctionCallInfo fcinfo)
{
	plpgsql_check_info		cinfo;
	plpgsql_check_result_info ri;
	ReturnSetInfo *rsinfo;

	if (PG_NARGS() != 2)
		elog(ERROR, "unexpected number of parameters, you should to update extension");

	/* check to see if caller supports us returning a tuplestore */
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	SetReturningFunctionCheck(rsinfo);

	plpgsql_check_info_init(&cinfo, fnoid);

	cinfo.relid = PG_GETARG_OID(1);
	cinfo.fatal_errors = false;
	cinfo.other_warnings = false;
	cinfo.performance_warnings = false;
	cinfo.extra_warnings = false;

	cinfo.proctuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(cinfo.fn_oid));
	if (!HeapTupleIsValid(cinfo.proctuple))
		elog(ERROR, "cache lookup failed for function %u", cinfo.fn_oid);

	plpgsql_check_get_function_info(cinfo.proctuple,
									&cinfo.rettype,
									&cinfo.volatility,
									&cinfo.trigtype,
									&cinfo.is_procedure);

	plpgsql_check_precheck_conditions(&cinfo);

	plpgsql_check_init_ri(&ri, PLPGSQL_SHOW_DEPENDENCY_FORMAT_TABULAR, rsinfo);

	plpgsql_check_function_internal(&ri, &cinfo);

	plpgsql_check_finalize_ri(&ri);

	ReleaseSysCache(cinfo.proctuple);

	return (Datum) 0;
}

/*
 * Displaying a function profile
 */
static Datum
profiler_function_tb_internal(Oid fnoid, FunctionCallInfo fcinfo)
{
	plpgsql_check_info		cinfo;
	plpgsql_check_result_info ri;
	ReturnSetInfo *rsinfo;

	if (PG_NARGS() != 1)
		elog(ERROR, "unexpected number of parameters, you should to update extension");

	/* check to see if caller supports us returning a tuplestore */
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	SetReturningFunctionCheck(rsinfo);

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

	cinfo.src = plpgsql_check_get_src(cinfo.proctuple);

	plpgsql_check_init_ri(&ri, PLPGSQL_SHOW_PROFILE_TABULAR, rsinfo);

	plpgsql_check_profiler_show_profile(&ri, &cinfo);

	plpgsql_check_finalize_ri(&ri);

	pfree(cinfo.src);
	ReleaseSysCache(cinfo.proctuple);

	return (Datum) 0;
}

/*
 * Displaying a function profile
 */
static Datum
profiler_function_statements_tb_internal(Oid fnoid, FunctionCallInfo fcinfo)
{
	plpgsql_check_info		cinfo;
	plpgsql_check_result_info ri;
	ReturnSetInfo *rsinfo;

	if (PG_NARGS() != 1)
		elog(ERROR, "unexpected number of parameters, you should to update extension");

	/* check to see if caller supports us returning a tuplestore */
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	SetReturningFunctionCheck(rsinfo);

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

	plpgsql_check_init_ri(&ri, PLPGSQL_SHOW_PROFILE_STATEMENTS_TABULAR, rsinfo);

	plpgsql_check_profiler_show_profile_statements(&ri, &cinfo, NULL);

	plpgsql_check_finalize_ri(&ri);

	ReleaseSysCache(cinfo.proctuple);

	return (Datum) 0;
}

/*
 * Public functions
 */
Datum
plpgsql_check_function(PG_FUNCTION_ARGS)
{
	Oid fnoid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	fnoid = PG_GETARG_OID(0);

	return check_function_internal(fnoid, fcinfo);
}

Datum
plpgsql_check_function_tb(PG_FUNCTION_ARGS)
{
	Oid fnoid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	fnoid = PG_GETARG_OID(0);

	return check_function_tb_internal(fnoid, fcinfo);
}

Datum
plpgsql_show_dependency_tb(PG_FUNCTION_ARGS)
{
	Oid fnoid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	fnoid = PG_GETARG_OID(0);

	return show_dependency_tb_internal(fnoid, fcinfo);
}

Datum
plpgsql_profiler_function_tb(PG_FUNCTION_ARGS)
{
	Oid fnoid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	fnoid = PG_GETARG_OID(0);

	return profiler_function_tb_internal(fnoid, fcinfo);
}

Datum
plpgsql_profiler_function_statements_tb(PG_FUNCTION_ARGS)
{
	Oid fnoid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	fnoid = PG_GETARG_OID(0);

	return profiler_function_statements_tb_internal(fnoid, fcinfo);
}

Datum
plpgsql_check_function_name(PG_FUNCTION_ARGS)
{
	Oid 	fnoid;
	char   *name_or_signature;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	name_or_signature = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fnoid = plpgsql_check_parse_name_or_signature(name_or_signature);

	return check_function_internal(fnoid, fcinfo);
}

Datum
plpgsql_check_function_tb_name(PG_FUNCTION_ARGS)
{
	Oid		fnoid;
	char   *name_or_signature;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	name_or_signature = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fnoid = plpgsql_check_parse_name_or_signature(name_or_signature);

	return check_function_tb_internal(fnoid, fcinfo);
}

Datum
plpgsql_show_dependency_tb_name(PG_FUNCTION_ARGS)
{
	Oid		fnoid;
	char   *name_or_signature;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	name_or_signature = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fnoid = plpgsql_check_parse_name_or_signature(name_or_signature);

	return show_dependency_tb_internal(fnoid, fcinfo);
}

Datum
plpgsql_profiler_function_tb_name(PG_FUNCTION_ARGS)
{
	Oid		fnoid;
	char   *name_or_signature;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	name_or_signature = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fnoid = plpgsql_check_parse_name_or_signature(name_or_signature);

	return profiler_function_tb_internal(fnoid, fcinfo);
}

Datum
plpgsql_profiler_function_statements_tb_name(PG_FUNCTION_ARGS)
{
	Oid		fnoid;
	char   *name_or_signature;

	if (PG_ARGISNULL(0))
		elog(ERROR, "the first argument should not be null");

	name_or_signature = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fnoid = plpgsql_check_parse_name_or_signature(name_or_signature);

	return profiler_function_statements_tb_internal(fnoid, fcinfo);
}
