

	plpgsql_check_info		cinfo;
	plpgsql_check_result_info ri;
	ReturnSetInfo *rsinfo;
	ErrorContextCallback *prev_errorcontext;

	if (PG_NARGS() != 6)
		elog(ERROR, "unexpected number of parameters, you should to update extension");

	/* check to see if caller supports us returning a tuplestore */
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	SetReturningFunctionCheck(rsinfo);

	cinfo->fn_oid = PG_GETARG_OID(0);
	cinfo->relid = PG_GETARG_OID(1);
	cinfo->fatal_errors = PG_GETARG_BOOL(2);
	cinfo->other_warnings = PG_GETARG_BOOL(3);
	cinfo->performance_warnings = PG_GETARG_BOOL(4);
	cinfo->extra_warnings = PG_GETARG_BOOL(5);

	cinfo->proctuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(cinfo->fn_oid));
	if (!HeapTupleIsValid(cinfo->proctuple))
		elog(ERROR, "cache lookup failed for function %u", cinfo->fn_oid);

	cinfo->trigtype = plpgsql_check_get_trigtype(cinfo->proctuple);
	plpgsql_check_precheck_conditions(cinfo);

	/* Envelope outer plpgsql function is not interesting */
	prev_errorcontext = error_context_stack;
	error_context_stack = NULL;

	plpgsql_check_init_ri(&ri, PLPGSQL_CHECK_FORMAT_TABULAR, rsinfo);

	plpgsql_check_function_internal(plpgsql_check_result_info *ri, plpgsql_check_info *cinfo);

