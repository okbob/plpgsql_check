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

		setup_cstate(&cstate,
						func->fn_oid,
						func->fn_rettype,
						provolatile,
						NULL,
						NULL,
						plpgsql_check_fatal_errors,
						plpgsql_check_other_warnings,
						plpgsql_check_performance_warnings,
						plpgsql_check_extra_warnings,
						PLPGSQL_CHECK_FORMAT_ELOG,
						false,
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

