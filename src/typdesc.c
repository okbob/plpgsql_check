/*-------------------------------------------------------------------------
 *
 * typdesc.c
 *
 *			  deduction result tupdesc from expression
 *
 * by Pavel Stehule 2013-2019
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/spi_priv.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"

#if PG_VERSION_NUM >= 120000

#include "optimizer/optimizer.h"

#endif

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM >= 110000

/*
 * Try to calculate procedure row target from used INOUT variables
 *
 */
PLpgSQL_row *
plpgsql_check_CallExprGetRowTarget(PLpgSQL_checkstate *cstate, PLpgSQL_expr *CallExpr)
{
	Node	   *node;
	FuncExpr   *funcexpr;
	PLpgSQL_row *result = NULL;

	if (CallExpr->plan != NULL)
	{
		PLpgSQL_row *row;
		CachedPlanSource *plansource;
		HeapTuple		tuple;
		List	   *funcargs;
		Oid		   *argtypes;
		char	  **argnames;
		char	   *argmodes;
		ListCell   *lc;
		int			i;
		int			nfields = 0;

		plansource = plpgsql_check_get_plan_source(cstate, CallExpr->plan);

		/*
		 * Get the original CallStmt
		 */
		node = linitial_node(Query, plansource->query_list)->utilityStmt;
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
		row->dno = -1;
		row->refname = NULL;
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
 * Returns a tuple descriptor based on existing plan, When error is detected
 * returns null. Does hardwork when result is based on record type.
 *
 */
TupleDesc
plpgsql_check_expr_get_desc(PLpgSQL_checkstate *cstate,
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
		plansource = plpgsql_check_get_plan_source(cstate, query->plan);

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

#if PG_VERSION_NUM >= 120000

							LOCAL_FCINFO(fcinfo, 0);

#else

							FunctionCallInfoData fcinfo_data;
							FunctionCallInfo fcinfo = &fcinfo_data;

#endif

							TupleDesc	rd;
							Oid			rt;

							fmgr_info(fn->funcid, &flinfo);
							flinfo.fn_expr = (Node *) fn;
							fcinfo->flinfo = &flinfo;

							get_call_result_type(fcinfo, &rt, &rd);
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
