/*-------------------------------------------------------------------------
 *
 * typdesc.c
 *
 *			  deduction result tupdesc from expression
 *
 * by Pavel Stehule 2013-2025
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
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

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
		CallStmt   *stmt;
		HeapTuple	tuple;
		Oid		   *argtypes;
		char	  **argnames;
		char	   *argmodes;
		int			i;
		int			nfields = 0;
		int			numargs;

		plansource = plpgsql_check_get_plan_source(cstate, CallExpr->plan);

		/*
		 * Get the original CallStmt
		 */
		node = linitial_node(Query, plansource->query_list)->utilityStmt;
		if (!IsA(node, CallStmt))
			elog(ERROR, "returned row from not a CallStmt");

		stmt = castNode(CallStmt, node);
		funcexpr = stmt->funcexpr;

		/*
		 * Get the argument modes
		 */
		tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcexpr->funcid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for function %u", funcexpr->funcid);

		/*
		 * Get the argument names and modes, so that we can deliver on-point
		 * error messages when something is wrong.
		 */
		numargs = get_func_arg_info(tuple, &argtypes, &argnames, &argmodes);

		ReleaseSysCache(tuple);

		row = (PLpgSQL_row *) palloc0(sizeof(PLpgSQL_row));
		row->dtype = PLPGSQL_DTYPE_ROW;
		row->refname = NULL;
		row->dno = -1;
		row->lineno = -1;
		row->varnos = (int *) palloc(numargs * sizeof(int));

		/*
		 * Examine procedure's argument list.  Each output arg position should
		 * be an unadorned plpgsql variable (Datum), which we can insert into
		 * the row Datum.
		 */
		nfields = 0;
		for (i = 0; i < numargs; i++)
		{
			if (argmodes &&
				(argmodes[i] == PROARGMODE_INOUT ||
				 argmodes[i] == PROARGMODE_OUT))
			{
				Node	   *n = list_nth(stmt->outargs, nfields);

				if (IsA(n, Param))
				{
					Param	   *param = (Param *) n;
					int			dno;

					/* paramid is offset by 1 (see make_datum_param()) */
					dno = param->paramid - 1;
					/* must check assignability now, because grammar can't */
					plpgsql_check_is_assignable(cstate->estate, dno);
					row->varnos[nfields++] = dno;
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
		}

		Assert(nfields == list_length(stmt->outargs));

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

/*
 * Returns typoid, typmod associated with record variable
 */
void
plpgsql_check_recvar_info(PLpgSQL_rec *rec, Oid *typoid, int32 *typmod)
{
	if (rec->dtype != PLPGSQL_DTYPE_REC)
		elog(ERROR, "variable is not record type");

	if (rec->rectypeid != RECORDOID)
	{
		if (typoid != NULL)
			*typoid = rec->rectypeid;
		if (typmod != NULL)
			*typmod = -1;
	}
	else if (recvar_tupdesc(rec) != NULL)
	{
		TupleDesc	tdesc = recvar_tupdesc(rec);

		BlessTupleDesc(tdesc);

		if (typoid != NULL)
			*typoid = tdesc->tdtypeid;
		if (typmod != NULL)
			*typmod = tdesc->tdtypmod;
	}
	else
	{
		if (typoid != NULL)
			*typoid = RECORDOID;
		if (typmod != NULL)
			*typmod = -1;
	}
}

static TupleDesc
param_get_desc(PLpgSQL_checkstate *cstate, Param *p)
{
	TupleDesc	rettupdesc = NULL;

	if (!type_is_rowtype(p->paramtype))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("function does not return composite type, is not possible to identify composite type")));

	if (p->paramkind == PARAM_EXTERN && p->paramid > 0 && p->location != -1)
	{
		int			dno;
		PLpgSQL_var *var;

		/*
		 * When paramid looks well and related datum is variable with same
		 * type, then we can check, if this variable has sanitized content
		 * already.
		 */
		dno = p->paramid - 1;
		var = (PLpgSQL_var *) cstate->estate->datums[dno];

		if (!var->datatype ||
			!OidIsValid(var->datatype->typoid) ||
			var->datatype->typoid == 0xFFFFFFFF ||
			var->datatype->typoid == p->paramtype)
		{
			TupleDesc	rectupdesc;

			if (var->dtype == PLPGSQL_DTYPE_REC)
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) var;
				Oid			typoid;
				int32		typmod;

				plpgsql_check_recvar_info(rec, &typoid, &typmod);

				rectupdesc = lookup_rowtype_tupdesc_noerror(typoid, typmod, true);
				if (rectupdesc)
				{
					rettupdesc = CreateTupleDescCopy(rectupdesc);
					ReleaseTupleDesc(rectupdesc);
				}
			}
			else
			{
				rectupdesc = lookup_rowtype_tupdesc_noerror(p->paramtype, p->paramtypmod, true);

				if (rectupdesc != NULL)
				{
					rettupdesc = CreateTupleDescCopy(rectupdesc);
					ReleaseTupleDesc(rectupdesc);
				}
			}
		}
	}

	return rettupdesc;
}

/*
 * Try to deduce result tuple descriptor from polymorphic function
 * like fce(.., anyelement, ..) returns anyelement
 */
static TupleDesc
pofce_get_desc(PLpgSQL_checkstate *cstate,
			   PLpgSQL_expr *expr,
			   FuncExpr *fn)
{
	HeapTuple	func_tuple;
	Form_pg_proc procStruct;
	Oid			fnoid = fn->funcid;
	TupleDesc	result = NULL;

	func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fnoid));
	if (!HeapTupleIsValid(func_tuple))
		elog(ERROR, "cache lookup failed for function %u", fnoid);

	procStruct = (Form_pg_proc) GETSTRUCT(func_tuple);

	if (procStruct->prorettype == ANYELEMENTOID)
	{
		Oid		   *argtypes;
		char	   *argmodes;
		char	  **argnames;
		int			pronallargs;
		int			i;

		pronallargs = get_func_arg_info(func_tuple, &argtypes, &argnames, &argmodes);

		for (i = 0; i < pronallargs; i++)
		{
			if (argmodes &&
				(argmodes[i] != PROARGMODE_IN &&
				 argmodes[i] != PROARGMODE_INOUT))
				continue;

			if (argtypes[i] == ANYELEMENTOID)
			{
				if (IsA(list_nth(fn->args, i), Param))
				{
					Param	   *p = (Param *) list_nth(fn->args, i);

					if (p->paramkind == PARAM_EXTERN && p->paramid > 0 && p->location != -1)
					{
						int			dno = p->paramid - 1;

						/*
						 * When paramid looks well and related datum is
						 * variable with same type, then we can check, if this
						 * variable has sanitized content already.
						 */
						if (expr && bms_is_member(dno, expr->paramnos))
						{
							PLpgSQL_var *var = (PLpgSQL_var *) cstate->estate->datums[dno];

							/*
							 * When we know a datatype, then we expect eq with
							 * param type. But sometimes a Oid of datatype is
							 * not valid - record type for some older
							 * releases. What is worse - sometimes Oid is 0 or
							 * FFFFFFFF.
							 */
							if (var->dtype == PLPGSQL_DTYPE_REC &&
								(!var->datatype ||
								 !OidIsValid(var->datatype->typoid) ||
								 var->datatype->typoid == 0xFFFFFFFF ||
								 var->datatype->typoid == p->paramtype))
							{
								PLpgSQL_rec *rec = (PLpgSQL_rec *) var;
								Oid			typoid;
								int32		typmod;
								TupleDesc	rectupdesc;

								plpgsql_check_recvar_info(rec, &typoid, &typmod);

								rectupdesc = lookup_rowtype_tupdesc_noerror(typoid, typmod, true);
								if (rectupdesc)
								{
									result = CreateTupleDescCopy(rectupdesc);
									ReleaseTupleDesc(rectupdesc);

									break;
								}
							}
						}
					}
				}
			}
		}

		if (argtypes)
			pfree(argtypes);
		if (argnames)
			pfree(argnames);
		if (argmodes)
			pfree(argmodes);
	}

	ReleaseSysCache(func_tuple);

	return result;
}

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

		/*
		 * The EXECUTE command can sucessfully execute empty string. Then the
		 * plansource is empty - NULL.
		 */
		if (!plansource)
			return NULL;

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
			TupleDesc	rettupdesc;

			rettupdesc = CreateTemplateTupleDesc(1);

			TupleDescInitEntry(rettupdesc, 1, "__array_element__", elemtype, -1, 0);

			FreeTupleDesc(tupdesc);
			BlessTupleDesc(rettupdesc);

			tupdesc = rettupdesc;
		}
		else
		{
			TupleDesc	elemtupdesc;

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
		cplan = GetCachedPlan(plansource, NULL, NULL, NULL);
		_stmt = (PlannedStmt *) linitial(cplan->stmt_list);

		if (IsA(_stmt, PlannedStmt) && _stmt->commandType == CMD_SELECT)
		{
			_plan = _stmt->planTree;

			if (IsA(_plan, Result) && list_length(_plan->targetlist) == 1)
			{
				tle = (TargetEntry *) linitial(_plan->targetlist);

				switch (((Node *) tle->expr)->type)
				{
					case T_FuncExpr:
						{
							FuncExpr   *fn = (FuncExpr *) tle->expr;
							FmgrInfo	flinfo;

							LOCAL_FCINFO(fcinfo, 0);
							TupleDesc	rd;
							Oid			rt;
							TypeFuncClass tfc;

							fmgr_info(fn->funcid, &flinfo);
							flinfo.fn_expr = (Node *) fn;
							fcinfo->flinfo = &flinfo;

							fcinfo->resultinfo = NULL;

							tfc = get_call_result_type(fcinfo, &rt, &rd);
							if (tfc == TYPEFUNC_SCALAR || tfc == TYPEFUNC_OTHER)
								ereport(ERROR,
										(errcode(ERRCODE_DATATYPE_MISMATCH),
										 errmsg("function does not return composite type, is not possible to identify composite type")));

							FreeTupleDesc(tupdesc);

							if (rd)
							{
								BlessTupleDesc(rd);
								tupdesc = rd;
							}
							else
							{
								/*
								 * for polymorphic function we can determine
								 * record typmod (and tupdesc) from arguments.
								 */
								tupdesc = pofce_get_desc(cstate, query, fn);
							}
						}
						break;

					case T_RowExpr:
						{
							RowExpr    *row = (RowExpr *) tle->expr;
							ListCell   *lc_colname;
							ListCell   *lc_arg;
							TupleDesc	rettupdesc;
							int			i = 1;

							rettupdesc = CreateTemplateTupleDesc(list_length(row->args));

							forboth(lc_colname, row->colnames, lc_arg, row->args)
							{
								Node	   *arg = lfirst(lc_arg);
								char	   *name = strVal(lfirst(lc_colname));

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
							Const	   *c = (Const *) tle->expr;

							FreeTupleDesc(tupdesc);

							if (c->consttype == RECORDOID && c->consttypmod == -1 && !c->constisnull)
							{
								Oid			tupType;
								int32		tupTypmod;

								HeapTupleHeader rec = DatumGetHeapTupleHeader(c->constvalue);

								tupType = HeapTupleHeaderGetTypeId(rec);
								tupTypmod = HeapTupleHeaderGetTypMod(rec);
								tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
							}
							else
								tupdesc = NULL;
						}
						break;

					case T_Param:
						{
							Param	   *p = (Param *) tle->expr;

							if (!type_is_rowtype(p->paramtype))
								ereport(ERROR,
										(errcode(ERRCODE_DATATYPE_MISMATCH),
										 errmsg("function does not return composite type, is not possible to identify composite type")));

							FreeTupleDesc(tupdesc);
							tupdesc = param_get_desc(cstate, p);
						}
						break;

					default:
						/* cannot to take tupdesc */
						FreeTupleDesc(tupdesc);
						tupdesc = NULL;
				}
			}
		}

		ReleaseCachedPlan(cplan, NULL);
	}
	return tupdesc;
}
