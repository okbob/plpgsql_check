/*-------------------------------------------------------------------------
 *
 * check_expr.c
 *
 *			  routines for enforce plans for every expr/query and
 *			  related checks over these plans.
 *
 * by Pavel Stehule 2013-2020
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "access/tupconvert.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/spi_priv.h"
#include "optimizer/clauses.h"

#if PG_VERSION_NUM >= 120000

#include "optimizer/optimizer.h"

#endif

#if PG_VERSION_NUM >= 140000

#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"

#endif

#include "tcop/utility.h"
#include "utils/lsyscache.h"

static void collect_volatility(PLpgSQL_checkstate *cstate, Query *query);
static Query * ExprGetQuery(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);

static CachedPlan * get_cached_plan(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool *has_result_desc);
static void plan_checks(PLpgSQL_checkstate *cstate, CachedPlan *cplan, char *query_str);
static void prohibit_write_plan(PLpgSQL_checkstate *cstate, CachedPlan *cplan, char *query_str);
static void prohibit_transaction_stmt(PLpgSQL_checkstate *cstate, CachedPlan *cplan, char *query_str);
static void check_fishy_qual(PLpgSQL_checkstate *cstate, CachedPlan *cplan, char *query_str);

static Const * expr_get_const(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
static bool is_const_null_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);
static void force_plan_checks(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr);

static int RowGetValidFields(PLpgSQL_row *row);
static int TupleDescNVatts(TupleDesc tupdesc);


/*
 * Generate a prepared plan - this is simplified copy from pl_exec.c Is not
 * necessary to check simple plan, returns true, when expression is
 * succesfully prepared.
 */
static void
prepare_plan(PLpgSQL_checkstate *cstate,
			 PLpgSQL_expr *expr,
			 int cursorOptions,
			 ParserSetupHook parser_setup,
			 void *arg)
{
	SPIPlanPtr	plan;
	Query	   *query;

	if (expr->plan == NULL)
	{
		MemoryContext old_cxt;

#if PG_VERSION_NUM >= 140000

		SPIPrepareOptions options;

		memset(&options, 0, sizeof(options));
		options.parserSetup = parser_setup ?
			parser_setup : (ParserSetupHook) plpgsql_check__parser_setup_p;
		options.parserSetupArg = arg ? arg : (void *) expr;
		options.parseMode = expr->parseMode;
		options.cursorOptions = cursorOptions;

#endif

		/*
		 * The grammar can't conveniently set expr->func while building the parse
		 * tree, so make sure it's set before parser hooks need it.
		 */
		expr->func = cstate->estate->func;

#if PG_VERSION_NUM >= 140000

		/*
		 * Generate and save the plan
		 */
		plan = SPI_prepare_extended(expr->query, &options);

#else

		/*
		 * Generate and save the plan
		 */
		plan = SPI_prepare_params(expr->query,
								  parser_setup ? parser_setup : (ParserSetupHook) plpgsql_check__parser_setup_p,
								  arg ? arg : (void *) expr,
								  cursorOptions);

#endif

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
		 * Save prepared plan to plpgsql_check state context. It will be
		 * released on end of check, and it should be valid to this time.
		 */
		old_cxt = MemoryContextSwitchTo(cstate->check_cxt);
		expr->plan = SPI_saveplan(plan);

		/* This plan should be released later */
		cstate->exprs = lappend(cstate->exprs, expr);

		MemoryContextSwitchTo(old_cxt);

		SPI_freeplan(plan);
	}

	query = ExprGetQuery(cstate, expr);

	/* there checks are common on every expr/query */
	plpgsql_check_funcexpr(cstate, query, expr->query);
	collect_volatility(cstate, query);
	plpgsql_check_detect_dependency(cstate, query);
}

/*
 * Update function's volatility flag by query
 */
static void
collect_volatility(PLpgSQL_checkstate *cstate, Query *query)
{
	if (cstate->skip_volatility_check ||
			cstate->volatility == PROVOLATILE_VOLATILE ||
			!cstate->cinfo->performance_warnings)
		return;

	if (query->commandType == CMD_SELECT)
	{
		if (!query->hasModifyingCTE && !query->hasForUpdate)
		{
			/* there is chance so query will be immutable */
			if (plpgsql_check_contain_volatile_functions((Node *) query, cstate))
				cstate->volatility = PROVOLATILE_VOLATILE;
			else if (!plpgsql_check_contain_mutable_functions((Node *) query, cstate))
			{
				/*
				 * when level is still immutable, check if there
				 * are not reference to tables.
				 */
				if (cstate->volatility == PROVOLATILE_IMMUTABLE)
				{
					if (plpgsql_check_has_rtable(query))
						cstate->volatility = PROVOLATILE_STABLE;
				}
			}
			else
				cstate->volatility = PROVOLATILE_STABLE;
		}
		else
			cstate->volatility = PROVOLATILE_VOLATILE;
	}
	else
		/* not read only statements requare VOLATILE flag */
		cstate->volatility = PROVOLATILE_VOLATILE;
}

/*
 * Validate plan and returns related node.
 */
CachedPlanSource *
plpgsql_check_get_plan_source(PLpgSQL_checkstate *cstate, SPIPlanPtr plan)
{
	CachedPlanSource *plansource;

	if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
		elog(ERROR, "cached plan is not valid plan");

	cstate->has_mp = false;

	if (list_length(plan->plancache_list) != 1)
	{
		/*
		 * We can allow multiple plans for commands executed by
		 * EXECUTE command. Result of last plan is result. But
		 * it can be allowed only in main query - not in parameters.
		 */
		if (cstate->allow_mp)
		{
			/* take last */
			plansource = (CachedPlanSource *) llast(plan->plancache_list);
			cstate->has_mp = true;
		}
		else
			elog(ERROR, "plan is not single execution planyy");
	}
	else
		plansource = (CachedPlanSource *) linitial(plan->plancache_list);

	return plansource;
}

/*
 * Returns Query node for expression
 *
 */
static Query *
ExprGetQuery(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	CachedPlanSource *plansource;
	Query *result = NULL;

	plansource = plpgsql_check_get_plan_source(cstate, expr->plan);

	/*
	 * query_list has more fields, when rules are used. There
	 * can be combination INSERT; NOTIFY
	 */
	if (list_length(plansource->query_list) > 1)
	{
		ListCell   *lc;
		CmdType		first_ctype = CMD_UNKNOWN;
		bool		first = true;

		foreach (lc, plansource->query_list)
		{
			Query	   *query = (Query *) lfirst(lc);

			if (first)
			{
				first = false;
				first_ctype = query->commandType;
				result = query;
			}
			else
			{
				/*
				 * When current command is SELECT, then first command
				 * should be SELECT too
				 */
				if (query->commandType == CMD_SELECT)
				{
					if (first_ctype != CMD_SELECT)
						ereport(ERROR,
								(errmsg("there is not single query"),
								 errdetail("plpgsql_check cannot detect result type"),
								 errhint("Probably there are some unsupported (by plpgsql_check) rules on related tables")));

					result = query;
				}
			}
		}
	}
	else
		result = linitial(plansource->query_list);

	cstate->was_pragma = false;

	/* the test of PRAGMA function call */
	if (result->commandType == CMD_SELECT)
	{

#if PG_VERSION_NUM < 100000

		if (plansource->raw_parse_tree &&
			IsA(plansource->raw_parse_tree, SelectStmt))
		{
			SelectStmt *selectStmt = (SelectStmt *) plansource->raw_parse_tree;

#else

		if (plansource->raw_parse_tree &&
			plansource->raw_parse_tree->stmt &&
			IsA(plansource->raw_parse_tree->stmt, SelectStmt))
		{
			SelectStmt *selectStmt = (SelectStmt *) plansource->raw_parse_tree->stmt;

#endif
			if (selectStmt->targetList && IsA(linitial(selectStmt->targetList), ResTarget))
			{
				ResTarget *rt = (ResTarget *) linitial(selectStmt->targetList);

				if (rt->val && IsA(rt->val, FuncCall))
				{
					char	   *funcname;
					char	   *schemaname;
					FuncCall   *fc = (FuncCall *) rt->val;

					DeconstructQualifiedName(fc->funcname, &schemaname, &funcname);

					if (strcmp(funcname, "plpgsql_check_pragma") == 0)
					{
						ListCell	   *lc;

						cstate->was_pragma = true;

						foreach(lc, fc->args)
						{
							Node *arg = (Node *) lfirst(lc);

							if (IsA(arg, A_Const))
							{
								A_Const *ac = (A_Const *) arg;

								if (ac->val.type == T_String)
									plpgsql_check_pragma_apply(cstate, strVal(&(ac->val)));
							}
						}
					}
				}
			}
		}
	}

	return result;
}

/*
 * Operations that requires cached plan
 *
 */

/*
 * Returns cached plan from plan cache.
 *
 */
static CachedPlan *
get_cached_plan(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool *has_result_desc)
{
	CachedPlanSource *plansource = NULL;
	CachedPlan	*cplan;

	plansource = plpgsql_check_get_plan_source(cstate, expr->plan);

	*has_result_desc = plansource->resultDesc ? true : false;

#if PG_VERSION_NUM >= 140000

	cplan = GetCachedPlan(plansource, NULL, NULL, NULL);

#elif PG_VERSION_NUM >= 100000

	cplan = GetCachedPlan(plansource, NULL, true, NULL);

#else

	cplan = GetCachedPlan(plansource, NULL, true);

#endif

	return cplan;
}

/*
 * Process common checks on cached plan
 *
 */
static void
plan_checks(PLpgSQL_checkstate *cstate, CachedPlan *cplan, char *query_str)
{
	/* disallow write op in read only function */
	prohibit_write_plan(cstate, cplan, query_str);

	/* detect bad casts in quals */
	check_fishy_qual(cstate, cplan, query_str);

	/* disallow BEGIN TRANS, COMMIT, ROLLBACK, .. */
	prohibit_transaction_stmt(cstate, cplan, query_str);
}

/*
 * Raise a error when plan is not read only
 */
static void
prohibit_write_plan(PLpgSQL_checkstate *cstate, CachedPlan *cplan, char *query_str)
{
	ListCell	*lc;

	if (!cstate->estate->readonly_func)
		return;

	foreach(lc, cplan->stmt_list)
	{

#if PG_VERSION_NUM >= 100000

		PlannedStmt *pstmt = (PlannedStmt *) lfirst(lc);

#else

		Node *pstmt = (Node *) lfirst(lc);

#endif

		if (!CommandIsReadOnly(pstmt))
		{
			StringInfoData message;

			initStringInfo(&message);

#if PG_VERSION_NUM >= 130000

			appendStringInfo(&message,
					"%s is not allowed in a non volatile function",
							GetCommandTagName(CreateCommandTag((Node *) pstmt)));

#else

			appendStringInfo(&message,
					"%s is not allowed in a non volatile function",
							CreateCommandTag((Node *) pstmt));

#endif

			plpgsql_check_put_error(cstate,
					  ERRCODE_FEATURE_NOT_SUPPORTED, 0,
					  message.data,
					  NULL,
					  NULL,
					  PLPGSQL_CHECK_ERROR,
					  0, query_str, NULL);

			pfree(message.data);
			message.data = NULL;
		}
	}
}

/*
 * Raise a error when plan is a transactional statement
 */
static void
prohibit_transaction_stmt(PLpgSQL_checkstate *cstate, CachedPlan *cplan, char *query_str)
{
	ListCell	*lc;

	foreach(lc, cplan->stmt_list)
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
			plpgsql_check_put_error(cstate,
									ERRCODE_FEATURE_NOT_SUPPORTED, 0,
									"cannot begin/end transactions in PL/pgSQL",
									NULL,
									"Use a BEGIN block with an EXCEPTION clause instead.",
									PLPGSQL_CHECK_ERROR,
									0, query_str, NULL);
		}
	}
}

/*
 * Raise a performance warning when plan hash fishy qual
 */
static void
check_fishy_qual(PLpgSQL_checkstate *cstate, CachedPlan *cplan, char *query_str)
{
	ListCell	*lc;

	if (!cstate->cinfo->performance_warnings)
		return;

	foreach(lc, cplan->stmt_list)
	{
		Param	*param;
		PlannedStmt *pstmt = (PlannedStmt *) lfirst(lc);
		Plan *plan = NULL;

		/* Only plans can contains fishy quals */
		if(!IsA(pstmt, PlannedStmt))
			continue;

		plan = pstmt->planTree;

		if (plpgsql_check_qual_has_fishy_cast(pstmt, plan, &param))
		{
			plpgsql_check_put_error(cstate,
					  ERRCODE_DATATYPE_MISMATCH, 0,
					  "implicit cast of attribute caused by different PLpgSQL variable type in WHERE clause",
					  "An index of some attribute cannot be used, when variable, used in predicate, has not right type like a attribute",
					  "Check a variable type - int versus numeric",
					  PLPGSQL_CHECK_WARNING_PERFORMANCE,
					  param->location,
					  query_str, NULL);
		}
	}
}

Node *
plpgsql_check_expr_get_node(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool force_plan_checks)
{
	PlannedStmt *_stmt;
	CachedPlan *cplan;
	Node	   *result = NULL;
	bool		has_result_desc;

	cplan = get_cached_plan(cstate, expr, &has_result_desc);
	if (!has_result_desc)
		elog(ERROR, "expression does not return data");

	/* do all checks for this plan, reduce a access to plan cache */
	if (force_plan_checks)
		plan_checks(cstate, cplan, expr->query);

	_stmt = (PlannedStmt *) linitial(cplan->stmt_list);

	if (has_result_desc && IsA(_stmt, PlannedStmt) &&_stmt->commandType == CMD_SELECT)
	{
		Plan	   *_plan;
		TargetEntry *tle;

		_plan = _stmt->planTree;
		if (IsA(_plan, Result) &&list_length(_plan->targetlist) == 1)
		{
			tle = (TargetEntry *) linitial(_plan->targetlist);
			result = (Node *) tle->expr;
		}
	}

#if PG_VERSION_NUM >= 140000

	ReleaseCachedPlan(cplan, NULL);

#else

	ReleaseCachedPlan(cplan, true);

#endif

	return result;
}

/*
 * Returns Const Value from expression if it is possible.
 *
 * Ensure all plan related checks on expression.
 *
 */
static Const *
expr_get_const(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	Node *node = plpgsql_check_expr_get_node(cstate, expr, true);

	if (node && node->type == T_Const)
		return (Const *) node;

	return NULL;
}

/*
 * Returns true, when expr is constant NULL
 *
 */
static bool
is_const_null_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	Const	   *c;

	c = expr_get_const(cstate, expr);

	return c && c->constisnull ? true : false;
}

char *
plpgsql_check_const_to_string(Const *c)
{
	if (IsA((Node *) c, Const))
	{
		if (!c->constisnull)
		{
			Oid		typoutput;
			bool	typisvarlena;

			getTypeOutputInfo(c->consttype, &typoutput, &typisvarlena);

			return OidOutputFunctionCall(typoutput, c->constvalue);
		}
	}

	return NULL;
}

/*
 * Returns string for any not null constant. isnull is true,
 * when constant is null.
 *
 */
char *
plpgsql_check_expr_get_string(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool *isnull)
{
	Const	   *c;

	c = expr_get_const(cstate, expr);
	if (c)
	{
		*isnull = c->constisnull;

		return plpgsql_check_const_to_string(c);
	}

	return NULL;
}

static void
force_plan_checks(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	CachedPlan *cplan;
	bool		has_result_desc;

	cplan = get_cached_plan(cstate, expr, &has_result_desc);

	/* do all checks for this plan, reduce a access to plan cache */
	plan_checks(cstate, cplan, expr->query);

#if PG_VERSION_NUM >= 140000

	ReleaseCachedPlan(cplan, NULL);

#else

	ReleaseCachedPlan(cplan, true);

#endif

}

/*
 * No casts, no other checks
 *
 */
void
plpgsql_check_expr_generic(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	prepare_plan(cstate, expr, 0, NULL, NULL);
	force_plan_checks(cstate, expr);
}

void
plpgsql_check_expr_generic_with_parser_setup(PLpgSQL_checkstate *cstate,
											 PLpgSQL_expr *expr,
											 ParserSetupHook parser_setup,
											 void *arg)
{
	prepare_plan(cstate, expr, 0, parser_setup, arg);
	force_plan_checks(cstate, expr);
}

/*
 * Top level checks - forces prepare_plan, protected by subtransaction.
 *
 */

/*
 * Verify to possible cast to bool, integer, ..
 *
 */
void
plpgsql_check_expr_with_scalar_type(PLpgSQL_checkstate *cstate,
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

		prepare_plan(cstate, expr, 0, NULL, NULL);
		/* record all variables used by the query */
		cstate->used_variables = bms_add_members(cstate->used_variables, expr->paramnos);

		tupdesc = plpgsql_check_expr_get_desc(cstate, expr, false, true, true, NULL);
		is_immutable_null = is_const_null_expr(cstate, expr);

		if (tupdesc)
		{
			/* when we know value or type */
			if (!is_immutable_null)
				plpgsql_check_assign_to_target_type(cstate,
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
		if (cstate->cinfo->fatal_errors)
			ReThrowError(edata);
		else
			plpgsql_check_put_error_edata(cstate, edata);
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
void
plpgsql_check_returned_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr, bool is_expression)
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

		prepare_plan(cstate, expr, 0, NULL, NULL);

		/* record all variables used by the query, should be after prepare_plan */
		cstate->used_variables = bms_add_members(cstate->used_variables, expr->paramnos);

		tupdesc = plpgsql_check_expr_get_desc(cstate, expr, false, true, is_expression, &first_level_typ);
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
					plpgsql_check_put_error(cstate,
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
				if (!cstate->fake_rtd && estate->rsi && IsA(estate->rsi, ReturnSetInfo))
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
					plpgsql_check_assign_to_target_type(cstate,
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
		if (cstate->cinfo->fatal_errors)
			ReThrowError(edata);
		else
			plpgsql_check_put_error_edata(cstate, edata);
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
void
plpgsql_check_expr_as_rvalue(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
					  PLpgSQL_rec *targetrec, PLpgSQL_row *targetrow,
					int targetdno, bool use_element_type, bool is_expression)
{
	ResourceOwner oldowner;
	MemoryContext oldCxt = CurrentMemoryContext;
	TupleDesc	tupdesc;
	bool is_immutable_null;
	volatile bool expand = true;
	Oid			first_level_typoid;
	Oid expected_typoid = InvalidOid;
	int expected_typmod = InvalidOid;

	if (targetdno != -1)
	{
		plpgsql_check_target(cstate, targetdno, &expected_typoid, &expected_typmod);

		/*
		 * When target variable is not compossite, then we should not
		 * to expand result tupdesc.
		 */
		if (!type_is_rowtype(expected_typoid))
			expand = false;

#if PG_VERSION_NUM >= 140000

		expr->target_param = targetdno;
	}
	else
		expr->target_param = -1;

#else

	}

#endif

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		prepare_plan(cstate, expr, 0, NULL, NULL);
		/* record all variables used by the query */

#if PG_VERSION_NUM >= 140000

		if (expr->target_param != -1)
		{
			int		target_dno = expr->target_param;
			Node *node;
			Oid		target_typoid = InvalidOid;
			Oid		value_typoid = InvalidOid;

			node = plpgsql_check_expr_get_node(cstate, expr, false);

			if (bms_is_member(target_dno, expr->paramnos))
			{
				/* recheck if target_dno is really used on right side of assignment */
				if (!plpgsql_check_vardno_is_used_for_reading(node, target_dno))
				{
					Bitmapset *paramnos;

					/* create set without target_param */
					paramnos = bms_copy(expr->paramnos);
					paramnos = bms_del_member(paramnos, expr->target_param);
					cstate->used_variables = bms_add_members(cstate->used_variables, paramnos);

					bms_free(paramnos);
				}
				else
					cstate->used_variables = bms_add_members(cstate->used_variables,
															 expr->paramnos);
			}
			else
				cstate->used_variables = bms_add_members(cstate->used_variables,
														 expr->paramnos);

			if (node && IsA(node, SubscriptingRef))
				node = (Node *) ((SubscriptingRef *) node)->refassgnexpr;

			/* check implicit coercion */
			if (node && IsA(node, FuncExpr))
			{
				FuncExpr	   *fexpr = (FuncExpr *) node;

				if (fexpr->funcformat == COERCE_IMPLICIT_CAST)
				{
					target_typoid = fexpr->funcresulttype;
					value_typoid = exprType(linitial(fexpr->args));
				}
			}
			else if (node && IsA(node, CoerceViaIO))
			{
				CoerceViaIO	   *cexpr = (CoerceViaIO *) node;

				if (cexpr->coerceformat == COERCE_IMPLICIT_CAST)
				{
					target_typoid = cexpr->resulttype;
					value_typoid = exprType((Node *) cexpr->arg);
				}
			}

			if (target_typoid != value_typoid)
			{
				StringInfoData	str;

				initStringInfo(&str);
				appendStringInfo(&str, "cast \"%s\" value to \"%s\" type",
											format_type_be(value_typoid),
											format_type_be(target_typoid));

				/* accent warning when cast is without supported explicit casting */
				if (!can_coerce_type(1, &value_typoid, &target_typoid, COERCION_EXPLICIT))
					plpgsql_check_put_error(cstate,
								  ERRCODE_DATATYPE_MISMATCH, 0,
								  "target type is different type than source type",
								  str.data,
								  "There are no possible explicit coercion between those types, possibly bug!",
								  PLPGSQL_CHECK_WARNING_OTHERS,
								  0, NULL, NULL);
				else if (!can_coerce_type(1, &value_typoid, &target_typoid, COERCION_ASSIGNMENT))
					plpgsql_check_put_error(cstate,
								  ERRCODE_DATATYPE_MISMATCH, 0,
								  "target type is different type than source type",
								  str.data,
								  "The input expression type does not have an assignment cast to the target type.",
								  PLPGSQL_CHECK_WARNING_OTHERS,
								  0, NULL, NULL);
				else
					plpgsql_check_put_error(cstate,
								  ERRCODE_DATATYPE_MISMATCH, 0,
								  "target type is different type than source type",
								  str.data,
								  "Hidden casting can be a performance issue.",
								  PLPGSQL_CHECK_WARNING_PERFORMANCE,
								  0, NULL, NULL);

				pfree(str.data);
			}
		}
		else
			cstate->used_variables = bms_add_members(cstate->used_variables,
													 expr->paramnos);

#else

		cstate->used_variables = bms_add_members(cstate->used_variables, expr->paramnos);

#endif

		/*
		 * there is a possibility to call a plpgsql_pragma like default for some aux
		 * variable. When we detect this case, then we mark target variable as used
		 * variable.
		 */
		if (cstate->was_pragma && targetdno != -1)
			cstate->used_variables = bms_add_member(cstate->used_variables, targetdno);

		tupdesc = plpgsql_check_expr_get_desc(cstate, expr, use_element_type, expand, is_expression, &first_level_typoid);
		is_immutable_null = is_const_null_expr(cstate, expr);

		/* try to detect safe variables */
		if (cstate->cinfo->security_warnings && targetdno != -1)
		{
			PLpgSQL_var *var = (PLpgSQL_var *) cstate->estate->datums[targetdno];

			if (var->dtype == PLPGSQL_DTYPE_VAR)
			{
				bool	typispreferred;
				char 	typcategory;

				get_type_category_preferred(var->datatype->typoid, &typcategory, &typispreferred);
				if (typcategory == 'S')
				{
					Node *node = plpgsql_check_expr_get_node(cstate, expr, false);
					int		location;

					if (plpgsql_check_is_sql_injection_vulnerable(cstate, expr, node, &location))
						cstate->safe_variables = bms_del_member(cstate->safe_variables, targetdno);
					else
						cstate->safe_variables = bms_add_member(cstate->safe_variables, targetdno);
				}
			}
		}

		if (expected_typoid != InvalidOid && type_is_rowtype(expected_typoid) && first_level_typoid != InvalidOid)
		{
			/* simple error, scalar source to composite target */
			if (!type_is_rowtype(first_level_typoid) && !is_immutable_null)
			{
				plpgsql_check_put_error(cstate,
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
				plpgsql_check_assign_tupdesc_row_or_rec(cstate, targetrow, targetrec, tupdesc, is_immutable_null);
			if (targetdno != -1)
				plpgsql_check_assign_tupdesc_dno(cstate, targetdno, tupdesc, is_immutable_null);

			if (targetrow)
			{
				if (RowGetValidFields(targetrow) > TupleDescNVatts(tupdesc))
					plpgsql_check_put_error(cstate,
								  0, 0,
								  "too few attributes for target variables",
								  "There are more target variables than output columns in query.",
						  "Check target variables in SELECT INTO statement.",
								  PLPGSQL_CHECK_WARNING_OTHERS,
								  0, NULL, NULL);
				else if (RowGetValidFields(targetrow) < TupleDescNVatts(tupdesc))
					plpgsql_check_put_error(cstate,
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
		if (cstate->cinfo->fatal_errors)
			ReThrowError(edata);
		else
			plpgsql_check_put_error_edata(cstate, edata);
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
void
plpgsql_check_expr_as_sqlstmt_nodata(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	if (expr && plpgsql_check_expr_as_sqlstmt(cstate, expr))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("query has no destination for result data")));
}

/*
 * Check a SQL statement, should to return data
 *
 */
void
plpgsql_check_expr_as_sqlstmt_data(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	if (expr && !plpgsql_check_expr_as_sqlstmt(cstate, expr))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("query does not return data")));
}

/*
 * Check a SQL statement, can (not) returs data. Returns true
 * when statement returns data - we are able to get tuple descriptor.
 */
bool
plpgsql_check_expr_as_sqlstmt(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	ResourceOwner oldowner;
	MemoryContext oldCxt = CurrentMemoryContext;
	TupleDesc	tupdesc;
	volatile bool result = false;

	if (!expr)
		return true;

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		prepare_plan(cstate, expr, 0, NULL, NULL);
		/* record all variables used by the query */
		cstate->used_variables = bms_add_members(cstate->used_variables, expr->paramnos);
		force_plan_checks(cstate, expr);

		tupdesc = plpgsql_check_expr_get_desc(cstate, expr, false, false, false, NULL);
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
		if (cstate->cinfo->fatal_errors)
			ReThrowError(edata);
		else
			plpgsql_check_put_error_edata(cstate, edata);
		MemoryContextSwitchTo(oldCxt);

		/* reconnect spi */
		SPI_restore_connection();
	}
	PG_END_TRY();

	return result;
}

/*
 * Verify an assignment of 'expr' to 'target' with possible slices
 *
 * it is used in FOREACH ARRAY where SLICE change a target type
 *
 */
void
plpgsql_check_assignment_with_possible_slices(PLpgSQL_checkstate *cstate,
											  PLpgSQL_expr *expr,
											  PLpgSQL_rec *targetrec,
											  PLpgSQL_row *targetrow,
											  int targetdno,
											  bool use_element_type)
{
	bool		is_expression = (targetrec == NULL && targetrow == NULL);

	if (expr)
		plpgsql_check_expr_as_rvalue(cstate,
									 expr,
									 targetrec,
									 targetrow,
									 targetdno,
									 use_element_type,
									 is_expression);
}

/*
 * Verify a expression
 *
 */
void
plpgsql_check_expr(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr)
{
	if (expr)
		plpgsql_check_expr_as_rvalue(cstate,
									 expr,
									 NULL,
									 NULL,
									 -1,
									 false,
									 true);
}

/*
 * Verify an assignment of 'expr' to 'target'
 *
 */
void
plpgsql_check_assignment(PLpgSQL_checkstate *cstate,
						 PLpgSQL_expr *expr,
						 PLpgSQL_rec *targetrec,
						 PLpgSQL_row *targetrow,
						 int targetdno)
{
	bool		is_expression = (targetrec == NULL && targetrow == NULL);

	plpgsql_check_expr_as_rvalue(cstate,
								 expr,
								 targetrec,
								 targetrow,
								 targetdno,
								 false,
								 is_expression);
}

#if PG_VERSION_NUM >= 110000

void
plpgsql_check_assignment_to_variable(PLpgSQL_checkstate *cstate,
							 PLpgSQL_expr *expr,
							 PLpgSQL_variable *targetvar,
							 int targetdno)
{
	if (targetvar != NULL)
	{
		if (targetvar->dtype == PLPGSQL_DTYPE_ROW)
			plpgsql_check_expr_as_rvalue(cstate,
										 expr,
										 NULL,
										 (PLpgSQL_row *) targetvar,
										 targetdno,
										 false,
										 false);

		else if (targetvar->dtype == PLPGSQL_DTYPE_REC)
			plpgsql_check_expr_as_rvalue(cstate,
										 expr,
										 (PLpgSQL_rec *) targetvar,
										 NULL,
										 targetdno,
										 false,
										 false);

		else
			elog(ERROR, "unsupported target variable type");
	}
	else
		plpgsql_check_expr_as_rvalue(cstate,
									 expr,
									 NULL,
									 NULL,
									 targetdno,
									 false,
									 true);
}

#endif


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
 * returns number of valid fields
 */
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
