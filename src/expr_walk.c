/*-------------------------------------------------------------------------
 *
 * expr_walk.c
 *
 *			  set of Query/Expr walkers
 *
 * by Pavel Stehule 2013-2018
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/*
 * Send to ouput all not yet displayed relations and functions.
 */
static bool
detect_dependency_walker(Node *node, void *context)
{
	PLpgSQL_checkstate *cstate = (PLpgSQL_checkstate *) context;
	plpgsql_check_result_info *ri = cstate->result_info;

	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		ListCell *lc;

		foreach(lc, query->rtable)
		{
			RangeTblEntry *rt = (RangeTblEntry *) lfirst(lc);

			if (rt->rtekind == RTE_RELATION)
			{
				if (!bms_is_member(rt->relid, cstate->rel_oids))
				{
					plpgsql_check_put_dependency(ri,
												 "RELATION",
												 rt->relid,
												 get_namespace_name(get_rel_namespace(rt->relid)),
												 get_rel_name(rt->relid),
												 NULL);

					cstate->rel_oids = bms_add_member(cstate->rel_oids, rt->relid);
				}
			}
		}

		return query_tree_walker((Query *) node,
								 detect_dependency_walker,
								 context, 0);
	}

	if (IsA(node, FuncExpr) )
	{
		FuncExpr *fexpr = (FuncExpr *) node;

		if (get_func_namespace(fexpr->funcid) != PG_CATALOG_NAMESPACE)
		{
			if (!bms_is_member(fexpr->funcid, cstate->func_oids))
			{
				StringInfoData		str;
				ListCell		   *lc;
				int		i = 0;

				initStringInfo(&str);
				appendStringInfoChar(&str, '(');
				foreach(lc, fexpr->args)
				{
					Node *expr = (Node *) lfirst(lc);

					if (i++ > 0)
						appendStringInfoChar(&str, ',');

					appendStringInfoString(&str, format_type_be(exprType(expr)));
				}
				appendStringInfoChar(&str, ')');

				plpgsql_check_put_dependency(ri,
											  "FUNCTION",
											  fexpr->funcid,
											  get_namespace_name(get_func_namespace(fexpr->funcid)),
											  get_func_name(fexpr->funcid),
											  str.data);

				pfree(str.data);

				cstate->func_oids = bms_add_member(cstate->func_oids, fexpr->funcid);
			}
		}
	}

	return expression_tree_walker(node, detect_dependency_walker, context);
}

void
plpgsql_check_detect_dependency(PLpgSQL_checkstate *cstate, Query *query)
{
	if (cstate->result_info->format != PLPGSQL_SHOW_DEPENDENCY_FORMAT_TABULAR)
		return;

	detect_dependency_walker((Node *) query, cstate);
}

/*
 * Expecting persistent oid of nextval, currval and setval functions.
 * Ensured by regress tests.
 */
#define NEXTVAL_OID		1574
#define CURRVAL_OID		1575
#define SETVAL_OID		1576
#define SETVAL2_OID		1765

typedef struct 
{
	PLpgSQL_checkstate *cstate;
	char *query_str;
} check_seq_functions_walker_params;

/*
 * When sequence related functions has constant oid parameter, then ensure, so
 * this oid is related to some sequnce object.
 *
 */
static bool
check_seq_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node,
								 check_seq_functions_walker,
								 context, 0);
	}
	if (IsA(node, FuncExpr))
	{
		FuncExpr *fexpr = (FuncExpr *) node;
		int		location = -1;

		switch (fexpr->funcid)
		{
			case NEXTVAL_OID:
			case CURRVAL_OID:
			case SETVAL_OID:
			case SETVAL2_OID:
			{
				Node *first_arg = linitial(fexpr->args);

				location = fexpr->location;

				if (first_arg && IsA(first_arg, Const))
				{
					Const *c = (Const *) first_arg;

					if (c->consttype == REGCLASSOID && !c->constisnull)
					{
						Oid		classid;

						if (c->location != -1)
							location = c->location;

						classid = DatumGetObjectId(c->constvalue);

						if (get_rel_relkind(classid) != RELKIND_SEQUENCE)
						{
							char	message[1024];
							check_seq_functions_walker_params *wp;

							wp = (check_seq_functions_walker_params *) context;

							snprintf(message, sizeof(message), "\"%s\" is not a sequence", get_rel_name(classid));

							plpgsql_check_put_error(wp->cstate,
													ERRCODE_WRONG_OBJECT_TYPE, 0,
					  								message,
													NULL, NULL,
													PLPGSQL_CHECK_ERROR,
													location,
													wp->query_str, NULL);
						}
					}
				}
			}
		}
	}

	return expression_tree_walker(node, check_seq_functions_walker, context);
}

void
plpgsql_check_sequence_functions(PLpgSQL_checkstate *cstate, Query *query, char *query_str)
{
	check_seq_functions_walker_params  wp;

	wp.cstate = cstate;
	wp.query_str = query_str;

	check_seq_functions_walker((Node *) query, &wp);
}

/*
 * Try to detect relations in query
 */
static bool
has_rtable_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		bool		has_relation = false;
		ListCell *lc;

		foreach (lc, query->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

			if (rte->rtekind == RTE_RELATION)
			{
				has_relation = true;
				break;
			}
		}

		if (has_relation)
		{
			return true;
		}
		else
			return query_tree_walker(query, has_rtable_walker, context, 0);
	}
	return expression_tree_walker(node, has_rtable_walker, context);
}

/*
 * Returns true, if query use any relation
 */
bool
plpgsql_check_has_rtable(Query *query)
{
	return has_rtable_walker((Node *) query, NULL);
}

/*
 * Try to identify constraint where variable from one side is implicitly casted to
 * parameter type of second side. It can symptom of parameter wrong type.
 *
 */
static bool
contain_fishy_cast_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, OpExpr))
	{
		OpExpr *opexpr = (OpExpr *) node;

		if (!opexpr->opretset && opexpr->opresulttype == BOOLOID
				&& list_length(opexpr->args) == 2)
		{
			Node *l1 = linitial(opexpr->args);
			Node *l2 = lsecond(opexpr->args);
			Param *param = NULL;
			FuncExpr *fexpr = NULL;

			if (IsA(l1, Param))
				param = (Param *) l1;
			else if (IsA(l1, FuncExpr))
				fexpr = (FuncExpr *) l1;

			if (IsA(l2, Param))
				param = (Param *) l2;
			else if (IsA(l2, FuncExpr))
				fexpr = (FuncExpr *) l2;

			if (param != NULL && fexpr != NULL)
			{
				if (param->paramkind != PARAM_EXTERN)
					return false;

				if (fexpr->funcformat != COERCE_IMPLICIT_CAST ||
						fexpr->funcretset ||
						list_length(fexpr->args) != 1 ||
						param->paramtype != fexpr->funcresulttype)
					return false;

				if (!IsA(linitial(fexpr->args), Var))
					return false;

				*((Param **) context) = param;

				return true;
			}
		}
	}

	return expression_tree_walker(node, contain_fishy_cast_walker, context);
}

bool
plpgsql_check_qual_has_fishy_cast(PlannedStmt *plannedstmt, Plan *plan, Param **param)
{
	ListCell *lc;

	if (plan == NULL)
		return false;

	if (contain_fishy_cast_walker((Node *) plan->qual, param))
		return true;

	if (plpgsql_check_qual_has_fishy_cast(plannedstmt, innerPlan(plan), param))
		return true;
	if (plpgsql_check_qual_has_fishy_cast(plannedstmt, outerPlan(plan), param))
		return true;

	foreach(lc, plan->initPlan)
	{
		SubPlan *subplan = (SubPlan *) lfirst(lc);
		Plan *s_plan = exec_subplan_get_plan(plannedstmt, subplan);

		if (plpgsql_check_qual_has_fishy_cast(plannedstmt, s_plan, param))
			return true;
	}

	return false;
}
