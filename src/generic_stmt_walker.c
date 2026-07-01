/*-------------------------------------------------------------------------
 *
 * generic_stmt_walker.c
 *
 *			  allows to execute defined routines over PL/pgSQL AST
 *
 * by Pavel Stehule 2013-2026
 *
 *-------------------------------------------------------------------------
 *
 * This code is based on iterator over PL/pgSQL statement tree from
 * pl_funcs.c, but modified for plpgsql_check purposes.
 */
#include "postgres.h"
#include "plpgsql.h"
#include "plpgsql_check.h"

#ifndef foreach_ptr

/*
 * cut from pg_list.h
 *
 * remove when foreach_ptr will be supported on all versions
 *
 */
#define foreach_ptr(type, var, lst) foreach_internal(type, *, var, lst, lfirst)
#define foreach_int(var, lst)	foreach_internal(int, , var, lst, lfirst_int)
#define foreach_oid(var, lst)	foreach_internal(Oid, , var, lst, lfirst_oid)
#define foreach_xid(var, lst)	foreach_internal(TransactionId, , var, lst, lfirst_xid)

/*
 * The internal implementation of the above macros.  Do not use directly.
 *
 * This macro actually generates two loops in order to declare two variables of
 * different types.  The outer loop only iterates once, so we expect optimizing
 * compilers will unroll it, thereby optimizing it away.
 */
#define foreach_internal(type, pointer, var, lst, func) \
	for (type pointer var = 0, pointer var##__outerloop = (type pointer) 1; \
		 var##__outerloop; \
		 var##__outerloop = 0) \
		for (ForEachState var##__state = {(lst), 0}; \
			 (var##__state.l != NIL && \
			  var##__state.i < var##__state.l->length && \
			 (var = (type pointer) func(&var##__state.l->elements[var##__state.i]), true)); \
			 var##__state.i++)

#endif


void
plch_statement_tree_walker_impl(PLpgSQL_stmt *stmt,
								plch_stmt_walker_callback stmt_callback,
								plch_expr_walker_callback expr_callback,
								void *context)
{
#define S_WALK(st) do { if (stmt_callback) stmt_callback(st, context); } while(0)
#define E_WALK(st, ex) do { if (expr_callback) expr_callback(st, ex, context); } while(0)
#define S_LIST_WALK(lst) foreach_ptr(PLpgSQL_stmt, st, lst) S_WALK(st)
#define E_LIST_WALK(lst) foreach_ptr(PLpgSQL_expr, ex, lst) E_WALK(stmt, ex)

	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			{
				PLpgSQL_stmt_block *bstmt = (PLpgSQL_stmt_block *) stmt;

				S_LIST_WALK(bstmt->body);
				if (bstmt->exceptions)
				{
					foreach_ptr(PLpgSQL_exception, exc, bstmt->exceptions->exc_list)
					{
						/* conditions list has no interesting sub-structure */
						S_LIST_WALK(exc->action);
					}
				}
				break;
			}
		case PLPGSQL_STMT_ASSIGN:
			{
				PLpgSQL_stmt_assign *astmt = (PLpgSQL_stmt_assign *) stmt;

				E_WALK(stmt, astmt->expr);
				break;
			}
		case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *ifstmt = (PLpgSQL_stmt_if *) stmt;

				E_WALK(stmt, ifstmt->cond);
				S_LIST_WALK(ifstmt->then_body);
				foreach_ptr(PLpgSQL_if_elsif, elif, ifstmt->elsif_list)
				{
					E_WALK(stmt, elif->cond);
					S_LIST_WALK(elif->stmts);
				}
				S_LIST_WALK(ifstmt->else_body);
				break;
			}
		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *cstmt = (PLpgSQL_stmt_case *) stmt;

				E_WALK(stmt, cstmt->t_expr);
				foreach_ptr(PLpgSQL_case_when, cwt, cstmt->case_when_list)
				{
					E_WALK(stmt, cwt->expr);
					S_LIST_WALK(cwt->stmts);
				}
				S_LIST_WALK(cstmt->else_stmts);
				break;
			}
		case PLPGSQL_STMT_LOOP:
			{
				PLpgSQL_stmt_loop *lstmt = (PLpgSQL_stmt_loop *) stmt;

				S_LIST_WALK(lstmt->body);
				break;
			}
		case PLPGSQL_STMT_WHILE:
			{
				PLpgSQL_stmt_while *wstmt = (PLpgSQL_stmt_while *) stmt;

				E_WALK(stmt, wstmt->cond);
				S_LIST_WALK(wstmt->body);
				break;
			}
		case PLPGSQL_STMT_FORI:
			{
				PLpgSQL_stmt_fori *fori = (PLpgSQL_stmt_fori *) stmt;

				E_WALK(stmt, fori->lower);
				E_WALK(stmt, fori->upper);
				E_WALK(stmt, fori->step);
				S_LIST_WALK(fori->body);
				break;
			}
		case PLPGSQL_STMT_FORS:
			{
				PLpgSQL_stmt_fors *fors = (PLpgSQL_stmt_fors *) stmt;

				S_LIST_WALK(fors->body);
				E_WALK(stmt, fors->query);
				break;
			}
		case PLPGSQL_STMT_FORC:
			{
				PLpgSQL_stmt_forc *forc = (PLpgSQL_stmt_forc *) stmt;

				S_LIST_WALK(forc->body);
				E_WALK(stmt, forc->argquery);
				break;
			}
		case PLPGSQL_STMT_FOREACH_A:
			{
				PLpgSQL_stmt_foreach_a *fstmt = (PLpgSQL_stmt_foreach_a *) stmt;

				E_WALK(stmt, fstmt->expr);
				S_LIST_WALK(fstmt->body);
				break;
			}
		case PLPGSQL_STMT_EXIT:
			{
				PLpgSQL_stmt_exit *estmt = (PLpgSQL_stmt_exit *) stmt;

				E_WALK(stmt, estmt->cond);
				break;
			}
		case PLPGSQL_STMT_RETURN:
			{
				PLpgSQL_stmt_return *rstmt = (PLpgSQL_stmt_return *) stmt;

				E_WALK(stmt, rstmt->expr);
				break;
			}
		case PLPGSQL_STMT_RETURN_NEXT:
			{
				PLpgSQL_stmt_return_next *rstmt = (PLpgSQL_stmt_return_next *) stmt;

				E_WALK(stmt, rstmt->expr);
				break;
			}
		case PLPGSQL_STMT_RETURN_QUERY:
			{
				PLpgSQL_stmt_return_query *rstmt = (PLpgSQL_stmt_return_query *) stmt;

				E_WALK(stmt, rstmt->query);
				E_WALK(stmt, rstmt->dynquery);
				E_LIST_WALK(rstmt->params);
				break;
			}
		case PLPGSQL_STMT_RAISE:
			{
				PLpgSQL_stmt_raise *rstmt = (PLpgSQL_stmt_raise *) stmt;

				E_LIST_WALK(rstmt->params);
				foreach_ptr(PLpgSQL_raise_option, opt, rstmt->options)
				{
					E_WALK(stmt, opt->expr);
				}
				break;
			}
		case PLPGSQL_STMT_ASSERT:
			{
				PLpgSQL_stmt_assert *astmt = (PLpgSQL_stmt_assert *) stmt;

				E_WALK(stmt, astmt->cond);
				E_WALK(stmt, astmt->message);
				break;
			}
		case PLPGSQL_STMT_EXECSQL:
			{
				PLpgSQL_stmt_execsql *xstmt = (PLpgSQL_stmt_execsql *) stmt;

				E_WALK(stmt, xstmt->sqlstmt);
				break;
			}
		case PLPGSQL_STMT_DYNEXECUTE:
			{
				PLpgSQL_stmt_dynexecute *dstmt = (PLpgSQL_stmt_dynexecute *) stmt;

				E_WALK(stmt, dstmt->query);
				E_LIST_WALK(dstmt->params);
				break;
			}
		case PLPGSQL_STMT_DYNFORS:
			{
				PLpgSQL_stmt_dynfors *dstmt = (PLpgSQL_stmt_dynfors *) stmt;

				S_LIST_WALK(dstmt->body);
				E_WALK(stmt, dstmt->query);
				E_LIST_WALK(dstmt->params);
				break;
			}
		case PLPGSQL_STMT_GETDIAG:
			{
				/* no interesting sub-structure */
				break;
			}
		case PLPGSQL_STMT_OPEN:
			{
				PLpgSQL_stmt_open *ostmt = (PLpgSQL_stmt_open *) stmt;

				E_WALK(stmt, ostmt->argquery);
				E_WALK(stmt, ostmt->query);
				E_WALK(stmt, ostmt->dynquery);
				E_LIST_WALK(ostmt->params);
				break;
			}
		case PLPGSQL_STMT_FETCH:
			{
				PLpgSQL_stmt_fetch *fstmt = (PLpgSQL_stmt_fetch *) stmt;

				E_WALK(stmt, fstmt->expr);
				break;
			}
		case PLPGSQL_STMT_CLOSE:
			{
				/* no interesting sub-structure */
				break;
			}
		case PLPGSQL_STMT_PERFORM:
			{
				PLpgSQL_stmt_perform *pstmt = (PLpgSQL_stmt_perform *) stmt;

				E_WALK(stmt, pstmt->expr);
				break;
			}
		case PLPGSQL_STMT_CALL:
			{
				PLpgSQL_stmt_call *cstmt = (PLpgSQL_stmt_call *) stmt;

				E_WALK(stmt, cstmt->expr);
				break;
			}
		case PLPGSQL_STMT_COMMIT:
		case PLPGSQL_STMT_ROLLBACK:
			{
				/* no interesting sub-structure */
				break;
			}
	}
}

/*
 * Returns true, when statement is any form of loop
 */
bool
plch_statement_is_loop(PLpgSQL_stmt *stmt)
{
	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_FORS:
		case PLPGSQL_STMT_FORC:
		case PLPGSQL_STMT_DYNFORS:
		case PLPGSQL_STMT_FOREACH_A:
		case PLPGSQL_STMT_WHILE:
			return true;
		default:
			return false;
	}
}

/*
 * Returns statements assigned to cycle's body
 */
List *
plch_statement_get_loop_body(PLpgSQL_stmt *stmt)
{
	List	   *stmts;

	Assert(plch_statement_is_loop(stmt));

	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_WHILE:
			stmts = ((PLpgSQL_stmt_while *) stmt)->body;
			break;
		case PLPGSQL_STMT_LOOP:
			stmts = ((PLpgSQL_stmt_loop *) stmt)->body;
			break;
		case PLPGSQL_STMT_FORI:
			stmts = ((PLpgSQL_stmt_fori *) stmt)->body;
			break;
		case PLPGSQL_STMT_FORS:
			stmts = ((PLpgSQL_stmt_fors *) stmt)->body;
			break;
		case PLPGSQL_STMT_FORC:
			stmts = ((PLpgSQL_stmt_forc *) stmt)->body;
			break;
		case PLPGSQL_STMT_DYNFORS:
			stmts = ((PLpgSQL_stmt_dynfors *) stmt)->body;
			break;
		case PLPGSQL_STMT_FOREACH_A:
			stmts = ((PLpgSQL_stmt_foreach_a *) stmt)->body;
			break;
		default:
			stmts = NIL;
			break;
	}

	return stmts;
}

/*
 * Returns true when statement can contains another statements
 */
bool
plch_statement_is_container(PLpgSQL_stmt *stmt)
{
	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
		case PLPGSQL_STMT_IF:
		case PLPGSQL_STMT_CASE:
		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_FORS:
		case PLPGSQL_STMT_FORC:
		case PLPGSQL_STMT_DYNFORS:
		case PLPGSQL_STMT_FOREACH_A:
		case PLPGSQL_STMT_WHILE:
			return true;
		default:
			return false;
	}
}

/*
 * Given a PLpgSQL_stmt, return the underlying PLpgSQL_expr that may contain a
 * queryid.
 */
PLpgSQL_expr *
plch_statement_get_expr(PLpgSQL_stmt *stmt, bool *is_dynamic, List **params, const char **exprname)
{
	PLpgSQL_expr *expr = NULL;
	List	   *_params = NIL;
	bool		_is_dynamic = false;
	const char *_exprname = "expr";

	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_ASSIGN:
			expr = ((PLpgSQL_stmt_assign *) stmt)->expr;
			break;
		case PLPGSQL_STMT_PERFORM:
			expr = ((PLpgSQL_stmt_perform *) stmt)->expr;
			_exprname = "perform";
			break;
		case PLPGSQL_STMT_CALL:
			expr = ((PLpgSQL_stmt_call *) stmt)->expr;
			break;
		case PLPGSQL_STMT_IF:
			expr = ((PLpgSQL_stmt_if *) stmt)->cond;
			_exprname = "cond";
			break;
		case PLPGSQL_STMT_CASE:
			expr = ((PLpgSQL_stmt_case *) stmt)->t_expr;
			_exprname = "cond";
			break;
		case PLPGSQL_STMT_WHILE:
			expr = ((PLpgSQL_stmt_while *) stmt)->cond;
			break;
		case PLPGSQL_STMT_FORC:
			expr = ((PLpgSQL_stmt_forc *) stmt)->argquery;
			_exprname = "query";
			break;
		case PLPGSQL_STMT_FORS:
			expr = ((PLpgSQL_stmt_fors *) stmt)->query;
			_exprname = "query";
			break;
		case PLPGSQL_STMT_DYNFORS:
			expr = ((PLpgSQL_stmt_dynfors *) stmt)->query;
			_exprname = "query";
			_params = ((PLpgSQL_stmt_dynfors *) stmt)->params;
			_is_dynamic = true;
			break;
		case PLPGSQL_STMT_FOREACH_A:
			expr = ((PLpgSQL_stmt_foreach_a *) stmt)->expr;
			break;
		case PLPGSQL_STMT_FETCH:
			expr = ((PLpgSQL_stmt_fetch *) stmt)->expr;
			break;
		case PLPGSQL_STMT_EXIT:
			expr = ((PLpgSQL_stmt_exit *) stmt)->cond;
			break;
		case PLPGSQL_STMT_RETURN:
			expr = ((PLpgSQL_stmt_return *) stmt)->expr;
			break;
		case PLPGSQL_STMT_RETURN_NEXT:
			expr = ((PLpgSQL_stmt_return_next *) stmt)->expr;
			break;
		case PLPGSQL_STMT_RETURN_QUERY:
			{
				PLpgSQL_stmt_return_query *q;

				q = (PLpgSQL_stmt_return_query *) stmt;
				if (q->query)
				{
					expr = q->query;
					_exprname = "query";
				}
				else
				{
					expr = q->dynquery;
					_params = q->params;
					_is_dynamic = true;
				}
			}
			break;
		case PLPGSQL_STMT_ASSERT:
			expr = ((PLpgSQL_stmt_assert *) stmt)->cond;
			break;
		case PLPGSQL_STMT_EXECSQL:
			expr = ((PLpgSQL_stmt_execsql *) stmt)->sqlstmt;
			_exprname = "query";
			break;
		case PLPGSQL_STMT_DYNEXECUTE:
			expr = ((PLpgSQL_stmt_dynexecute *) stmt)->query;
			_params = ((PLpgSQL_stmt_dynexecute *) stmt)->params;
			_is_dynamic = true;
			break;
		case PLPGSQL_STMT_OPEN:
			{
				PLpgSQL_stmt_open *o;

				o = (PLpgSQL_stmt_open *) stmt;
				if (o->query)
				{
					expr = o->query;
					_exprname = "query";
				}
				else if (o->dynquery)
				{
					expr = o->dynquery;
					_params = o->params;
					_is_dynamic = true;
				}
				else
					expr = o->argquery;
			}
		case PLPGSQL_STMT_BLOCK:
		case PLPGSQL_STMT_COMMIT:
		case PLPGSQL_STMT_ROLLBACK:
		case PLPGSQL_STMT_GETDIAG:
		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_RAISE:
		case PLPGSQL_STMT_CLOSE:
			break;
	}

	if (is_dynamic)
		*is_dynamic = _is_dynamic;

	if (params)
		*params = _params;

	if (exprname)
		*exprname = _exprname;

	return expr;
}
