/*-------------------------------------------------------------------------
 *
 * stmtwalk.c
 *
 *			  iteration over plpgsql statements loop
 *
 * by Pavel Stehule 2013-2019
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "access/tupconvert.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"

#include "nodes/nodeFuncs.h"
#include "parser/parse_node.h"

#if PG_VERSION_NUM < 90600

#include "parser/keywords.h"

#else

#include "common/keywords.h"

#endif

static void check_stmts(PLpgSQL_checkstate *cstate, List *stmts, int *closing, List **exceptions);
static PLpgSQL_stmt_stack_item * push_stmt_to_stmt_stack(PLpgSQL_checkstate *cstate);
static void pop_stmt_from_stmt_stack(PLpgSQL_checkstate *cstate);
static bool is_any_loop_stmt(PLpgSQL_stmt *stmt);
static PLpgSQL_stmt * find_nearest_loop(PLpgSQL_stmt_stack_item *current);
static PLpgSQL_stmt * find_stmt_with_label(char *label, PLpgSQL_stmt_stack_item *current);
static int possibly_closed(int c);
static int merge_closing(int c, int c_local, List **exceptions, List *exceptions_local, int err_code);
static bool exception_matches_conditions(int sqlerrstate, PLpgSQL_condition *cond);
static bool found_shadowed_variable(char *varname, PLpgSQL_stmt_stack_item *current, PLpgSQL_checkstate *cstate);

#if PG_VERSION_NUM >= 110000

static void check_dynamic_sql(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt, PLpgSQL_expr *query, bool into, PLpgSQL_variable *target, List *params);

#else

static void check_dynamic_sql(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt, PLpgSQL_expr *query, bool into, PLpgSQL_row *row, PLpgSQL_rec *rec, List *params);

#endif

#if PG_VERSION_NUM >= 110000

static void
check_variable(PLpgSQL_checkstate *cstate, PLpgSQL_variable *var)
{
	/* leave quickly when var is not defined */
	if (var == NULL)
		return;

	if (var->dtype == PLPGSQL_DTYPE_ROW)
	{
		PLpgSQL_row *row = (PLpgSQL_row *) var;
		int		fnum;

		for (fnum = 0; fnum < row->nfields; fnum++)
		{
			/* skip dropped columns */
			if (row->varnos[fnum] < 0)
				continue;

			plpgsql_check_target(cstate, row->varnos[fnum], NULL, NULL);
		}
		plpgsql_check_record_variable_usage(cstate, row->dno, true);

		return;
	}

	if (var->dtype == PLPGSQL_DTYPE_REC)
	{
		PLpgSQL_rec *rec = (PLpgSQL_rec *) var;

		/*
		 * There are no checks done on records currently; just record that the
		 * variable is not unused.
		 */
		plpgsql_check_record_variable_usage(cstate, rec->dno, true);

		return;
	}

	elog(ERROR, "unsupported dtype %d", var->dtype);
}

#endif

bool
plpgsql_check_is_reserved_keyword(char *name)
{
	int		i;

#if PG_VERSION_NUM < 120000

	for (i = 0; i < NumScanKeywords; i++)
	{
		if (ScanKeywords[i].category == RESERVED_KEYWORD &&
				strcmp(name, ScanKeywords[i].name) == 0)
			return true;
	}

#else

	for (i = 0; i < ScanKeywords.num_keywords; i++)
	{
		if (ScanKeywordCategories[i] == RESERVED_KEYWORD)
		{
			char	   *value;

			value = unconstify(char *, GetScanKeyword(i, &ScanKeywords));
			if (strcmp(name, value) == 0)
				return true;
		}
	}

#endif

	return false;
}

/*
 * walk over all plpgsql statements - search and check expressions
 *
 */
void
plpgsql_check_stmt(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt, int *closing, List **exceptions)
{
	TupleDesc	tupdesc = NULL;
	PLpgSQL_function *func;
	ListCell   *l;
	ResourceOwner oldowner;
	MemoryContext oldCxt = CurrentMemoryContext;
	PLpgSQL_stmt_stack_item *outer_stmt;

	if (stmt == NULL)
		return;

	if (cstate->stop_check)
		return;

	cstate->estate->err_stmt = stmt;
	func = cstate->estate->func;

	/*
	 * Attention - returns NULL, when there are not any outer level
	 */
	outer_stmt = push_stmt_to_stmt_stack(cstate);

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldCxt);

	PG_TRY();
	{
		switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
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

						if (d->dtype == PLPGSQL_DTYPE_VAR ||
							d->dtype == PLPGSQL_DTYPE_ROW ||
							d->dtype == PLPGSQL_DTYPE_REC)
						{
							PLpgSQL_variable *var = (PLpgSQL_variable *) d;
							StringInfoData str;

							initStringInfo(&str);
							appendStringInfo(&str, "during statement block local variable \"%s\" initialization on line %d",
												 var->refname,
												 var->lineno);

							cstate->estate->err_text = str.data;

#if PG_VERSION_NUM >= 110000

							if (var->default_val)
								plpgsql_check_assignment(cstate,
														 var->default_val,
														 NULL,
														 NULL,
														 var->dno);

#else

							if (d->dtype == PLPGSQL_DTYPE_VAR &&
									((PLpgSQL_var *) var)->default_val)
								plpgsql_check_assignment(cstate,
														 ((PLpgSQL_var *) var)->default_val,
														 NULL,
														 NULL,
														 var->dno);


#endif

							cstate->estate->err_text = NULL;
							pfree(str.data);
						}

						refname = plpgsql_check_datum_get_refname(d);
						if (refname != NULL)
						{
							ListCell   *l;

							if (plpgsql_check_is_reserved_keyword(refname))
							{
								StringInfoData str;

								initStringInfo(&str);
								appendStringInfo(&str, "name of variable \"%s\" is reserved keyword",
												 refname);

								plpgsql_check_put_error(cstate,
											  0, 0,
											  str.data,
											  "The reserved keyword was used as variable name.",
											  NULL,
											  PLPGSQL_CHECK_WARNING_OTHERS,
											  0, NULL, NULL);
								pfree(str.data);
							}

							foreach(l, cstate->argnames)
							{
								char	   *argname = (char *) lfirst(l);

								if (strcmp(argname, refname) == 0)
								{
									StringInfoData str;

									initStringInfo(&str);
									appendStringInfo(&str, "parameter \"%s\" is overlapped",
													 refname);

									plpgsql_check_put_error(cstate,
												  0, 0,
												  str.data,
												  "Local variable overlap function parameter.",
												  NULL,
												  PLPGSQL_CHECK_WARNING_OTHERS,
												  0, NULL, NULL);
									pfree(str.data);
								}
							}

							if (found_shadowed_variable(refname, outer_stmt, cstate))
							{
								StringInfoData str;

								initStringInfo(&str);
								appendStringInfo(&str, "variable \"%s\" shadows a previously defined variable",
													 refname);

								plpgsql_check_put_error(cstate,
												  0, 0,
												  str.data,
												  NULL,
												  "SET plpgsql.extra_warnings TO 'shadowed_variables'",
												  PLPGSQL_CHECK_WARNING_EXTRA,
												  0, NULL, NULL);
								pfree(str.data);
							}
						}
					}

					check_stmts(cstate, stmt_block->body, closing, exceptions);

					if (stmt_block->exceptions)
					{
						int closing_local;
						List   *exceptions_local = NIL;
						int		closing_handlers = PLPGSQL_CHECK_UNKNOWN;
						List   *exceptions_transformed = NIL;

						if (*closing == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
						{
							ListCell   *lc;
							int			i = 0;
							int    *err_codes = NULL;
							int		nerr_codes = 0;

							/* copy errcodes to a array */
							nerr_codes = list_length(*exceptions);
							err_codes = palloc(sizeof(int) * nerr_codes);

							foreach(lc, *exceptions)
							{
								err_codes[i++] = lfirst_int(lc);
							}

							foreach(l, stmt_block->exceptions->exc_list)
							{
								PLpgSQL_exception *exception = (PLpgSQL_exception *) lfirst(l);

								/* RETURN in exception handler ~ is possible closing */
								check_stmts(cstate, exception->action,
												&closing_local, &exceptions_local);

								if (*exceptions != NIL)
								{
									int		i;

									for (i = 0; i < nerr_codes; i++)
									{
										int		err_code = err_codes[i];

										if (err_code != -1 &&
											exception_matches_conditions(err_code, exception->conditions))
										{
											closing_handlers = merge_closing(closing_handlers, closing_local,
																			 &exceptions_transformed, exceptions_local,
																			 err_code);
											*exceptions = list_delete_int(*exceptions, err_code);
											err_codes[i] = -1;
										}
									}
								}
							}

							Assert(err_codes != NULL);
							pfree(err_codes);

							if (closing_handlers != PLPGSQL_CHECK_UNKNOWN)
							{
								*closing = closing_handlers;
								if (closing_handlers == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
									*exceptions = list_concat_unique_int(*exceptions, exceptions_transformed);
								else
									*exceptions = NIL;
							}
						}
						else
						{
							closing_handlers = *closing;

							foreach(l, stmt_block->exceptions->exc_list)
							{
								PLpgSQL_exception *exception = (PLpgSQL_exception *) lfirst(l);

								/* RETURN in exception handler ~ it is possible closing only */
								check_stmts(cstate, exception->action,
												&closing_local, &exceptions_local);

								closing_handlers = merge_closing(closing_handlers, closing_local,
																 &exceptions_transformed, exceptions_local,
																 -1);
							}

							*closing = closing_handlers;

							if (closing_handlers == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
								*exceptions = exceptions_transformed;
							else
								*exceptions = NIL;
						}

						/*
						 * Mark the hidden variables SQLSTATE and SQLERRM used
						 * even if they actually weren't.  Not using them
						 * should practically never be a sign of a problem, so
						 * there's no point in annoying the user.
						 */
						plpgsql_check_record_variable_usage(cstate, stmt_block->exceptions->sqlstate_varno, false);
						plpgsql_check_record_variable_usage(cstate, stmt_block->exceptions->sqlerrm_varno, false);
					}
				}
				break;

			case PLPGSQL_STMT_ASSERT:
				{
					PLpgSQL_stmt_assert *stmt_assert = (PLpgSQL_stmt_assert *) stmt;

					/*
					 * Should or should not to depends on plpgsql_check_asserts?
					 * I am thinking, so any code (active or inactive) should be valid,
					 * so I ignore plpgsql_check_asserts option.
					 */
					plpgsql_check_expr_with_scalar_type(cstate,
									 stmt_assert->cond, BOOLOID, true);
					if (stmt_assert->message != NULL)
						plpgsql_check_expr(cstate, stmt_assert->message);
				}
				break;

			case PLPGSQL_STMT_ASSIGN:
				{
					PLpgSQL_stmt_assign *stmt_assign = (PLpgSQL_stmt_assign *) stmt;
					PLpgSQL_datum *d = (PLpgSQL_datum *) cstate->estate->datums[stmt_assign->varno];
					StringInfoData str;

					initStringInfo(&str);

					if (d->dtype == PLPGSQL_DTYPE_VAR ||
						d->dtype == PLPGSQL_DTYPE_ROW ||
						d->dtype == PLPGSQL_DTYPE_REC)
					{
						PLpgSQL_variable *var = (PLpgSQL_variable *) d;

						appendStringInfo(&str, "at assignment to variable \"%s\" declared on line %d",
										 var->refname,
										 var->lineno);

						cstate->estate->err_text = str.data;
					}
					else if (d->dtype == PLPGSQL_DTYPE_RECFIELD)
					{
						PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) d;
						PLpgSQL_variable *var = (PLpgSQL_variable *) cstate->estate->datums[recfield->recparentno];

						appendStringInfo(&str, "at assignment to field \"%s\" of variable \"%s\" declared on line %d",
										 recfield->fieldname,
										 var->refname,
										 var->lineno);

						cstate->estate->err_text = str.data;
					}
					else if (d->dtype == PLPGSQL_DTYPE_ARRAYELEM)
					{
						PLpgSQL_arrayelem *elem = (PLpgSQL_arrayelem *) d;
						PLpgSQL_variable *var = (PLpgSQL_variable *) cstate->estate->datums[elem->arrayparentno];

						appendStringInfo(&str, "at assignment to element of variable \"%s\" declared on line %d",
										 var->refname,
										 var->lineno);

						cstate->estate->err_text = str.data;
					}

					plpgsql_check_assignment(cstate, stmt_assign->expr, NULL, NULL,
											 stmt_assign->varno);

					pfree(str.data);
					cstate->estate->err_text = NULL;
				}
				break;

			case PLPGSQL_STMT_IF:
				{
					PLpgSQL_stmt_if	*stmt_if = (PLpgSQL_stmt_if *) stmt;
					ListCell    *l;
					int		closing_local;
					int		closing_all_paths = PLPGSQL_CHECK_UNKNOWN;
					List   *exceptions_local;

					plpgsql_check_expr_with_scalar_type(cstate,
									     stmt_if->cond, BOOLOID, true);

					check_stmts(cstate, stmt_if->then_body, &closing_local,
								&exceptions_local);
					closing_all_paths = merge_closing(closing_all_paths,
													  closing_local,
													  exceptions,
													  exceptions_local,
													  -1);

					foreach(l, stmt_if->elsif_list)
					{
						PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(l);

						plpgsql_check_expr_with_scalar_type(cstate,
										     elif->cond, BOOLOID, true);
						check_stmts(cstate, elif->stmts, &closing_local,
									&exceptions_local);
						closing_all_paths = merge_closing(closing_all_paths,
														  closing_local,
														  exceptions,
														  exceptions_local,
														  -1);
					}

					check_stmts(cstate, stmt_if->else_body, &closing_local,
								&exceptions_local);
					closing_all_paths = merge_closing(closing_all_paths,
													  closing_local,
													  exceptions,
													  exceptions_local,
													  -1);

					if (stmt_if->else_body != NULL)
						*closing = closing_all_paths;
					else if (closing_all_paths == PLPGSQL_CHECK_UNCLOSED)
						*closing = PLPGSQL_CHECK_UNCLOSED;
					else
						*closing = PLPGSQL_CHECK_POSSIBLY_CLOSED;
				}
				break;

			case PLPGSQL_STMT_CASE:
				{
					PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;
					Oid			result_oid;
					int		closing_local;
					List	*exceptions_local;
					int		closing_all_paths = PLPGSQL_CHECK_UNKNOWN;

					if (stmt_case->t_expr != NULL)
					{
						PLpgSQL_var *t_var = (PLpgSQL_var *) cstate->estate->datums[stmt_case->t_varno];

						/*
						 * we need to set hidden variable type
						 */
						plpgsql_check_expr_generic(cstate, stmt_case->t_expr);

						/* record all variables used by the query */
						cstate->used_variables = bms_add_members(cstate->used_variables,
																 stmt_case->t_expr->paramnos);

						tupdesc = plpgsql_check_expr_get_desc(cstate,
												stmt_case->t_expr,
												false,	/* no element type */
												true,	/* expand record */
												true,	/* is expression */
												NULL);
						result_oid = TupleDescAttr(tupdesc, 0)->atttypid;

						/*
						 * When expected datatype is different from real,
						 * change it. Note that what we're modifying here is
						 * an execution copy of the datum, so this doesn't
						 * affect the originally stored function parse tree.
						 */
						if (t_var->datatype->typoid != result_oid)

#ifdef PLPGSQL_BUILD_DATATYPE_4

							t_var->datatype = plpgsql_check__build_datatype_p(result_oid,
																	 -1,
								   cstate->estate->func->fn_input_collation,
								   t_var->datatype->origtypname);

#else

							t_var->datatype = plpgsql_check__build_datatype_p(result_oid,
																	 -1,
								   cstate->estate->func->fn_input_collation);

#endif

						ReleaseTupleDesc(tupdesc);
					}
					foreach(l, stmt_case->case_when_list)
					{
						PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);

						plpgsql_check_expr(cstate, cwt->expr);
						check_stmts(cstate, cwt->stmts, &closing_local, &exceptions_local);
						closing_all_paths = merge_closing(closing_all_paths,
														  closing_local,
														  exceptions,
														  exceptions_local,
														  -1);
					}

					if (stmt_case->else_stmts)
					{
						check_stmts(cstate, stmt_case->else_stmts, &closing_local, &exceptions_local);
						*closing = merge_closing(closing_all_paths,
														  closing_local,
														  exceptions,
														  exceptions_local,
														  -1);
					}
					else
						/* is not ensured all path evaluation */
						*closing = possibly_closed(closing_all_paths);
				}
				break;

			case PLPGSQL_STMT_LOOP:
				check_stmts(cstate, ((PLpgSQL_stmt_loop *) stmt)->body, closing, exceptions);
				break;

			case PLPGSQL_STMT_WHILE:
				{
					PLpgSQL_stmt_while *stmt_while = (PLpgSQL_stmt_while *) stmt;
					int		closing_local;
					List   *exceptions_local;

					plpgsql_check_expr_with_scalar_type(cstate,
										     stmt_while->cond,
										     BOOLOID,
										     true);

					/*
					 * When is not guaranteed execution (possible zero loops),
					 * then ignore closing info from body.
					 */
					check_stmts(cstate, stmt_while->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_FORI:
				{
					PLpgSQL_stmt_fori *stmt_fori = (PLpgSQL_stmt_fori *) stmt;
					int			dno = stmt_fori->var->dno;
					int		closing_local;
					List   *exceptions_local;

					/* prepare plan if desn't exist yet */
					plpgsql_check_assignment(cstate, stmt_fori->lower, NULL, NULL, dno);
					plpgsql_check_assignment(cstate, stmt_fori->upper, NULL, NULL, dno);

					if (stmt_fori->step)
						plpgsql_check_assignment(cstate, stmt_fori->step, NULL, NULL, dno);

					/* this variable should not be updated */
					cstate->protected_variables = bms_add_member(cstate->protected_variables, dno);
					cstate->auto_variables = bms_add_member(cstate->auto_variables, dno);

					check_stmts(cstate, stmt_fori->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_FORS:
				{
					PLpgSQL_stmt_fors *stmt_fors = (PLpgSQL_stmt_fors *) stmt;
					int		closing_local;
					List   *exceptions_local;

#if PG_VERSION_NUM >= 110000

					check_variable(cstate, stmt_fors->var);

					/* we need to set hidden variable type */
					plpgsql_check_assignment_to_variable(cstate, stmt_fors->query,
									 stmt_fors->var, -1);

#else

					plpgsql_check_row_or_rec(cstate, stmt_fors->row, stmt_fors->rec);

					/* we need to set hidden variable type */
					plpgsql_check_assignment(cstate, stmt_fors->query,
									 stmt_fors->rec, stmt_fors->row, -1);

#endif

					check_stmts(cstate, stmt_fors->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_FORC:
				{
					PLpgSQL_stmt_forc *stmt_forc = (PLpgSQL_stmt_forc *) stmt;
					PLpgSQL_var *var = (PLpgSQL_var *) func->datums[stmt_forc->curvar];
					int		closing_local;
					List   *exceptions_local;

#if PG_VERSION_NUM >= 110000

					check_variable(cstate, stmt_forc->var);

#else

					plpgsql_check_row_or_rec(cstate, stmt_forc->row, stmt_forc->rec);

#endif

					plpgsql_check_expr_as_sqlstmt_data(cstate, stmt_forc->argquery);

#if PG_VERSION_NUM >= 110000

					if (var->cursor_explicit_expr != NULL)
						plpgsql_check_assignment_to_variable(cstate, var->cursor_explicit_expr,
										 stmt_forc->var, -1);

#else

					if (var->cursor_explicit_expr != NULL)
						plpgsql_check_assignment(cstate, var->cursor_explicit_expr,
										 stmt_forc->rec, stmt_forc->row, -1);

#endif

					check_stmts(cstate, stmt_forc->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);

					cstate->used_variables = bms_add_member(cstate->used_variables,
										 stmt_forc->curvar);
				}
				break;

			case PLPGSQL_STMT_DYNFORS:
				{
					PLpgSQL_stmt_dynfors *stmt_dynfors = (PLpgSQL_stmt_dynfors *) stmt;
					int		closing_local;
					List   *exceptions_local;

					check_dynamic_sql(cstate,
									  stmt,
									  stmt_dynfors->query,
									  true,

#if PG_VERSION_NUM >= 110000

									  stmt_dynfors->var,

#else

									  stmt_dynfors->row,
									  stmt_dynfors->rec,

#endif

									  stmt_dynfors->params);

					check_stmts(cstate, stmt_dynfors->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_FOREACH_A:
				{
					PLpgSQL_stmt_foreach_a *stmt_foreach_a = (PLpgSQL_stmt_foreach_a *) stmt;
					bool use_element_type;
					int		closing_local;
					List   *exceptions_local;

					plpgsql_check_target(cstate, stmt_foreach_a->varno, NULL, NULL);

					/*
					 * When slice > 0, then result and target are a array.
					 * We shoudl to disable a array element refencing.
					 */
					use_element_type = stmt_foreach_a->slice == 0;

					plpgsql_check_assignment_with_possible_slices(cstate,
											 stmt_foreach_a->expr, 
											 NULL, NULL,
											 stmt_foreach_a->varno,
											 use_element_type);

					check_stmts(cstate, stmt_foreach_a->body, &closing_local, &exceptions_local);
					*closing = possibly_closed(closing_local);
				}
				break;

			case PLPGSQL_STMT_EXIT:
				{
					PLpgSQL_stmt_exit *stmt_exit = (PLpgSQL_stmt_exit *) stmt;

					plpgsql_check_expr_with_scalar_type(cstate,
										     stmt_exit->cond,
										     BOOLOID,
										     false);

					if (stmt_exit->label != NULL)
					{
						PLpgSQL_stmt *labeled_stmt = find_stmt_with_label(stmt_exit->label,
												    outer_stmt);
						if (labeled_stmt == NULL)
							ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("label \"%s\" does not exist", stmt_exit->label)));

						/* CONTINUE only allows loop labels */
						if (!is_any_loop_stmt(labeled_stmt) && !stmt_exit->is_exit)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("block label \"%s\" cannot be used in CONTINUE",
									 stmt_exit->label)));
					}
					else
					{
						if (find_nearest_loop(outer_stmt) == NULL)
							ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("%s cannot be used outside a loop",
								 plpgsql_check__stmt_typename_p((PLpgSQL_stmt *) stmt_exit))));
					}
				}
				break;

			case PLPGSQL_STMT_PERFORM:
				plpgsql_check_expr_as_sqlstmt(cstate, ((PLpgSQL_stmt_perform *) stmt)->expr);

				/*
				 * Note: if you want to raise warning when used expressions returns
				 * some value (other than VOID type), change previous command plpgsql_check_expr
				 * to following check_expr_with_expected_scalar_type. This should be 
				 * not enabled by default, because PERFORM can be used with reason
				 * to ignore result.
				 *
				 * check_expr_with_expected_scalar_type(cstate,
				 * 					     ((PLpgSQL_stmt_perform *) stmt)->expr,
				 * 					     VOIDOID,
				 * 					     true);
				 */

				break;

			case PLPGSQL_STMT_RETURN:
				{
					PLpgSQL_stmt_return *stmt_rt = (PLpgSQL_stmt_return *) stmt;

					if (stmt_rt->retvarno >= 0)
					{
						PLpgSQL_datum *retvar = cstate->estate->datums[stmt_rt->retvarno];
						PLpgSQL_execstate *estate = cstate->estate;

						cstate->used_variables = bms_add_member(cstate->used_variables, stmt_rt->retvarno);

						switch (retvar->dtype)
						{
							case PLPGSQL_DTYPE_VAR:
								{
									PLpgSQL_var *var = (PLpgSQL_var *) retvar;

									plpgsql_check_assign_to_target_type(cstate,
										 cstate->estate->func->fn_rettype, -1,
										 var->datatype->typoid, false);
								}
								break;

							case PLPGSQL_DTYPE_REC:
								{
									PLpgSQL_rec *rec = (PLpgSQL_rec *) retvar;

									if (recvar_tupdesc(rec) && estate->rsi && IsA(estate->rsi, ReturnSetInfo))
									{
										TupleDesc	rettupdesc = estate->rsi->expectedDesc;
										TupleConversionMap *tupmap ;

										tupmap = convert_tuples_by_position(recvar_tupdesc(rec), rettupdesc,
											 gettext_noop("returned record type does not match expected record type"));

										if (tupmap)
											free_conversion_map(tupmap);
									}
								}
								break;

							case PLPGSQL_DTYPE_ROW:
								{
									PLpgSQL_row *row = (PLpgSQL_row *) retvar;

									if (row->rowtupdesc && estate->rsi && IsA(estate->rsi, ReturnSetInfo))
									{
										TupleDesc	rettupdesc = estate->rsi->expectedDesc;
										TupleConversionMap *tupmap ;

										tupmap = convert_tuples_by_position(row->rowtupdesc, rettupdesc,
											 gettext_noop("returned record type does not match expected record type"));

										if (tupmap)
											free_conversion_map(tupmap);
									}
								}
								break;

							default:
								;		/* nope */
						}
					}

					*closing = PLPGSQL_CHECK_CLOSED;

					if (stmt_rt->expr)
						plpgsql_check_returned_expr(cstate, stmt_rt->expr, true);
				}
				break;

			case PLPGSQL_STMT_RETURN_NEXT:
				{
					PLpgSQL_stmt_return_next *stmt_rn = (PLpgSQL_stmt_return_next *) stmt;

					if (stmt_rn->retvarno >= 0)
					{
						PLpgSQL_datum *retvar = cstate->estate->datums[stmt_rn->retvarno];
						PLpgSQL_execstate *estate = cstate->estate;
						TupleDesc	tupdesc;
						int		natts;

						cstate->used_variables = bms_add_member(cstate->used_variables, stmt_rn->retvarno);

						if (!estate->retisset)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use RETURN NEXT in a non-SETOF function")));

#if PG_VERSION_NUM >= 110000

						tupdesc = estate->tuple_store_desc;

#else

						tupdesc = estate->rettupdesc;

#endif

						natts = tupdesc ? tupdesc->natts : 0;

						switch (retvar->dtype)
						{
							case PLPGSQL_DTYPE_VAR:
								{
									PLpgSQL_var *var = (PLpgSQL_var *) retvar;

									if (natts > 1)
										ereport(ERROR,
												(errcode(ERRCODE_DATATYPE_MISMATCH),
												 errmsg("wrong result type supplied in RETURN NEXT")));

									plpgsql_check_assign_to_target_type(cstate,
										 cstate->estate->func->fn_rettype, -1,
										 var->datatype->typoid, false);
								}
								break;

							case PLPGSQL_DTYPE_REC:
								{
									PLpgSQL_rec *rec = (PLpgSQL_rec *) retvar;
									TupleConversionMap *tupmap;

									if (!HeapTupleIsValid(recvar_tuple(rec)))
										ereport(ERROR,
												  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
												   errmsg("record \"%s\" is not assigned yet",
												   rec->refname),
										errdetail("The tuple structure of a not-yet-assigned"
															  " record is indeterminate.")));

									if (tupdesc)
									{
										tupmap = convert_tuples_by_position(recvar_tupdesc(rec),
																tupdesc,
											gettext_noop("wrong record type supplied in RETURN NEXT"));
										if (tupmap)
											free_conversion_map(tupmap);
									}
								}
								break;

							case PLPGSQL_DTYPE_ROW:
								{
									PLpgSQL_row *row = (PLpgSQL_row *) retvar;
									bool	row_is_valid_result;

									row_is_valid_result = true;

									if (tupdesc)
									{
										if (row->nfields == natts)
										{
											int		i;

											for (i = 0; i < natts; i++)
											{
												PLpgSQL_var *var;

												if (TupleDescAttr(tupdesc, i)->attisdropped)
													continue;
												if (row->varnos[i] < 0)
													elog(ERROR, "dropped rowtype entry for non-dropped column");

												var = (PLpgSQL_var *) (cstate->estate->datums[row->varnos[i]]);
												if (var->datatype->typoid != TupleDescAttr(tupdesc, i)->atttypid)
												{
													row_is_valid_result = false;
													break;
												}
											}
										}
										else
											row_is_valid_result = false;

										if (!row_is_valid_result)
											ereport(ERROR,
													(errcode(ERRCODE_DATATYPE_MISMATCH),
											errmsg("wrong record type supplied in RETURN NEXT")));
									}
								}
								break;

							default:
								;		/* nope */
						}
					}

					if (stmt_rn->expr)
						plpgsql_check_returned_expr(cstate, stmt_rn->expr, true);
				}
				break;

			case PLPGSQL_STMT_RETURN_QUERY:
				{
					PLpgSQL_stmt_return_query *stmt_rq = (PLpgSQL_stmt_return_query *) stmt;

					if (stmt_rq->query)
					{
						plpgsql_check_returned_expr(cstate, stmt_rq->query, false);
						cstate->found_return_query = true;
					}

					if (stmt_rq->dynquery)
					{
						check_dynamic_sql(cstate,
										  stmt,
										  stmt_rq->dynquery,
										  false,

#if PG_VERSION_NUM >= 110000

										  NULL,

#else

										  NULL, NULL,

#endif

										  stmt_rq->params);
						cstate->found_return_query = true;
					}
				}
				break;

			case PLPGSQL_STMT_RAISE:
				{
					PLpgSQL_stmt_raise *stmt_raise = (PLpgSQL_stmt_raise *) stmt;
					ListCell   *current_param;
					char	   *cp;
					int			err_code = 0;

					if (stmt_raise->condname != NULL)
						err_code = plpgsql_check__recognize_err_condition_p(stmt_raise->condname, true);

					foreach(l, stmt_raise->params)
					{
						plpgsql_check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}

					foreach(l, stmt_raise->options)
					{
						PLpgSQL_raise_option *opt = (PLpgSQL_raise_option *) lfirst(l);

						plpgsql_check_expr(cstate, opt->expr);

						if (opt->opt_type == PLPGSQL_RAISEOPTION_ERRCODE)
						{
							bool		isnull;
							char	   *value;

							value = plpgsql_check_expr_get_string(cstate, opt->expr, &isnull);

							if (value != NULL)
								err_code = plpgsql_check__recognize_err_condition_p(value, true);
							else
								err_code = -1;		/* cannot be calculated now */
						}
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

#if PG_VERSION_NUM >= 130000

								current_param = lnext(stmt_raise->params, current_param);

#else

								current_param = lnext(current_param);

#endif

							}
						}
					}
					if (current_param != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("too many parameters specified for RAISE")));

					if (stmt_raise->elog_level >= ERROR)
					{
						*closing = PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS;
						if (err_code == 0)
							err_code = ERRCODE_RAISE_EXCEPTION;
						else if (err_code == -1)
							err_code = 0; /* cannot be calculated */
						*exceptions = list_make1_int(err_code);
					}
					/* without any parameters it is reRAISE */
					if (stmt_raise->condname == NULL && stmt_raise->message == NULL &&
						stmt_raise->options == NIL)
					{
						*closing = PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS;
						/* should be enhanced in future */
						*exceptions = list_make1_int(-2); /* reRAISE */
					}
				}
				break;

			case PLPGSQL_STMT_EXECSQL:
				{
					PLpgSQL_stmt_execsql *stmt_execsql = (PLpgSQL_stmt_execsql *) stmt;

					if (stmt_execsql->into)
					{

#if PG_VERSION_NUM >= 110000

						check_variable(cstate, stmt_execsql->target);
						plpgsql_check_assignment_to_variable(cstate, stmt_execsql->sqlstmt,
													  stmt_execsql->target, -1);

#else

						plpgsql_check_row_or_rec(cstate, stmt_execsql->row, stmt_execsql->rec);
						plpgsql_check_assignment(cstate, stmt_execsql->sqlstmt,
								   stmt_execsql->rec, stmt_execsql->row, -1);

#endif

					}
					else
						/* only statement */
						plpgsql_check_expr_as_sqlstmt_nodata(cstate, stmt_execsql->sqlstmt);
				}
				break;

			case PLPGSQL_STMT_DYNEXECUTE:
				{
					PLpgSQL_stmt_dynexecute *stmt_dynexecute = (PLpgSQL_stmt_dynexecute *) stmt;

					check_dynamic_sql(cstate,
									  stmt,
									  stmt_dynexecute->query,
									  stmt_dynexecute->into,

#if PG_VERSION_NUM >= 110000

									  stmt_dynexecute->target,

#else

									  stmt_dynexecute->row,
									  stmt_dynexecute->rec,

#endif

									  stmt_dynexecute->params);
				}
				break;

			case PLPGSQL_STMT_OPEN:
				{
					PLpgSQL_stmt_open *stmt_open = (PLpgSQL_stmt_open *) stmt;
					PLpgSQL_var *var = (PLpgSQL_var *) (cstate->estate->datums[stmt_open->curvar]);

					plpgsql_check_expr_as_sqlstmt_data(cstate, var->cursor_explicit_expr);
					plpgsql_check_expr_as_sqlstmt_data(cstate, stmt_open->query);

					if (var != NULL && stmt_open->query != NULL)
						var->cursor_explicit_expr = stmt_open->query;

					plpgsql_check_expr_as_sqlstmt_data(cstate, stmt_open->argquery);

					plpgsql_check_expr(cstate, stmt_open->dynquery);

					foreach(l, stmt_open->params)
					{
						plpgsql_check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
					}

					cstate->modif_variables = bms_add_member(cstate->modif_variables,
									 stmt_open->curvar);
				}
				break;

			case PLPGSQL_STMT_GETDIAG:
				{
					PLpgSQL_stmt_getdiag *stmt_getdiag = (PLpgSQL_stmt_getdiag *) stmt;
					ListCell   *lc;

					foreach(lc, stmt_getdiag->diag_items)
					{
						PLpgSQL_diag_item *diag_item = (PLpgSQL_diag_item *) lfirst(lc);

						plpgsql_check_target(cstate, diag_item->target, NULL, NULL);
					}
				}
				break;

			case PLPGSQL_STMT_FETCH:
				{
					PLpgSQL_stmt_fetch *stmt_fetch = (PLpgSQL_stmt_fetch *) stmt;
					PLpgSQL_var *var = (PLpgSQL_var *) (cstate->estate->datums[stmt_fetch->curvar]);

#if PG_VERSION_NUM >= 110000

					check_variable(cstate, stmt_fetch->target);

					if (var != NULL && var->cursor_explicit_expr != NULL)
						plpgsql_check_assignment_to_variable(cstate, var->cursor_explicit_expr,
									   stmt_fetch->target, -1);

#else

					plpgsql_check_row_or_rec(cstate, stmt_fetch->row, stmt_fetch->rec);

					if (var != NULL && var->cursor_explicit_expr != NULL)
						plpgsql_check_assignment(cstate, var->cursor_explicit_expr,
									   stmt_fetch->rec, stmt_fetch->row, -1);

#endif

					plpgsql_check_expr(cstate, stmt_fetch->expr);

					cstate->used_variables = bms_add_member(cstate->used_variables, stmt_fetch->curvar);
				}
				break;

			case PLPGSQL_STMT_CLOSE:
				cstate->used_variables = bms_add_member(cstate->used_variables,
								 ((PLpgSQL_stmt_close *) stmt)->curvar);

				break;

#if PG_VERSION_NUM >= 110000

			case PLPGSQL_STMT_SET:
				/*
				 * We can not check this now, syntax should be ok.
				 * The expression there has not plan.
				 */
				break;

			case PLPGSQL_STMT_COMMIT:
			case PLPGSQL_STMT_ROLLBACK:
				/* These commands are allowed only in procedures */
				if (!cstate->cinfo->is_procedure)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION),
							 errmsg("invalid transaction termination")));
				break;

			case PLPGSQL_STMT_CALL:
				{
					PLpgSQL_stmt_call *stmt_call = (PLpgSQL_stmt_call *) stmt;
					PLpgSQL_row *target;
					bool		has_data;

					has_data = plpgsql_check_expr_as_sqlstmt(cstate, stmt_call->expr);

					/* any check_expr_xxx should be called before CallExprGetRowTarget */
					target = plpgsql_check_CallExprGetRowTarget(cstate, stmt_call->expr);

					if (has_data != (target != NULL))
						elog(ERROR, "plpgsql internal error, broken CALL statement");

					if (target != NULL)
					{
						check_variable(cstate, (PLpgSQL_variable *) target);
						plpgsql_check_assignment_to_variable(cstate, stmt_call->expr,
																(PLpgSQL_variable *) target, -1);

						pfree(target->varnos);
						pfree(target);
					}
				}
				break;

#endif

			default:
				elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
		}

		pop_stmt_from_stmt_stack(cstate);

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

		pop_stmt_from_stmt_stack(cstate);

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
 * Ensure check for all statements in list
 *
 */
static void
check_stmts(PLpgSQL_checkstate *cstate, List *stmts, int *closing, List **exceptions)
{
	ListCell   *lc;
	int			closing_local;
	List	   *exceptions_local;
	bool		dead_code_alert = false;

	*closing = PLPGSQL_CHECK_UNCLOSED;
	*exceptions = NIL;

	foreach(lc, stmts)
	{
		PLpgSQL_stmt	   *stmt = (PLpgSQL_stmt *) lfirst(lc);

		closing_local = PLPGSQL_CHECK_UNCLOSED;
		exceptions_local = NIL;
		plpgsql_check_stmt(cstate, stmt, &closing_local, &exceptions_local);

		/* raise dead_code_alert only for visible statements */
		if (dead_code_alert && stmt->lineno > 0)
		{
			plpgsql_check_put_error(cstate,
						  0, stmt->lineno,
						  "unreachable code",
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);
			/* don't raise this warning every line */
			dead_code_alert = false;
		}

		if (closing_local == PLPGSQL_CHECK_CLOSED)
		{
			dead_code_alert = true;
			*closing = PLPGSQL_CHECK_CLOSED;
			*exceptions = NIL;
		}
		else if (closing_local == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
		{
			dead_code_alert = true;
			if (*closing == PLPGSQL_CHECK_UNCLOSED ||
				*closing == PLPGSQL_CHECK_POSSIBLY_CLOSED ||
				*closing == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
			{
				*closing = PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS;
				*exceptions = exceptions_local;
			}
		}
		else if (closing_local == PLPGSQL_CHECK_POSSIBLY_CLOSED)
		{
			if (*closing == PLPGSQL_CHECK_UNCLOSED)
			{
				*closing = PLPGSQL_CHECK_POSSIBLY_CLOSED;
				*exceptions = NIL;
			}
		}
	}
}

/*
 * Add label to stack of labels
 */
static PLpgSQL_stmt_stack_item *
push_stmt_to_stmt_stack(PLpgSQL_checkstate *cstate)
{
	PLpgSQL_stmt *stmt = cstate->estate->err_stmt;
	PLpgSQL_stmt_stack_item *stmt_stack_item;
	PLpgSQL_stmt_stack_item *current = cstate->top_stmt_stack;

	stmt_stack_item = (PLpgSQL_stmt_stack_item *) palloc(sizeof(PLpgSQL_stmt_stack_item));
	stmt_stack_item->stmt = stmt;

	switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			stmt_stack_item->label = ((PLpgSQL_stmt_block *) stmt)->label;
			break;

		case PLPGSQL_STMT_EXIT:
			stmt_stack_item->label = ((PLpgSQL_stmt_exit *) stmt)->label;
			break;

		case PLPGSQL_STMT_LOOP:
			stmt_stack_item->label = ((PLpgSQL_stmt_loop *) stmt)->label;
			break;

		case PLPGSQL_STMT_WHILE:
			stmt_stack_item->label = ((PLpgSQL_stmt_while *) stmt)->label;
			break;

		case PLPGSQL_STMT_FORI:
			stmt_stack_item->label = ((PLpgSQL_stmt_fori *) stmt)->label;
			break;

		case PLPGSQL_STMT_FORS:
			stmt_stack_item->label = ((PLpgSQL_stmt_fors *) stmt)->label;
			break;

		case PLPGSQL_STMT_FORC:
			stmt_stack_item->label = ((PLpgSQL_stmt_forc *) stmt)->label;
			break;

		case PLPGSQL_STMT_DYNFORS:
			stmt_stack_item->label = ((PLpgSQL_stmt_dynfors *) stmt)->label;
			break;

		case PLPGSQL_STMT_FOREACH_A:
			stmt_stack_item->label = ((PLpgSQL_stmt_foreach_a *) stmt)->label;
			break;

		default:
			stmt_stack_item->label = NULL;
	}

	stmt_stack_item->outer = current;
	cstate->top_stmt_stack = stmt_stack_item;

	return current;
}

static void
pop_stmt_from_stmt_stack(PLpgSQL_checkstate *cstate)
{
	PLpgSQL_stmt_stack_item *current = cstate->top_stmt_stack;

	Assert(cstate->top_stmt_stack != NULL);

	cstate->top_stmt_stack = current->outer;
	pfree(current);
}

/*
 * Returns true, when stmt is any loop statement
 */
static bool
is_any_loop_stmt(PLpgSQL_stmt *stmt)
{
	switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
	{
		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_WHILE:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_FORS:
		case PLPGSQL_STMT_FORC:
		case PLPGSQL_STMT_DYNFORS:
		case PLPGSQL_STMT_FOREACH_A:
			return true;
		default:
			return false;
	}
}

/*
 * Searching a any statement related to CONTINUE/EXIT statement.
 * label cannot be NULL.
 */
static PLpgSQL_stmt *
find_stmt_with_label(char *label, PLpgSQL_stmt_stack_item *current)
{
	while (current != NULL)
	{
		if (current->label != NULL
				&& strcmp(current->label, label) == 0)
			return current->stmt;

		current = current->outer;
	}

	return NULL;
}

static PLpgSQL_stmt *
find_nearest_loop(PLpgSQL_stmt_stack_item *current)
{
	while (current != NULL)
	{
		if (is_any_loop_stmt(current->stmt))
			return current->stmt;

		current = current->outer;
	}

	return NULL;
}

/*
 * returns false, when a variable doesn't shadows any other variable
 */
static bool
found_shadowed_variable(char *varname, PLpgSQL_stmt_stack_item *current, PLpgSQL_checkstate *cstate)
{
	while (current != NULL)
	{
		if (current->stmt->cmd_type == PLPGSQL_STMT_BLOCK)
		{
			PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) current->stmt;
			int			i;
			PLpgSQL_datum *d;

			for (i = 0; i < stmt_block->n_initvars; i++)
			{
				char	   *refname;

				d = cstate->estate->func->datums[stmt_block->initvarnos[i]];
				refname = plpgsql_check_datum_get_refname(d);

				if (refname != NULL && strcmp(refname, varname) == 0)
					return true;
			}
		}

		current = current->outer;
	}

	return false;
}

/*
 * Reduce ending states of execution paths.
 *
 */
static int
possibly_closed(int c)
{
	switch (c)
	{
		case PLPGSQL_CHECK_CLOSED:
		case PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS:
		case PLPGSQL_CHECK_POSSIBLY_CLOSED:
			return PLPGSQL_CHECK_POSSIBLY_CLOSED;
		default:
			return PLPGSQL_CHECK_UNCLOSED;
	}
}

/*
 * Deduce ending state of execution paths.
 *
 */
static int
merge_closing(int c, int c_local, List **exceptions, List *exceptions_local, int err_code)
{
	*exceptions = NIL;

	if (c == PLPGSQL_CHECK_UNKNOWN)
	{
		if (c_local == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
			*exceptions = exceptions_local;

		return c_local;
	}

	if (c_local == PLPGSQL_CHECK_UNKNOWN)
		return c;

	if (c == c_local)
	{
		if (c == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
		{

			if (err_code != -1)
			{
				ListCell *lc;

				/* replace reRAISE symbol (-2) by real err_code */
				foreach(lc, exceptions_local)
				{
					int		t_err_code = lfirst_int(lc);

					*exceptions = list_append_unique_int(*exceptions,
														t_err_code != -2 ? t_err_code : err_code);
				}
			}
			else
				*exceptions = list_concat_unique_int(*exceptions, exceptions_local);
		}

		return c_local;
	}

	if (c == PLPGSQL_CHECK_CLOSED || c_local == PLPGSQL_CHECK_CLOSED)
	{
		if (c == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS ||
			c_local == PLPGSQL_CHECK_CLOSED_BY_EXCEPTIONS)
		return PLPGSQL_CHECK_CLOSED;
	}

	return PLPGSQL_CHECK_POSSIBLY_CLOSED;
}

/*
 * Returns true, if exception with sqlerrstate is handled.
 *
 */
static bool
exception_matches_conditions(int sqlerrstate, PLpgSQL_condition *cond)
{
	for (; cond != NULL; cond = cond->next)
	{
		int			_sqlerrstate = cond->sqlerrstate;

		/*
		 * OTHERS matches everything *except* query-canceled and
		 * assert-failure.  If you're foolish enough, you can match those
		 * explicitly.
		 */
		if (_sqlerrstate == 0)
		{
			if (sqlerrstate != ERRCODE_QUERY_CANCELED

#if PG_VERSION_NUM >= 90500

				 && sqlerrstate != ERRCODE_ASSERT_FAILURE

#endif

				)
				return true;
		}
		/* Exact match? */
		else if (sqlerrstate == _sqlerrstate)
			return true;
		/* Category match? */
		else if (ERRCODE_IS_CATEGORY(_sqlerrstate) &&
				 ERRCODE_TO_CATEGORY(sqlerrstate) == _sqlerrstate)
			return true;
	}
	return false;
}

/*
 * Dynamic SQL processing.
 *
 * When dynamic query is constant, we can do same work like with
 * static SQL.
 */

typedef struct
{
	List			   *args;
	PLpgSQL_checkstate *cstate;
	bool	use_params;
} DynSQLParams;

static Node *
dynsql_param_ref(ParseState *pstate, ParamRef *pref)
{
	DynSQLParams *params = (DynSQLParams *) pstate->p_ref_hook_state;
	List	   *args = params->args;
	int			nargs = list_length(args);
	Param	   *param = NULL;
	PLpgSQL_expr *expr;
	TupleDesc	tupdesc;

	if (pref->number < 1 || pref->number > nargs)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("there is no parameter $%d", pref->number),
				 parser_errposition(pstate, pref->location)));

	expr = (PLpgSQL_expr *) list_nth(args, pref->number - 1);

	tupdesc = plpgsql_check_expr_get_desc(params->cstate,
										  expr,
										  false,
										  false,
										  true,
										  NULL);

	if (tupdesc)
	{
		param = makeNode(Param);
		param->paramkind = PARAM_EXTERN;
		param->paramid = pref->number;
		param->paramtype = TupleDescAttr(tupdesc, 0)->atttypid;
		param->location = pref->location;

		/*
		 * SPI_execute_with_args doesn't allow pass typmod.
		 */
		param->paramtypmod = -1;
		param->paramcollid = InvalidOid;

		ReleaseTupleDesc(tupdesc);
	}
	else
		elog(ERROR, "cannot to detect type of $%d parameter", pref->number);

	params->use_params = true;

	return (Node *) param;
}

/*
 * Dynamic query requires own setup. In reality it is executed by
 * different SPI, here we need to emulate different environment.
 * Parameters are not mapped to function parameters, but to USING
 * clause expressions.
 */
static void
dynsql_parser_setup(struct ParseState *pstate, DynSQLParams *params)
{
	pstate->p_pre_columnref_hook = NULL;
	pstate->p_post_columnref_hook = NULL;
	pstate->p_paramref_hook = dynsql_param_ref;
	pstate->p_ref_hook_state = (void *) params;
}

/*
 * Returns true if record variable has assigned some type
 */
static bool
has_assigned_tupdesc(PLpgSQL_checkstate *cstate, PLpgSQL_rec *rec)
{
	PLpgSQL_rec *target = (PLpgSQL_rec *) (cstate->estate->datums[rec->dno]);

	Assert(rec->dtype == PLPGSQL_DTYPE_REC);

	if (recvar_tupdesc(target))
		return true;

	return false;
}

static void
check_dynamic_sql(PLpgSQL_checkstate *cstate,
				  PLpgSQL_stmt *stmt,
				  PLpgSQL_expr *query,
				  bool into,

#if PG_VERSION_NUM >= 110000

				  PLpgSQL_variable *target,

#else

				  PLpgSQL_row *row,
				  PLpgSQL_rec *rec,

#endif

				  List *params)
{
	bool		prev_has_execute_stmt = cstate->has_execute_stmt;
	Node	   *expr_node;
	ListCell   *l;
	int			loc = -1;
	volatile bool raise_unknown_rec_warning = false;

	/*
	 * possible checks:
	 *
	 * 1. When expression is string literal, then we can check this query similary
	 *    like cursor query with parameters. When this query has not a parameters,
	 *    and it is not DDL, DML, then we can raise a performance warning'.
	 *
	 * 2. When expression is real expression, then we should to check any string
	 *    kind parameters if are sanitized by functions quote_ident, qoute_literal,
	 *    or format.
	 */

	cstate->has_execute_stmt = true;

	foreach(l, params)
	{
		plpgsql_check_expr(cstate, (PLpgSQL_expr *) lfirst(l));
	}

	plpgsql_check_expr(cstate, query);
	expr_node = plpgsql_check_expr_get_node(cstate, query, false);

	if (IsA(expr_node, Const))
	{
		char *query = plpgsql_check_const_to_string((Const *) expr_node);
		PLpgSQL_expr		dynexpr;
		DynSQLParams		dsp;
		bool				is_mp;

		memset(&dynexpr, 0, sizeof(PLpgSQL_expr));

#if PG_VERSION_NUM < 110000

		dynexpr.dtype = PLPGSQL_DTYPE_EXPR;
		dynexpr.dno = -1;

#endif

#if PG_VERSION_NUM >= 90500

		dynexpr.rwparam = -1;

#endif

		dynexpr.query = query;

		dsp.args = params;
		dsp.cstate = cstate;
		dsp.use_params = false;

		PG_TRY();
		{
			cstate->allow_mp = true;

			plpgsql_check_expr_generic_with_parser_setup(cstate,
													 &dynexpr,
													 (ParserSetupHook) dynsql_parser_setup,
													 &dsp);

			is_mp = cstate->has_mp;
			cstate->has_mp = false;
		}
		PG_CATCH();
		{
			cstate->allow_mp = false;
			cstate->has_mp = false;

			PG_RE_THROW();
		}
		PG_END_TRY();

		if (!is_mp && (!params || !dsp.use_params))
		{

			/* probably useless dynamic command */
			plpgsql_check_put_error(cstate,
									0, 0,
									"immutable expression without parameters found",
									"the EXECUTE command is not necessary probably",
									"Don't use dynamic SQL when you can use static SQL.",
									PLPGSQL_CHECK_WARNING_PERFORMANCE,
									0, NULL, NULL);
		}

		if (params && !dsp.use_params)
		{
			plpgsql_check_put_error(cstate,
									0, 0,
						  "values passed to EXECUTE statement by USING clause was not used",
									NULL,
									NULL,
									PLPGSQL_CHECK_WARNING_OTHERS,
									0, NULL, NULL);
		}

		if (dynexpr.plan)
		{
			if (stmt->cmd_type == PLPGSQL_STMT_RETURN_QUERY)
			{
				plpgsql_check_returned_expr(cstate, &dynexpr, false);
				cstate->found_return_query = true;
			}
			else if (into)
			{

#if PG_VERSION_NUM >= 110000

				check_variable(cstate, target);
				plpgsql_check_assignment_to_variable(cstate, &dynexpr, target, -1);

#else

				plpgsql_check_row_or_rec(cstate, row, rec);
				plpgsql_check_assignment(cstate, &dynexpr, rec, row, -1);

#endif

			}

			SPI_freeplan(dynexpr.plan);
			cstate->exprs = list_delete_ptr(cstate->exprs, &dynexpr);
		}

		/* this is not real dynamic SQL statement */
		if (!is_mp)
			cstate->has_execute_stmt = prev_has_execute_stmt;
	}
	else
	{
		/*
		 * execute string is not constant (is not safe),
		 * but we can check sanitize parameters.
		 */
		if (cstate->cinfo->security_warnings &&
			plpgsql_check_is_sql_injection_vulnerable(cstate, query, expr_node, &loc))
		{
			if (loc != -1)
				plpgsql_check_put_error(cstate,
										0, 0,
							"text type variable is not sanitized",
							"The EXECUTE expression is SQL injection vulnerable.",
							"Use quote_ident, quote_literal or format function to secure variable.",
										PLPGSQL_CHECK_WARNING_SECURITY,
										loc,
										query->query,
										NULL);
			else
				plpgsql_check_put_error(cstate,
										0, 0,
							"the expression is not SQL injection safe",
							"Cannot ensure so dynamic EXECUTE statement is SQL injection secure.",
							"Use quote_ident, quote_literal or format function to secure variable.",
										PLPGSQL_CHECK_WARNING_SECURITY,
										-1,
										query->query,
										NULL);
		}

		/*
		 * In this case, we don't know a result type, and we should
		 * to raise warning about this situation.
		 */
		if (into)
		{

#if PG_VERSION_NUM >= 110000

			if (target->dtype == PLPGSQL_DTYPE_REC)
				raise_unknown_rec_warning = true;

#else

			if (rec)
				raise_unknown_rec_warning = true;

#endif

		}
	}

	/* recheck if target rec var has assigned tupdesc */
	if (into)
	{

#if PG_VERSION_NUM >= 110000

		check_variable(cstate, target);

		if (raise_unknown_rec_warning ||
			(target->dtype == PLPGSQL_DTYPE_REC &&
			 !has_assigned_tupdesc(cstate, (PLpgSQL_rec *) target)))

#else

		plpgsql_check_row_or_rec(cstate, row, rec);

		if (raise_unknown_rec_warning || (rec != NULL && !has_assigned_tupdesc(cstate, rec)))

#endif

		{
			plpgsql_check_put_error(cstate,
									0, 0,
									"cannot determinate a result of dynamic SQL",
									"There is a risk of related false alarms.",
						  "Don't use dynamic SQL and record type together, when you would check function.",
									PLPGSQL_CHECK_WARNING_OTHERS,
									0, NULL, NULL);
		}
	}
}
