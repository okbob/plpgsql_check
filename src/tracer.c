/*-------------------------------------------------------------------------
 *
 * tracer.c
 *
 *			  tracer related code
 *
 * by Pavel Stehule 2013-2020
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

bool plpgsql_check_enable_tracer = false;
bool plpgsql_check_tracer = false;
bool plpgsql_check_trace_assert = false;

/* the output is modified for regress tests */
bool plpgsql_check_tracer_test_mode = false;

PGErrorVerbosity plpgsql_check_tracer_verbosity = PGERROR_DEFAULT;
PGErrorVerbosity plpgsql_check_trace_assert_verbosity = PGERROR_DEFAULT;

int plpgsql_check_tracer_errlevel = NOTICE;
int plpgsql_check_tracer_variable_max_length = 1024;

/*
 * Convert binary value to text
 */
static char *
convert_value_to_string(PLpgSQL_execstate *estate, Datum value, Oid valtype)
{
	char	   *result;
	MemoryContext oldcontext;
	Oid			typoutput;
	bool		typIsVarlena;

	oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
	getTypeOutputInfo(valtype, &typoutput, &typIsVarlena);
	result = OidOutputFunctionCall(typoutput, value);
	MemoryContextSwitchTo(oldcontext);

	return result;
}

static char *
convert_plpgsql_datum_to_string(PLpgSQL_execstate *estate,
								PLpgSQL_datum *dtm,
								bool *isnull,
								char **refname)
{
	switch (dtm->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) dtm;

				*refname = var->refname;

				if (!var->isnull)
				{
					*isnull = false;

					return convert_value_to_string(estate,
												   var->value,
												   var->datatype->typoid);
				}
				else
				{
					*isnull = true;

					return NULL;
				}
			}

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) dtm;

				*refname = rec->refname;
				*isnull = false;

				return pstrdup("...");
			}

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) dtm;

				*refname = row->refname;
				*isnull = false;

				return pstrdup("...");
			}

		default:
			{
				*refname = NULL;
				*isnull = true;

				return NULL;
			}
	}
}

/*
 * Trim string value to n bytes
 */
static void
trim_string(char *str, int n)
{
	size_t l = strlen(str);

	if (l <= (size_t) n)
		return;

	if (pg_database_encoding_max_length() == 1)
	{
		str[n] = '\0';
		return;
	}

	while (n > 0)
	{
		int mbl = pg_mblen(str);

		if (mbl > n)
			break;
		str += mbl;
		n -= mbl;
	}

	*str = '\0';
}

/*
 * Print function's arguments
 */
static void
print_func_args(PLpgSQL_execstate *estate, PLpgSQL_function *func, int frame_num, int level)
{
	int		i;
	int indent = level * 2 + (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 0);
	int frame_width = plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 3;

	StringInfoData		ds;

	initStringInfo(&ds);

	/* print value of arguments */
	for (i = 0; i < func->fn_nargs; i++)
	{
		int			n = func->fn_argvarnos[i];
		bool		isnull;
		char	   *refname;
		char	   *str;

		str = convert_plpgsql_datum_to_string(estate,
											  estate->datums[n],
											  &isnull,
											  &refname);

		if (refname)
		{
			if (!isnull)
			{
				/*
				 * when this output is too long or contains new line, print it
				 * separately
				 */
				if (((int) strlen(str)) > plpgsql_check_tracer_variable_max_length ||
					strchr(str, '\n') != NULL)
				{
					if (*ds.data)
					{
						elog(plpgsql_check_tracer_errlevel,
							 "#%-*d%*s %s",
											frame_width,
											frame_num,
											indent + 4, "",
											ds.data);

						resetStringInfo(&ds);
					}

					trim_string(str, plpgsql_check_tracer_variable_max_length);
					elog(plpgsql_check_tracer_errlevel,
						 "#%-*d%*s \"%s\" => '%s'",
										frame_width,
										frame_num,
										indent + 4, "",
										refname,
										str);
				}
				else
				{
					if (*ds.data)
						appendStringInfoString(&ds, ", ");

					appendStringInfo(&ds, "\"%s\" => '%s'", refname, str);
				}
			}
			else
			{
				if (*ds.data)
					appendStringInfoString(&ds, ", ");

				appendStringInfo(&ds, "\"%s\" => null", refname);
			}
		}

		if (str)
			pfree(str);

		/*print too long lines immediately */
		if (ds.len > plpgsql_check_tracer_variable_max_length)
		{
			elog(plpgsql_check_tracer_errlevel,
				 "#%-*d%*s %s",
										frame_width,
										frame_num,
										indent + 4, "",
										ds.data);
			resetStringInfo(&ds);
		}
	}

	if (*ds.data)
		elog(plpgsql_check_tracer_errlevel,
			 "#%-*d%*s %s",
									frame_width,
									frame_num,
									indent + 4, "",
									ds.data);

	pfree(ds.data);
}

/*
 * Print expression's arguments
 */
static void
print_expr_args(PLpgSQL_execstate *estate,
				PLpgSQL_expr *expr,
				char *frame,
				int level)
{
	int		dno;
	int indent = level * 2 + (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 0);
	int frame_width = plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 3;

	StringInfoData		ds;

	initStringInfo(&ds);

	/*
	 * When expression hasn't assigned plan, then we cannot to show a related
	 * variables. So we can enforce creating plan.
	 */
	if (!expr->plan)
	{
		SPIPlanPtr		plan;

		expr->func = estate->func;

		/*
		 * Generate the plan (enforce expr query parsing) and throw plan 
		 */
		plan = SPI_prepare_params(expr->query,
								  (ParserSetupHook) plpgsql_check__parser_setup_p,
								  (void *) expr,
								  0);
		SPI_freeplan(plan);
	}

	/* print value of arguments */
	dno = -1;
	while ((dno = bms_next_member(expr->paramnos, dno)) >= 0)
	{
		bool		isnull;
		char	   *refname;
		char	   *str;

		str = convert_plpgsql_datum_to_string(estate,
											  estate->datums[dno],
											  &isnull,
											  &refname);

		if (refname)
		{
			if (!isnull)
			{
				/*
				 * when this output is too long or contains new line, print it
				 * separately
				 */
				if (((int) strlen(str)) > plpgsql_check_tracer_variable_max_length ||
					strchr(str, '\n') != NULL)
				{
					if (*ds.data)
					{
						elog(plpgsql_check_tracer_errlevel,
							 "#%-*s%*s %s",
											frame_width,
											frame,
											indent + 4, "",
											ds.data);

						resetStringInfo(&ds);
					}

					trim_string(str, plpgsql_check_tracer_variable_max_length);
					elog(plpgsql_check_tracer_errlevel,
						 "#%-*s%*s \"%s\" => '%s'",
										frame_width,
										frame,
										indent + 4, "",
										refname,
										str);
				}
				else
				{
					if (*ds.data)
						appendStringInfoString(&ds, ", ");

					appendStringInfo(&ds, "\"%s\" => '%s'", refname, str);
				}
			}
			else
			{
				if (*ds.data)
					appendStringInfoString(&ds, ", ");

				appendStringInfo(&ds, "\"%s\" => null", refname);
			}
		}

		if (str)
			pfree(str);

		/*print too long lines immediately */
		if (ds.len > plpgsql_check_tracer_variable_max_length)
		{
			elog(plpgsql_check_tracer_errlevel,
				 "#%-*s%*s %s",
										frame_width,
										frame,
										indent + 4, "",
										ds.data);
			resetStringInfo(&ds);
		}
	}

	if (*ds.data)
		elog(plpgsql_check_tracer_errlevel,
			 "#%-*s%*s %s",
									frame_width,
									frame,
									indent + 4, "",
									ds.data);

	pfree(ds.data);
}


/*
 * Print all frame variables
 */
static void
print_all_variables(PLpgSQL_execstate *estate)
{
	int		dno;

	StringInfoData		ds;
	bool		indent = 1;

	initStringInfo(&ds);

	for (dno = 0; dno < estate->ndatums; dno++)
	{
		bool		isnull;
		char	   *refname;
		char	   *str;

		if (dno == estate->found_varno)
			continue;

		str = convert_plpgsql_datum_to_string(estate,
											  estate->datums[dno],
											  &isnull,
											  &refname);

		if (strcmp(refname, "*internal*") == 0 ||
				strcmp(refname, "(unnamed row)") == 0)
			refname = NULL;

		if (refname)
		{
			if (!isnull)
			{
				/*
				 * when this output is too long or contains new line, print it
				 * separately
				 */
				if (((int) strlen(str)) > plpgsql_check_tracer_variable_max_length ||
					strchr(str, '\n') != NULL)
				{
					if (*ds.data)
					{
						elog(plpgsql_check_tracer_errlevel, "%*s%s", indent, "", ds.data);
						indent = 2;
						resetStringInfo(&ds);
					}

					trim_string(str, plpgsql_check_tracer_variable_max_length);
					elog(plpgsql_check_tracer_errlevel,
														"%*s \"%s\" => '%s'",
														indent, "",
														refname,
														str);
					indent = 2;
				}
				else
				{
					if (*ds.data)
						appendStringInfoString(&ds, ", ");

					appendStringInfo(&ds, "\"%s\" => '%s'", refname, str);
				}
			}
			else
			{
				if (*ds.data)
					appendStringInfoString(&ds, ", ");

				appendStringInfo(&ds, "\"%s\" => null", refname);
			}
		}

		if (str)
			pfree(str);

		/*print too long lines immediately */
		if (ds.len > plpgsql_check_tracer_variable_max_length)
		{
			elog(plpgsql_check_tracer_errlevel, "%*s%s", indent, "", ds.data);
			indent = 2;
			resetStringInfo(&ds);
		}
	}

	if (*ds.data)
		elog(plpgsql_check_tracer_errlevel, "%*s%s", indent, "", ds.data);

	pfree(ds.data);
}


/*
 * Print plpgsql datum
 */
static void
print_datum(PLpgSQL_execstate *estate, PLpgSQL_datum *dtm, char *frame, int level)
{
	int indent = level * 2 + (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 0);
	int frame_width = plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 3;

	bool		isnull;
	char	   *refname;
	char	   *str;

	str = convert_plpgsql_datum_to_string(estate,
											  dtm,
											  &isnull,
											  &refname);

	if (refname)
	{
		if (!isnull)
		{
			trim_string(str, plpgsql_check_tracer_variable_max_length);
			elog(plpgsql_check_tracer_errlevel,
				 "#%-*s%*s \"%s\" => '%s'",
										frame_width,
										frame,
										indent + 4, "",
										refname,
										str);
		}
		else
			elog(plpgsql_check_tracer_errlevel,
				 "#%-*s%*s \"%s\" => null",
										frame_width,
										frame,
										indent + 4, "",
										refname);
	}

	if (str)
		pfree(str);
}

/*
 * Tracer event routines
 */
void
plpgsql_check_tracer_on_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	PLpgSQL_execstate *outer_estate;
	int		frame_num;
	int		level;
	instr_time start_time;
	Oid		fn_oid;
	int		indent;
	int		frame_width;

	Assert(plpgsql_check_tracer);

	/* Allow tracing only when it is explicitly allowed */
	if (!plpgsql_check_enable_tracer)
		return;

	fn_oid = plpgsql_check_tracer_test_mode ? 0 : func->fn_oid;

	/*
	 * initialize plugin's near_outer_estate and level fields
	 * from stacked error contexts. Have to be called here.
	 */
	plpgsql_check_init_trace_info(estate);
	(void) plpgsql_check_get_trace_info(estate,
										&outer_estate,
										&frame_num,
										&level,
										&start_time);

	(void) start_time;

	indent = level * 2 + (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 0);
	frame_width = plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 3;

	if (plpgsql_check_tracer_verbosity >= PGERROR_DEFAULT)
		elog(plpgsql_check_tracer_errlevel,
			 "#%-*d%*s ->> start of %s%s (oid=%u)",
												  frame_width,
												  frame_num,
												  indent,
												  "",
												  func->fn_oid ? "function " : "block ",
												  func->fn_signature,
												  fn_oid);
	else
		elog(plpgsql_check_tracer_errlevel,
			 "#%-*d start of %s (oid=%u)",
												  frame_width,
												  frame_num,
												  func->fn_oid ? get_func_name(func->fn_oid) : "inline code block",
												  fn_oid);

	if (plpgsql_check_tracer_verbosity >= PGERROR_DEFAULT)
	{
		if (outer_estate)
		{
			if (outer_estate->err_stmt)
				elog(plpgsql_check_tracer_errlevel,
					 "#%-*d%*s  call by %s line %d at %s",
														frame_width,
														frame_num,
														indent + 4, "",
														outer_estate->func->fn_signature,
														outer_estate->err_stmt->lineno,
														plpgsql_check__stmt_typename_p(outer_estate->err_stmt));
			else
				elog(plpgsql_check_tracer_errlevel,
					 "#%-*d%*s  call by %s",
														frame_width,
														frame_num,
														indent + 4, "  ",
														outer_estate->func->fn_signature);
		}

		print_func_args(estate, func, frame_num, level);
	}
}

void
plpgsql_check_tracer_on_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	int		level;
	int		frame_num;
	instr_time start_time;
	PLpgSQL_execstate *outer_estate;

	Assert(plpgsql_check_tracer);

	/* Allow tracing only when it is explicitly allowed */
	if (!plpgsql_check_enable_tracer)
		return;

	if (plpgsql_check_get_trace_info(estate,
									 &outer_estate,
									 &frame_num,
									 &level,
									 &start_time))
	{
		instr_time		end_time;
		uint64			elapsed;
		int indent = level * 2 + (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 0);
		int frame_width = plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 3;

		INSTR_TIME_SET_CURRENT(end_time);
		INSTR_TIME_SUBTRACT(end_time, start_time);

		elapsed = INSTR_TIME_GET_MICROSEC(end_time);

		/* For output in regress tests use immutable time 0.010 ms */
		if (plpgsql_check_tracer_test_mode)
			elapsed = 10;

		if (plpgsql_check_tracer_verbosity >= PGERROR_DEFAULT)
		{
			if (func->fn_oid)
				elog(plpgsql_check_tracer_errlevel,
					 "#%-*d%*s <<- end of function %s (elapsed time=%.3f ms)",
															frame_width,
															frame_num,
															indent, "",
															get_func_name(func->fn_oid),
															elapsed / 1000.0);
			else
				elog(plpgsql_check_tracer_errlevel,
					 "#%-*d%*s <<- end of block (elapsed time=%.3f ms)",
															frame_width,
															frame_num,
															indent, "",
															elapsed / 1000.0);
		}
		else
			elog(plpgsql_check_tracer_errlevel,
				 "#%-3d end of %s",
								frame_num,
								func->fn_oid ? get_func_name(func->fn_oid) : "inline code block");
	}
}

static char *
copy_string_part(char *dest, char *src, int n)
{
	char *retval = dest;

	while (*src && n > 0)
	{
		int mbl = pg_mblen(src);

		memcpy(dest, src, mbl);
		src += mbl;
		dest += mbl;
		n -= mbl;
	}

	if (*src)
	{
		memcpy(dest, " ...", 3);
		dest += 3;
	}

	*dest = '\0';

	return retval;
}

void
plpgsql_check_tracer_on_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	Assert(plpgsql_check_tracer);

	/* don't trace invisible statements */
	if (stmt->cmd_type == PLPGSQL_STMT_BLOCK || stmt->lineno < 1)
		return;

	if (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE)
	{
		 PLpgSQL_execstate *outer_estate;
		 int		frame_num;
		 int		level;
		 instr_time start_time;

		if (plpgsql_check_get_trace_info(estate,
										 &outer_estate,
										 &frame_num,
										 &level,
										 &start_time))
		{
			int		indent = level * 2;
			int		frame_width = 6;
			char	printbuf[20];
			char	exprbuf[200];
			PLpgSQL_expr *expr = NULL;
			char   *exprname = NULL;
			int				retvarno = -1;

			switch (stmt->cmd_type)
			{
				case PLPGSQL_STMT_PERFORM:
					expr = ((PLpgSQL_stmt_perform *) stmt)->expr;
					exprname = "expr";
					break;

				case PLPGSQL_STMT_ASSIGN:
					expr = ((PLpgSQL_stmt_assign *) stmt)->expr;
					exprname = "expr";
					break;

				case PLPGSQL_STMT_RETURN:
					expr = ((PLpgSQL_stmt_return *) stmt)->expr;
					retvarno = ((PLpgSQL_stmt_return *) stmt)->retvarno;
					exprname = "expr";
					break;

				case PLPGSQL_STMT_ASSERT:
					expr = ((PLpgSQL_stmt_assert *) stmt)->cond;
					exprname = "expr";
					break;

#if PG_VERSION_NUM >= 110000

				case PLPGSQL_STMT_CALL:
					expr = ((PLpgSQL_stmt_call *) stmt)->expr;
					exprname = "expr";
					break;

#endif

				case PLPGSQL_STMT_EXECSQL:
					expr = ((PLpgSQL_stmt_execsql *) stmt)->sqlstmt;
					exprname = "query";
					break;

				case PLPGSQL_STMT_IF:
					expr = ((PLpgSQL_stmt_if *) stmt)->cond;
					exprname = "cond";

				default:
					;
			}

#if PG_VERSION_NUM >= 120000

			instr_time *stmt_start_time;

			plpgsql_check_get_trace_stmt_info(estate, stmt->stmtid, &stmt_start_time);

			if (stmt_start_time)
				INSTR_TIME_SET_CURRENT(*stmt_start_time);

			snprintf(printbuf, 20, "%d.%d", frame_num, stmt->stmtid);

#else

			snprintf(printbuf, 20, "%d", frame_num);

#endif

			if (expr)
			{
				int startpos = (strcmp(exprname, "query") == 0) ? 0 : 7;

				elog(plpgsql_check_tracer_errlevel,
					 "#%-*s %4d %*s --> start of %s (%s='%s')",
												frame_width, printbuf,
												stmt->lineno,
												indent, "",
												plpgsql_check__stmt_typename_p(stmt),
												exprname,
												copy_string_part(exprbuf, expr->query + startpos, 30));
			}
			else
				elog(plpgsql_check_tracer_errlevel,
					 "#%-*s %4d %*s --> start of %s",
												frame_width, printbuf,
												stmt->lineno,
												indent, "",
												plpgsql_check__stmt_typename_p(stmt));

			if (expr)
				print_expr_args(estate, expr, printbuf, level);

			if (retvarno >= 0)
				print_datum(estate, estate->datums[retvarno], printbuf, level);

			switch (stmt->cmd_type)
			{
				case PLPGSQL_STMT_IF:
					{
						PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;
						ListCell *lc;

						foreach (lc, stmt_if->elsif_list)
						{
							PLpgSQL_if_elsif *ifelseif = (PLpgSQL_if_elsif *) lfirst(lc);

							elog(plpgsql_check_tracer_errlevel,
								 "#%-*s %4d %*s     ELSEIF (expr='%s')",
												frame_width, printbuf,
												ifelseif->lineno,
												indent, "",
												copy_string_part(exprbuf, ifelseif->cond->query + 7, 30));

							print_expr_args(estate, ifelseif->cond, printbuf, level);
						}
						break;
					}

				default:
					;
			}

		}
	}
}

void
plpgsql_check_tracer_on_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	Assert(plpgsql_check_tracer);

	/* don't trace invisible statements */
	if (stmt->cmd_type == PLPGSQL_STMT_BLOCK || stmt->lineno < 1)
		return;

	if (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE)
	{
		 PLpgSQL_execstate *outer_estate;
		 int		frame_num;
		 int		level;
		 instr_time start_time;

		if (plpgsql_check_get_trace_info(estate,
										 &outer_estate,
										 &frame_num,
										 &level,
										 &start_time))
		{
			int		indent = level * 2;
			int		frame_width = 6;
			char	printbuf[20];
			uint64			elapsed = 0;

#if PG_VERSION_NUM >= 120000

			instr_time *stmt_start_time;

			plpgsql_check_get_trace_stmt_info(estate, stmt->stmtid, &stmt_start_time);

			if (stmt_start_time)
			{
				instr_time		end_time;

				INSTR_TIME_SET_CURRENT(end_time);
				INSTR_TIME_SUBTRACT(end_time, *stmt_start_time);

				elapsed = INSTR_TIME_GET_MICROSEC(end_time);
			}

			snprintf(printbuf, 20, "%d.%d", frame_num, stmt->stmtid);

#else

			snprintf(printbuf, 20, "%d", frame_num);

#endif

			elog(plpgsql_check_tracer_errlevel,
				 "#%-*s      %*s <-- end of %s (elapsed time=%.3f ms)",
													frame_width, printbuf,
													indent, "",
													plpgsql_check__stmt_typename_p(stmt),
													elapsed/1000.0);

			if (stmt->cmd_type == PLPGSQL_STMT_ASSIGN)
				print_datum(estate,
							estate->datums[((PLpgSQL_stmt_assign *) stmt)->varno],
							printbuf,
							level);
		}
	}
}

void
plpgsql_check_trace_assert_on_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	PLpgSQL_var result;
	PLpgSQL_type typ;
	char		exprbuf[200];
	PLpgSQL_stmt_assert *stmt_assert = (PLpgSQL_stmt_assert *) stmt;

	/* Allow tracing only when it is explicitly allowed */
	if (!plpgsql_check_enable_tracer)
		return;

	memset(&result, 0, sizeof(result));
	memset(&typ, 0, sizeof(typ));

	result.dtype = PLPGSQL_DTYPE_VAR;
	result.refname = "*auxstorage*";
	result.datatype = &typ;
	result.value = (Datum) 5;

	typ.typoid = BOOLOID;
	typ.ttype = PLPGSQL_TTYPE_SCALAR;
	typ.typlen = 1;
	typ.typbyval = true;
	typ.typtype = 'b';

	(*plpgsql_check_plugin_var_ptr)->assign_expr(estate, (PLpgSQL_datum *) &result, stmt_assert->cond);

	if ((bool) result.value)
	{
		if (plpgsql_check_trace_assert_verbosity >= PGERROR_DEFAULT)
			elog(plpgsql_check_tracer_errlevel, "PLpgSQL assert expression (%s) on line %d of %s is true",
												copy_string_part(exprbuf, stmt_assert->cond->query + 7, 30),
												stmt->lineno,
												estate->func->fn_signature);
	}
	else
	{
		ErrorContextCallback *econtext;
		int		frame_num = 0;

		for (econtext = error_context_stack->previous;
			 econtext != NULL;
			 econtext = econtext->previous)
			frame_num += 1;

		elog(plpgsql_check_tracer_errlevel, "#%d PLpgSQL assert expression (%s) on line %d of %s is false",
											frame_num,
											copy_string_part(exprbuf, stmt_assert->cond->query + 7, 30),
											stmt->lineno,
											estate->func->fn_signature);

		print_all_variables(estate);

		/* Show stack and all variables in verbose mode */
		if (plpgsql_check_trace_assert_verbosity >= PGERROR_DEFAULT)
		{
			for (econtext = error_context_stack->previous;
				 econtext != NULL;
				 econtext = econtext->previous)
			{
				frame_num -= 1;

				/*
				 * We detect PLpgSQL related estate by known error callback function.
				 * This is inspirated by PLDebugger.
				 */
				if (econtext->callback == (*plpgsql_check_plugin_var_ptr)->error_callback)
				{
					PLpgSQL_execstate *oestate = (PLpgSQL_execstate *) econtext->arg;

					if (oestate->err_stmt)
						elog(plpgsql_check_tracer_errlevel,
							 "#%d PL/pgSQL function %s line %d at %s",
															  frame_num,
															  oestate->func->fn_signature,
															  oestate->err_stmt->lineno,
															  plpgsql_check__stmt_typename_p(oestate->err_stmt));

					else
						elog(plpgsql_check_tracer_errlevel,
							 "#%d PLpgSQL function %s",
															  frame_num,
															  oestate->func->fn_signature);

					if (plpgsql_check_trace_assert_verbosity == PGERROR_VERBOSE)
						print_all_variables(oestate);
				}
			}
		}
	}
}
