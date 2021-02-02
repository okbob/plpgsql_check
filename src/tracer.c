/*-------------------------------------------------------------------------
 *
 * tracer.c
 *
 *			  tracer related code
 *
 * by Pavel Stehule 2013-2021
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

#if PG_VERSION_NUM < 110000

#include "access/htup_details.h"

#endif

bool plpgsql_check_enable_tracer = false;
bool plpgsql_check_tracer = false;
bool plpgsql_check_trace_assert = false;

/* the output is modified for regress tests */
bool plpgsql_check_tracer_test_mode = false;

PGErrorVerbosity plpgsql_check_tracer_verbosity = PGERROR_DEFAULT;
PGErrorVerbosity plpgsql_check_trace_assert_verbosity = PGERROR_DEFAULT;

int plpgsql_check_tracer_errlevel = NOTICE;
int plpgsql_check_tracer_variable_max_length = 1024;

static void print_datum(PLpgSQL_execstate *estate, PLpgSQL_datum *dtm, char *frame, int level);
static char *convert_plpgsql_datum_to_string(PLpgSQL_execstate *estate, PLpgSQL_datum *dtm, bool *isnull, char **refname);

#if PG_VERSION_NUM >= 140000

#define STREXPR_START		0

#else

#define STREXPR_START		7

#endif

#if PG_VERSION_NUM >= 120000

static void set_stmts_group_number(List *stmts, int *group_numbers, int *parent_group_numbers, int sgn, int *cgn, int spgn);

/*
 * sgn - statement group number
 * cgn - counter for group number
 * spgn - statemen parent group number
 */
void
plpgsql_check_set_stmt_group_number(PLpgSQL_stmt *stmt,
									int *group_numbers,
									int *parent_group_numbers,
									int sgn,
									int *cgn,
									int psgn)
{
	ListCell *lc;
	int			stmtid = stmt->stmtid - 1;

	group_numbers[stmtid] = sgn;
	parent_group_numbers[stmtid] = psgn;

	switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			{
				PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) stmt;

				set_stmts_group_number(stmt_block->body,
									  group_numbers,
									  parent_group_numbers,
									  ++(*cgn),
									  cgn,
									  sgn);

				if (stmt_block->exceptions)
				{
					foreach(lc, stmt_block->exceptions->exc_list)
					{
						set_stmts_group_number(
									  ((PLpgSQL_exception *) lfirst(lc))->action,
									  group_numbers,
									  parent_group_numbers,
									  ++(*cgn),
									  cgn,
									  sgn);
					}
				}
			}
			break;

		case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;

				set_stmts_group_number(stmt_if->then_body,
									  group_numbers,
									  parent_group_numbers,
									  ++(*cgn),
									  cgn,
									  sgn);

				foreach(lc, stmt_if->elsif_list)
				{
					set_stmts_group_number(((PLpgSQL_if_elsif *) lfirst(lc))->stmts,
										  group_numbers,
										  parent_group_numbers,
										  ++(*cgn),
										  cgn,
										  sgn);
				}

				set_stmts_group_number(stmt_if->else_body,
									  group_numbers,
									  parent_group_numbers,
									  ++(*cgn),
									  cgn,
									  sgn);
			}
			break;

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;

				foreach(lc, stmt_case->case_when_list)
				{
					set_stmts_group_number(((PLpgSQL_case_when *) lfirst(lc))->stmts,
										  group_numbers,
										  parent_group_numbers,
										  ++(*cgn),
										  cgn,
										  sgn);
				}

				set_stmts_group_number(stmt_case->else_stmts,
									  group_numbers,
									  parent_group_numbers,
									  ++(*cgn),
									  cgn,
									  sgn);
			}
			break;

		case PLPGSQL_STMT_LOOP:
			{
				set_stmts_group_number(((PLpgSQL_stmt_loop *) stmt)->body,
									   group_numbers,
									   parent_group_numbers,
									    ++(*cgn),
									    cgn,
									    sgn);
			}
			break;

		case PLPGSQL_STMT_FORI:
			{
				set_stmts_group_number(((PLpgSQL_stmt_fori *) stmt)->body,
									   group_numbers,
									   parent_group_numbers,
									    ++(*cgn),
									    cgn,
									    sgn);
			}
			break;

		case PLPGSQL_STMT_FORS:
			{
				set_stmts_group_number(((PLpgSQL_stmt_fors *) stmt)->body,
									   group_numbers,
									   parent_group_numbers,
									    ++(*cgn),
									    cgn,
									    sgn);
			}
			break;

		case PLPGSQL_STMT_FORC:
			{
				set_stmts_group_number(((PLpgSQL_stmt_forc *) stmt)->body,
									   group_numbers,
									   parent_group_numbers,
									    ++(*cgn),
									    cgn,
									    sgn);
			}
			break;

		case PLPGSQL_STMT_DYNFORS:
			{
				set_stmts_group_number(((PLpgSQL_stmt_dynfors *) stmt)->body,
									   group_numbers,
									   parent_group_numbers,
									    ++(*cgn),
									    cgn,
									    sgn);
			}
			break;

		case PLPGSQL_STMT_FOREACH_A:
			{
				set_stmts_group_number(((PLpgSQL_stmt_foreach_a *) stmt)->body,
									   group_numbers,
									   parent_group_numbers,
									    ++(*cgn),
									    cgn,
									    sgn);
			}
			break;

		case PLPGSQL_STMT_WHILE:
			{
				set_stmts_group_number(((PLpgSQL_stmt_while *) stmt)->body,
									   group_numbers,
									   parent_group_numbers,
									    ++(*cgn),
									    cgn,
									    sgn);
			}
			break;

		default:
			;
	}
}

static void
set_stmts_group_number(List *stmts,
					   int *group_numbers,
					   int *parent_group_numbers,
					   int sgn,
					   int *cgn,
					   int psgn)
{
	ListCell *lc;
	bool		is_first = true;

	foreach(lc, stmts)
	{
		plpgsql_check_set_stmt_group_number((PLpgSQL_stmt *) lfirst(lc),
							  group_numbers,
							  parent_group_numbers,
							  sgn,
							  cgn,
							  is_first ? psgn : -1);
		is_first = false;
	}
}

#endif

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

static void
StringInfoPrintRow(StringInfo ds, PLpgSQL_execstate *estate, PLpgSQL_row *row)
{
	bool		isfirst = true;
	int			i;
	bool		isnull;
	char	   *str;
	char	   *refname;

	appendStringInfoChar(ds, '(');

	for (i = 0; i < row->nfields; i++)
	{
		str = convert_plpgsql_datum_to_string(estate,
											  estate->datums[row->varnos[i]],
											  &isnull,
											  &refname);
		if (!isfirst)
			appendStringInfoChar(ds, ',');
		else
			isfirst = false;

		if (!isnull)
		{
			if (*str)
				appendStringInfoString(ds, str);
			else
				appendStringInfoString(ds, "\"\"");

			pfree(str);
		}
		else
			appendStringInfoString(ds, "");
	}

	appendStringInfoChar(ds, ')');
}

static char *
convert_plpgsql_datum_to_string(PLpgSQL_execstate *estate,
								PLpgSQL_datum *dtm,
								bool *isnull,
								char **refname)
{
	*isnull = true;
	*refname = NULL;

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
					return NULL;
			}
			break;

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) dtm;

				*refname = rec->refname;

#if PG_VERSION_NUM < 110000

				if (rec->tup && HeapTupleIsValid(rec->tup))
				{
					Datum		value;
					Oid			typid;
					MemoryContext oldcontext;

					Assert(rec->tupdesc);

					BlessTupleDesc(rec->tupdesc);

					*isnull = false;

					oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
					typid = rec->tupdesc->tdtypeid;
					value = heap_copy_tuple_as_datum(rec->tup, rec->tupdesc);
					MemoryContextSwitchTo(oldcontext);

					return convert_value_to_string(estate,
												   value,
												   typid);
				}
				else
					return NULL;

#else

				if (rec->erh && !ExpandedRecordIsEmpty(rec->erh))
				{
					*isnull = false;

					return convert_value_to_string(estate,
												   ExpandedRecordGetDatum(rec->erh),
												   rec->rectypeid);
				}
				else
					return NULL;

#endif

			}
			break;

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) dtm;
				StringInfoData ds;

				*isnull = false;

				*refname = row->refname;

				initStringInfo(&ds);

				StringInfoPrintRow(&ds, estate, row);

				return ds.data;
			}

		default:
			;
	}

	return NULL;
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

#if PG_VERSION_NUM < 110000

	if (func->fn_is_trigger == PLPGSQL_DML_TRIGGER)
	{
		const char *trgtyp;
		const char *trgtime;
		const char *trgcmd;
		int		rec_new_varno = func->new_varno;
		int		rec_old_varno = func->old_varno;
		char buffer[20];
		PLpgSQL_var *var;
		char *str;

		var = (PLpgSQL_var *) estate->datums[func->tg_when_varno];
		Assert(!var->isnull);
		str = TextDatumGetCString(var->value);
		trgtime = strcmp(str, "BEFORE") == 0 ? "before" : "after";
		pfree(str);

		var = (PLpgSQL_var *) estate->datums[func->tg_level_varno];
		Assert(!var->isnull);
		str = TextDatumGetCString(var->value);
		trgtyp = strcmp(str, "ROW") == 0 ? "row" : "statement";
		pfree(str);

		var = (PLpgSQL_var *) estate->datums[func->tg_op_varno];
		Assert(!var->isnull);
		str = TextDatumGetCString(var->value);

		if (strcmp(str, "INSERT") == 0)
		{
			trgcmd = " insert";
			rec_old_varno = -1;
		}
		else if (strcmp(str, "UPDATE") == 0)
		{
			trgcmd = " update";
		}
		else if (strcmp(str, "DELETE") == 0)
		{
			trgcmd = " delete";
			rec_new_varno = -1;
		}
		else
			trgcmd = "";

		pfree(str);

		elog(plpgsql_check_tracer_errlevel,
							 "#%-*d%*s triggered by %s %s%s trigger",
											frame_width,
											frame_num,
											indent + 4, "",
											trgtime,
											trgtyp,
											trgcmd);

		sprintf(buffer, "%d", frame_num);

		if (rec_new_varno != -1)
			print_datum(estate, estate->datums[rec_new_varno], buffer, level);
		if (rec_old_varno != -1)
			print_datum(estate, estate->datums[rec_new_varno], buffer, level);
	}
	else if (func->fn_is_trigger == PLPGSQL_EVENT_TRIGGER)
	{
		elog(plpgsql_check_tracer_errlevel,
							 "#%-*d%*s triggered by event trigger",
											frame_width,
											frame_num,
											indent + 4, "");
	}

#else

	if (func->fn_is_trigger == PLPGSQL_DML_TRIGGER)
	{
		Assert(estate->trigdata);

		TriggerData *td = estate->trigdata;
		const char *trgtyp;
		const char *trgtime;
		const char *trgcmd;
		int		rec_new_varno = func->new_varno;
		int		rec_old_varno = func->old_varno;
		char buffer[20];

		trgtyp = TRIGGER_FIRED_FOR_ROW(td->tg_event) ? "row" : "statement";
		trgtime = TRIGGER_FIRED_BEFORE(td->tg_event) ? "before" : "after";

		if (TRIGGER_FIRED_BY_INSERT(td->tg_event))
		{
			trgcmd = " insert";
			rec_old_varno = -1;
		}
		else if (TRIGGER_FIRED_BY_UPDATE(td->tg_event))
		{
			trgcmd = " update";
		}
		else if (TRIGGER_FIRED_BY_DELETE(td->tg_event))
		{
			trgcmd = " delete";
			rec_new_varno = -1;
		}
		else
		{
			trgcmd = "";
		}

		elog(plpgsql_check_tracer_errlevel,
							 "#%-*d%*s triggered by %s %s%s trigger",
											frame_width,
											frame_num,
											indent + 4, "",
											trgtime,
											trgtyp,
											trgcmd);

		sprintf(buffer, "%d", frame_num);

		if (rec_new_varno != -1)
			print_datum(estate, estate->datums[rec_new_varno], buffer, level);
		if (rec_old_varno != -1)
			print_datum(estate, estate->datums[rec_new_varno], buffer, level);
	}

	if (func->fn_is_trigger == PLPGSQL_EVENT_TRIGGER)
	{
		Assert(estate->evtrigdata);

		elog(plpgsql_check_tracer_errlevel,
							 "#%-*d%*s triggered by event trigger",
											frame_width,
											frame_num,
											indent + 4, "");
	}

#endif

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

#if PG_VERSION_NUM >= 140000

		SPIPrepareOptions options;

		memset(&options, 0, sizeof(options));
		options.parserSetup = (ParserSetupHook) plpgsql_check__parser_setup_p;
		options.parserSetupArg = (void *) expr;
		options.parseMode = expr->parseMode;
		options.cursorOptions = 0;

#endif

		expr->func = estate->func;

#if PG_VERSION_NUM >= 140000

		/*
		 * Generate and save the plan
		 */
		plan = SPI_prepare_extended(expr->query, &options);

#else

		/*
		 * Generate the plan (enforce expr query parsing) and throw plan 
		 */
		plan = SPI_prepare_params(expr->query,
								  (ParserSetupHook) plpgsql_check__parser_setup_p,
								  (void *) expr,
								  0);

#endif

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
 * Print expression's arguments
 */
static void
print_assert_args(PLpgSQL_execstate *estate, PLpgSQL_stmt_assert *stmt)
{
	int		dno;

	StringInfoData		ds;

	initStringInfo(&ds);

	/*
	 * When expression hasn't assigned plan, then we cannot to show a related
	 * variables. So we can enforce creating plan.
	 */
	if (!stmt->cond->plan)
	{
		SPIPlanPtr		plan;

#if PG_VERSION_NUM >= 140000

		SPIPrepareOptions options;

		memset(&options, 0, sizeof(options));
		options.parserSetup = (ParserSetupHook) plpgsql_check__parser_setup_p;
		options.parserSetupArg = (void *) stmt->cond;
		options.parseMode = stmt->cond->parseMode;
		options.cursorOptions = 0;

#endif

		stmt->cond->func = estate->func;

#if PG_VERSION_NUM >= 140000

		/*
		 * Generate and save the plan
		 */
		plan = SPI_prepare_extended((void *) stmt->cond->query, &options);

#else

		/*
		 * Generate the plan (enforce expr query parsing) and throw plan
		 */
		plan = SPI_prepare_params(stmt->cond->query,
								  (ParserSetupHook) plpgsql_check__parser_setup_p,
								  (void *) stmt->cond,
								  0);

#endif

		SPI_freeplan(plan);
	}

	/* print value of arguments */
	dno = -1;
	while ((dno = bms_next_member(stmt->cond->paramnos, dno)) >= 0)
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
						elog(plpgsql_check_tracer_errlevel, " %s", ds.data);

						resetStringInfo(&ds);
					}

					trim_string(str, plpgsql_check_tracer_variable_max_length);
					elog(plpgsql_check_tracer_errlevel,
						 " \"%s\" => '%s'",
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
			elog(plpgsql_check_tracer_errlevel, " %s", ds.data);
			resetStringInfo(&ds);
		}
	}

	if (*ds.data)
		elog(plpgsql_check_tracer_errlevel, " %s", ds.data);

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
	if (plpgsql_check_get_trace_info(estate,
										NULL,
										&outer_estate,
										&frame_num,
										&level,
										&start_time))
	{
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
									 NULL,
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
										 stmt,
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
			bool	is_assignment = false;
			bool	is_perform = false;

			switch (stmt->cmd_type)
			{
				case PLPGSQL_STMT_PERFORM:
					expr = ((PLpgSQL_stmt_perform *) stmt)->expr;
					exprname = "perform";
					is_perform = true;
					break;

				case PLPGSQL_STMT_ASSIGN:
					{
						PLpgSQL_stmt_assign	*stmt_assign = (PLpgSQL_stmt_assign *) stmt;

#if PG_VERSION_NUM >= 140000

						PLpgSQL_datum	   *target = estate->datums[stmt_assign->varno];

						expr = stmt_assign->expr;

						if (target->dtype == PLPGSQL_DTYPE_VAR)
							expr->target_param = target->dno;
						else
							expr->target_param = -1;

#else

						expr = stmt_assign->expr;

#endif

						exprname = "expr";
						is_assignment = true;
					}
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

			plpgsql_check_get_trace_stmt_info(estate, stmt->stmtid - 1, &stmt_start_time);

			if (stmt_start_time)
				INSTR_TIME_SET_CURRENT(*stmt_start_time);

			snprintf(printbuf, 20, "%d.%d", frame_num, stmt->stmtid);

#else

			snprintf(printbuf, 20, "%d", frame_num);

#endif

			if (expr)
			{
				int startpos;

				if (strcmp(exprname, "perform") == 0)
				{
					startpos = 7;
					exprname = "expr";
				}
				else if (strcmp(exprname, "query") == 0)
					startpos = 0;
				else
					startpos = STREXPR_START;

				if (is_assignment)
				{
					elog(plpgsql_check_tracer_errlevel,
						 "#%-*s %4d %*s --> start of assignment %s",
													frame_width, printbuf,
													stmt->lineno,
													indent, "",
													copy_string_part(exprbuf, expr->query + startpos, 30));
				}
				else if (is_perform)
				{
					elog(plpgsql_check_tracer_errlevel,
						 "#%-*s %4d %*s --> start of perform %s",
													frame_width, printbuf,
													stmt->lineno,
													indent, "",
													copy_string_part(exprbuf, expr->query + startpos, 30));
				}
				else
				{
					elog(plpgsql_check_tracer_errlevel,
						 "#%-*s %4d %*s --> start of %s (%s='%s')",
													frame_width, printbuf,
													stmt->lineno,
													indent, "",
													plpgsql_check__stmt_typename_p(stmt),
													exprname,
													copy_string_part(exprbuf, expr->query + startpos, 30));
				}
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
												copy_string_part(exprbuf, ifelseif->cond->query + STREXPR_START, 30));

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
										 stmt,
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

			plpgsql_check_get_trace_stmt_info(estate, stmt->stmtid - 1, &stmt_start_time);

			if (stmt_start_time)
			{
				instr_time		end_time;

				INSTR_TIME_SET_CURRENT(end_time);
				INSTR_TIME_SUBTRACT(end_time, *stmt_start_time);

				elapsed = INSTR_TIME_GET_MICROSEC(end_time);
				if (plpgsql_check_tracer_test_mode)
					elapsed = 10;
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
		{
			elog(plpgsql_check_tracer_errlevel, "PLpgSQL assert expression (%s) on line %d of %s is true",
												copy_string_part(exprbuf, stmt_assert->cond->query + STREXPR_START, 30),
												stmt->lineno,
												estate->func->fn_signature);

			print_assert_args(estate, stmt_assert);
		}
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
											copy_string_part(exprbuf, stmt_assert->cond->query + STREXPR_START, 30),
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
