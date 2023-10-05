/*-------------------------------------------------------------------------
 *
 * tracer.c
 *
 *			  tracer related code
 *
 * by Pavel Stehule 2013-2023
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/guc.h"
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

static void print_datum(PLpgSQL_execstate *estate, PLpgSQL_datum *dtm, char *frame, int level);
static char *convert_plpgsql_datum_to_string(PLpgSQL_execstate *estate, PLpgSQL_datum *dtm, bool *isnull, char **refname);


PG_FUNCTION_INFO_V1(plpgsql_check_tracer_ctrl);

#if PG_VERSION_NUM >= 140000

#define STREXPR_START		0

#else

#define STREXPR_START		7

#endif

/*
 * This structure is used as pldbgapi2 extension parameter
 */
typedef struct tracer_info
{
	Oid			fn_oid;
	int			frame_num;
	char	   *fn_name;
	char	   *fn_signature;
	instr_time  start_time;
	instr_time *stmts_start_time;
	bool	   *stmts_tracer_state;

	/* true when function is traced from func_beg */
	bool		is_traced;
} tracer_info;

static void trace_assert(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, tracer_info *tinfo);


//static void set_stmts_group_number(List *stmts, int *group_numbers, int *parent_group_numbers, int sgn, int *cgn, int spgn);

static void tracer_func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info);
static void tracer_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info);
static void tracer_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info);
static void tracer_func_end_aborted(Oid fn_oid, void **plugin2_info);
static void tracer_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, void **plugin2_info);
static void tracer_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, void **plugin2_info);
static void tracer_stmt_end_aborted(Oid fn_oid, int stmtid, void **plugin2_info);

static plpgsql_check_plugin2 tracer_plugin2 = { tracer_func_setup,
												tracer_func_beg, tracer_func_end, tracer_func_end_aborted,
												tracer_stmt_beg, tracer_stmt_end, tracer_stmt_end_aborted,
												NULL, NULL, NULL, NULL, NULL };

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
	char	   *refname;

	appendStringInfoChar(ds, '(');

	for (i = 0; i < row->nfields; i++)
	{
		char	   *str;

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

				if (rec->erh && !ExpandedRecordIsEmpty(rec->erh))
				{
					*isnull = false;

					return convert_value_to_string(estate,
												   ExpandedRecordGetDatum(rec->erh),
												   rec->rectypeid);
				}
				else
					return NULL;
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
	StringInfoData ds;

	initStringInfo(&ds);

	if (func->fn_is_trigger == PLPGSQL_DML_TRIGGER)
	{
		TriggerData *td = estate->trigdata;
		const char *trgtyp;
		const char *trgtime;
		const char *trgcmd;
		int		rec_new_varno = func->new_varno;
		int		rec_old_varno = func->old_varno;
		char buffer[20];

		Assert(estate->trigdata);

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
	int			dno;
	int			indent = level * 2 + (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 0);
	int			frame_width = plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 3;
	StringInfoData ds;

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
	int			dno;
	StringInfoData ds;

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
	int			dno;
	StringInfoData ds;
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

static void
tracer_func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info)
{
	tracer_info *tinfo = NULL;

	if (plpgsql_check_enable_tracer)
	{
		tinfo = palloc0(sizeof(tracer_info));

		tinfo->stmts_start_time = palloc0(sizeof(instr_time) * func->nstatements);
		tinfo->stmts_tracer_state = palloc(sizeof(bool) * func->nstatements);

		tinfo->fn_oid = func->fn_oid;

		tinfo->fn_name = plpgsql_check_get_current_func_info_name();
		tinfo->fn_signature = plpgsql_check_get_current_func_info_signature();

		INSTR_TIME_SET_CURRENT(tinfo->start_time);
	}

	*plugin2_info = tinfo;
}

/*
 * get_caller_estate - try to returns near outer estate
 */
static void
get_outer_info(char **errcontextstr, int *frame_num)
{
	ErrorContextCallback *econtext;
	MemoryContext oldcxt = CurrentMemoryContext;

	*errcontextstr = NULL;
	*frame_num = 0;

	for (econtext = error_context_stack->previous;
		 econtext != NULL;
		 econtext = econtext->previous)
	{
		*frame_num += 1;
	}

	if (plpgsql_check_tracer_verbosity >= PGERROR_DEFAULT &&
		error_context_stack->previous)
	{
		ErrorData *edata;

		econtext = error_context_stack->previous;

#if PG_VERSION_NUM >= 130000

		errstart(ERROR, TEXTDOMAIN);

#else

		errstart(ERROR, __FILE__, __LINE__, PG_FUNCNAME_MACRO, TEXTDOMAIN);

#endif

		MemoryContextSwitchTo(oldcxt);

		(*econtext->callback)(econtext->arg);

		edata = CopyErrorData();
		FlushErrorState();

		if (edata->context)
			*errcontextstr = edata->context;

		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * Tracer event routines
 */
static void
tracer_func_beg(PLpgSQL_execstate *estate,
				PLpgSQL_function *func,
				void **plugin2_info)
{
	tracer_info *tinfo = *plugin2_info;
	char	   *caller_errcontext = NULL;
	Oid			fn_oid;
	int			indent;
	int			frame_width;

	if (!tinfo)
		return;

	fn_oid = plpgsql_check_tracer_test_mode ? 0 : func->fn_oid;
	get_outer_info(&caller_errcontext, &tinfo->frame_num);

	if (!plpgsql_check_tracer)
		return;

	indent = tinfo->frame_num * 2 + (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 0);
	frame_width = plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 3;

	if (plpgsql_check_tracer_verbosity >= PGERROR_DEFAULT)
		elog(plpgsql_check_tracer_errlevel,
			 "#%-*d%*s ->> start of %s%s (oid=%u)",
												  frame_width,
												  tinfo->frame_num,
												  indent,
												  "",
												  func->fn_oid ? "function " : "block ",
												  func->fn_signature,
												  fn_oid);
	else
		elog(plpgsql_check_tracer_errlevel,
			 "#%-*d start of %s (oid=%u)",
												  frame_width,
												  tinfo->frame_num,
												  func->fn_oid ? get_func_name(func->fn_oid) : "inline code block",
												  fn_oid);

	if (plpgsql_check_tracer_verbosity >= PGERROR_DEFAULT)
	{
		if (caller_errcontext)
		{
			elog(plpgsql_check_tracer_errlevel,
				 "#%-*d%*s context: %s",
													frame_width,
													tinfo->frame_num,
													indent + 4, "  ",
													caller_errcontext);
			pfree(caller_errcontext);
		}

		print_func_args(estate, func, tinfo->frame_num, tinfo->frame_num);
	}

	tinfo->is_traced = true;
}

/*
 * workhorse for tracer_func_end and tracer_func_end_aborted
 */
static void
_tracer_func_end(tracer_info *tinfo, bool is_aborted)
{
	instr_time	end_time;
	uint64		elapsed;
	int			indent;
	int			frame_width;
	const char *aborted;

	Assert(tinfo);

	if (!tinfo->is_traced || !plpgsql_check_tracer)
		return;

	indent = tinfo->frame_num * 2 + (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 0);
	frame_width = plpgsql_check_tracer_verbosity == PGERROR_VERBOSE ? 6 : 3;

	aborted = is_aborted ? " aborted" : "";

	INSTR_TIME_SET_CURRENT(end_time);
	INSTR_TIME_SUBTRACT(end_time, tinfo->start_time);

	elapsed = INSTR_TIME_GET_MICROSEC(end_time);

	/* For output in regress tests use immutable time 0.010 ms */
	if (plpgsql_check_tracer_test_mode)
		elapsed = 10;

	if (plpgsql_check_tracer_verbosity >= PGERROR_DEFAULT)
	{
		if (OidIsValid(tinfo->fn_oid))
			elog(plpgsql_check_tracer_errlevel,
				 "#%-*d%*s <<- end of function %s (elapsed time=%.3f ms)%s",
														frame_width,
														tinfo->frame_num,
														indent, "",
														tinfo->fn_name,
														elapsed / 1000.0,
														aborted);
		else
			elog(plpgsql_check_tracer_errlevel,
				 "#%-*d%*s <<- end of block (elapsed time=%.3f ms)%s",
														frame_width,
														tinfo->frame_num,
														indent, "",
														elapsed / 1000.0,
														aborted);
	}
	else
		elog(plpgsql_check_tracer_errlevel,
			 "#%-3d end of %s%s",
							tinfo->frame_num,
							tinfo->fn_oid ? tinfo->fn_name : "inline code block",
							aborted);
}

static void
tracer_func_end(PLpgSQL_execstate *estate,
				PLpgSQL_function *func,
				void **plugin2_info)
{
	tracer_info *tinfo = *plugin2_info;

	if (!tinfo)
		return;

	Assert(tinfo->fn_oid == func->fn_oid);

	_tracer_func_end(tinfo, false);
}

static void
tracer_func_end_aborted(Oid fn_oid, void **plugin2_info)
{
	tracer_info *tinfo = *plugin2_info;

	if (!tinfo)
		return;

	Assert(tinfo->fn_oid == fn_oid);

	_tracer_func_end(tinfo, true);
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

static void
tracer_stmt_beg(PLpgSQL_execstate *estate,
				PLpgSQL_stmt *stmt,
				void **plugin2_info)
{
	tracer_info *tinfo = *plugin2_info;
	plpgsql_check_plugin2_stmt_info *sinfo;
	int			total_level;

	if (!tinfo)
		return;

	sinfo = plpgsql_check_get_current_stmt_info(stmt->stmtid);

	/* save current tracer state (enabled | disabled) */
	tinfo->stmts_tracer_state[stmt->stmtid - 1] = plpgsql_check_tracer;

	/* don't trace invisible statements */
	if (sinfo->is_invisible || !plpgsql_check_tracer)
		return;

	if (stmt->cmd_type == PLPGSQL_STMT_ASSERT && plpgsql_check_trace_assert)
		trace_assert(estate, stmt, tinfo);

	total_level = tinfo->frame_num + sinfo->level;

	if (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE)
	{
		int		indent = (tinfo->frame_num + sinfo->level) * 2;
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

			case PLPGSQL_STMT_CALL:
				expr = ((PLpgSQL_stmt_call *) stmt)->expr;
				exprname = "expr";
				break;

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

		INSTR_TIME_SET_CURRENT(tinfo->stmts_start_time[stmt->stmtid - 1]);

		snprintf(printbuf, 20, "%d.%d", tinfo->frame_num, sinfo->natural_id);

		if (expr)
		{
			int startpos;

#if PG_VERSION_NUM >= 140000

			if (strcmp(exprname, "perform") == 0)
			{
				startpos = 7;
				exprname = "expr";
			}
			else
				startpos = 0;

#else

			if (strcmp(exprname, "perform") == 0)
			{
				startpos = 7;
				exprname = "expr";
			}
			else if (strcmp(exprname, "query") == 0)
				startpos = 0;
			else
				startpos = STREXPR_START;

#endif

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
			print_expr_args(estate, expr, printbuf, total_level);

		if (retvarno >= 0)
			print_datum(estate, estate->datums[retvarno], printbuf, total_level);

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

						print_expr_args(estate, ifelseif->cond, printbuf, total_level);
					}
					break;
				}

			default:
				;
		}
	}
}

static void
_tracer_stmt_end(tracer_info *tinfo,
				 plpgsql_check_plugin2_stmt_info *sinfo,
				 int stmtid,
				 bool is_aborted)
{
	const char *aborted = is_aborted ? " aborted" : "";

	Assert(tinfo);
	Assert(sinfo);

	/* don't trace invisible statements */
	if (sinfo->is_invisible)
	{
		if (sinfo->is_container)
			/* restore tracer state (enabled | disabled) */
			plpgsql_check_tracer = tinfo->stmts_tracer_state[stmtid - 1];

		return;
	}

	if (tinfo->stmts_tracer_state[stmtid - 1] && 
		plpgsql_check_tracer_verbosity == PGERROR_VERBOSE)
	{
		int		indent = (tinfo->frame_num + sinfo->level) * 2;
		int		frame_width = 6;
		char	printbuf[20];
		uint64			elapsed = 0;

		if (!INSTR_TIME_IS_ZERO(tinfo->stmts_start_time[stmtid - 1]))
		{
			instr_time		end_time;

			INSTR_TIME_SET_CURRENT(end_time);
			INSTR_TIME_SUBTRACT(end_time, tinfo->stmts_start_time[stmtid - 1]);

			elapsed = INSTR_TIME_GET_MICROSEC(end_time);
			if (plpgsql_check_tracer_test_mode)
				elapsed = 10;
		}

		snprintf(printbuf, 20, "%d.%d", tinfo->frame_num, stmtid);

		elog(plpgsql_check_tracer_errlevel,
			 "#%-*s      %*s <-- end of %s (elapsed time=%.3f ms)%s",
												frame_width, printbuf,
												indent, "",
												sinfo->typname,
												elapsed/1000.0,
												aborted);
	}

	if (sinfo->is_container)
	{
		/* restore tracer state (enabled | disabled) */
		plpgsql_check_tracer = tinfo->stmts_tracer_state[stmtid - 1];
	}

}


static void
tracer_stmt_end(PLpgSQL_execstate *estate,
				PLpgSQL_stmt *stmt,
				void **plugin2_info)
{
	tracer_info *tinfo = *plugin2_info;
	plpgsql_check_plugin2_stmt_info *sinfo;

	if (!tinfo)
		return;

	sinfo = plpgsql_check_get_current_stmt_info(stmt->stmtid);

	_tracer_stmt_end(tinfo, sinfo, stmt->stmtid, false);

	if (!plpgsql_check_tracer)
		return;

	if (plpgsql_check_tracer_verbosity == PGERROR_VERBOSE &&
		stmt->cmd_type == PLPGSQL_STMT_ASSIGN &&
		!sinfo->is_invisible)
	{
		char	printbuf[20];

		snprintf(printbuf, 20, "%d.%d", tinfo->frame_num, sinfo->natural_id);

		print_datum(estate,
					estate->datums[((PLpgSQL_stmt_assign *) stmt)->varno],
					printbuf,
					tinfo->frame_num + sinfo->level);
	}
}

static void
tracer_stmt_end_aborted(Oid fn_oid, int stmtid, void **plugin2_info)
{
	tracer_info *tinfo = *plugin2_info;
	plpgsql_check_plugin2_stmt_info *sinfo;

	if (!tinfo)
		return;

	sinfo = plpgsql_check_get_current_stmt_info(stmtid);

	_tracer_stmt_end(tinfo, sinfo, stmtid, true);
}

static void
trace_assert(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, tracer_info *tinfo)
{
	PLpgSQL_var result;
	PLpgSQL_type typ;
	char		exprbuf[200];
	PLpgSQL_stmt_assert *stmt_assert = (PLpgSQL_stmt_assert *) stmt;

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

	tracer_plugin2.assign_expr(estate, (PLpgSQL_datum *) &result, stmt_assert->cond);

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
		int		frame_num = tinfo->frame_num;

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

void
plpgsql_check_tracer_init(void)
{
	plpgsql_check_register_pldbgapi2_plugin(&tracer_plugin2);
}

/*
 * Enable, disable, show state tracer
 */
Datum
plpgsql_check_tracer_ctrl(PG_FUNCTION_ARGS)
{
	char	   *optstr;
	bool		result;

#define OPTNAME_1		"plpgsql_check.tracer"
#define OPTNAME_2		"plpgsql_check.tracer_verbosity"

	if (!PG_ARGISNULL(0))
	{
		bool		optval = PG_GETARG_BOOL(0);

		(void) set_config_option(OPTNAME_1, optval ? "on" : "off",
								 (superuser() ? PGC_SUSET : PGC_USERSET),
								 PGC_S_SESSION, GUC_ACTION_SET,
								 true, 0, false);
	}

	if (!PG_ARGISNULL(1))
	{
		char	   *optval = TextDatumGetCString(PG_GETARG_DATUM(1));

		(void) set_config_option(OPTNAME_2, optval,
								 (superuser() ? PGC_SUSET : PGC_USERSET),
								 PGC_S_SESSION, GUC_ACTION_SET,
								 true, 0, false);
	}


	optstr = GetConfigOptionByName(OPTNAME_1, NULL, false);

	if (strcmp(optstr, "on") == 0)
	{
		elog(NOTICE, "tracer is active");
		result = true;
	}
	else
	{
		elog(NOTICE, "tracer is not active");
		result = false;
	}

	optstr = GetConfigOptionByName(OPTNAME_2, NULL, false);

	elog(NOTICE, "tracer verbosity is %s", optstr);

	if (result && !plpgsql_check_enable_tracer)
		ereport(NOTICE,
				(errmsg("tracer is still blocked"),
				 errdetail("The tracer should be enabled by the superuser for security reasons."),
				 errhint("Execute \"set plpgsql_check.enable_tracer to on\" (superuser only).")));

	PG_RETURN_BOOL(result);
}
