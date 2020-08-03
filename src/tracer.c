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
print_fargs(PLpgSQL_execstate *estate, PLpgSQL_function *func, int frame_num, int level)
{
	int		i;
	StringInfoData		ds;

	initStringInfo(&ds);

	/* print value of arguments */
	for (i = 0; i < func->fn_nargs; i++)
	{
		int			n = func->fn_argvarnos[i];

		switch (estate->datums[n]->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
				{
					PLpgSQL_var *var = (PLpgSQL_var *) estate->datums[n];

					if (var->isnull)
					{
						if (*ds.data)
							appendStringInfoString(&ds, ", ");

						appendStringInfo(&ds, "\"%s\" => null", var->refname);
					}
					else
					{
						char	   *str;
						bool		extra_line = false;

						str = convert_value_to_string(estate,
													var->value,
													var->datatype->typoid);

						extra_line = ((int) strlen(str)) > plpgsql_check_tracer_variable_max_length ||
									 strchr(str, '\n') != NULL;

						if (extra_line)
						{
							if (*ds.data)
								elog(NOTICE, "#%d%*s %s", frame_num, level * 2 + 4, "", ds.data);

							resetStringInfo(&ds);

							trim_string(str, plpgsql_check_tracer_variable_max_length);
							elog(NOTICE, "#%d%*s \"%s\" => '%s'", frame_num, level * 2 + 4, "", var->refname, str);
						}
						else
						{
							if (*ds.data)
								appendStringInfoString(&ds, ", ");

							appendStringInfo(&ds, "\"%s\" => '%s'", var->refname, str);
						}

						pfree(str);
					}
					break;
				}

			case PLPGSQL_DTYPE_REC:
				{
					break;
				}

			case PLPGSQL_DTYPE_ROW:
				{
					break;
				}

			default:
				/* do nothing */ ;
		}

		/*print too long lines immediately */
		if (ds.len > plpgsql_check_tracer_variable_max_length)
		{
			elog(NOTICE, "#%d%*s %s", frame_num, level * 2 + 4, "",  ds.data);
			resetStringInfo(&ds);
		}

	}

	if (*ds.data)
	{
		elog(NOTICE, "#%d%*s %s", frame_num, level * 2 + 4, "",  ds.data);
	}

	pfree(ds.data);
}

/*
 * Initialize tracer info data in plugin data, and displays info about function
 * entry.
 */
void
plpgsql_check_tracer_on_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	PLpgSQL_execstate *outer_estate;
	int		frame_num;
	int		level;
	instr_time start_time;
	Oid		fn_oid;

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

	elog(plpgsql_check_tracer_errlevel,
		 "#%d%*s ->> start of %s%s (Oid=%u)",
											  frame_num,
											  level * 2,
											  "",
											  func->fn_oid ? "function " : "block ",
											  func->fn_signature,
											  fn_oid);

	if (outer_estate)
	{
		if (outer_estate->err_stmt)
			elog(plpgsql_check_tracer_errlevel,
				 "#%d%*s previous execution of PLpgSQL function %s line %d at %s",
													frame_num,
													level * 2 + 4, "",
													outer_estate->func->fn_signature,
													outer_estate->err_stmt->lineno,
													plpgsql_check__stmt_typename_p(outer_estate->err_stmt));
		else
			elog(plpgsql_check_tracer_errlevel,
				 "#%d%*s previous execution of PLpgSQL function %s",
													frame_num,
													level * 2 + 4, "  ",
													outer_estate->func->fn_signature);
	}

	print_fargs(estate, func, frame_num, level);
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

		INSTR_TIME_SET_CURRENT(end_time);
		INSTR_TIME_SUBTRACT(end_time, start_time);

		elapsed = INSTR_TIME_GET_MICROSEC(end_time);

		/* For output in regress tests use immutable time 0.010 ms */
		if (plpgsql_check_tracer_test_mode)
			elapsed = 10;

		if (func->fn_oid)
			elog(plpgsql_check_tracer_errlevel,
				 "#%d%*s <<- end of function %s (elapsed time=%.3f ms)",
														frame_num,
														level * 2, "",
														get_func_name(func->fn_oid),
														elapsed / 1000.0);
		else
			elog(plpgsql_check_tracer_errlevel,
				 "#%d%*s <<- end of block (elapsed time=%.3f ms)",
														frame_num,
														level * 2, "",
														elapsed / 1000.0);
	}
}
