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
void
plpgsql_check_tracer_print_fargs(PLpgSQL_execstate *estate, PLpgSQL_function *func, long unsigned int run_id, int level)
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
								elog(NOTICE, "#%lu%*s %s", run_id, level * 2 + 4, "", ds.data);

							resetStringInfo(&ds);

							trim_string(str, plpgsql_check_tracer_variable_max_length);
							elog(NOTICE, "#%lu%*s \"%s\" => '%s'", run_id, level * 2 + 4, "", var->refname, str);
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
			elog(NOTICE, "#%lu%*s %s", run_id, level * 2 + 4, "",  ds.data);
			resetStringInfo(&ds);
		}

	}

	if (*ds.data)
	{
		elog(NOTICE, "#%lu%*s %s", run_id, level * 2 + 4, "",  ds.data);
	}

	pfree(ds.data);
}
