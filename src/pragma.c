/*-------------------------------------------------------------------------
 *
 * pragma.c
 *
 *			  pragma related code
 *
 * by Pavel Stehule 2013-2025
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "utils/builtins.h"
#include "utils/array.h"
#include "parser/scansup.h"

#include "tcop/tcopprot.h"
#include "tcop/utility.h"

#ifdef _MSC_VER

#define strcasecmp _stricmp
#define strncasecmp _strnicmp

#endif

PG_FUNCTION_INFO_V1(plpgsql_check_pragma);

static void
runtime_pragma_apply(char *pragma_str)
{
	while (scanner_isspace(*pragma_str))
		pragma_str++;

	if (strncasecmp(pragma_str, "STATUS:", 7) == 0)
	{
		pragma_str += 7;

		while (scanner_isspace(*pragma_str))
			pragma_str++;

		if (strcasecmp(pragma_str, "TRACER") == 0)
			elog(NOTICE, "tracer is %s",
				 plpgsql_check_tracer ? "enabled" : "disabled");
	}
	else if (strncasecmp(pragma_str, "ENABLE:", 7) == 0)
	{
		pragma_str += 7;

		while (scanner_isspace(*pragma_str))
			pragma_str++;

		if (strcasecmp(pragma_str, "TRACER") == 0)
			plpgsql_check_tracer = true;
	}
	else if (strncasecmp(pragma_str, "DISABLE:", 8) == 0)
	{
		pragma_str += 8;

		while (scanner_isspace(*pragma_str))
			pragma_str++;

		if (strcasecmp(pragma_str, "TRACER") == 0)
			plpgsql_check_tracer = false;
	}
}

static bool
pragma_apply(PLpgSQL_checkstate *cstate,
			 plpgsql_check_pragma_vector *pv,
			 char *pragma_str,
			 PLpgSQL_nsitem *ns,
			 int lineno)
{
	bool		is_valid = true;

	Assert(cstate);

	while (scanner_isspace(*pragma_str))
		pragma_str++;

	if (strncasecmp(pragma_str, "ECHO:", 5) == 0)
	{
		elog(NOTICE, "%s", plpgsql_check_process_echo_string(pragma_str + 5, cstate->cinfo));
	}
	else if (strncasecmp(pragma_str, "STATUS:", 7) == 0)
	{
		pragma_str += 7;

		while (scanner_isspace(*pragma_str))
			pragma_str++;

		if (strcasecmp(pragma_str, "CHECK") == 0)
			elog(NOTICE, "check is %s",
				 pv->disable_check ? "disabled" : "enabled");
		else if (strcasecmp(pragma_str, "TRACER") == 0)
			elog(NOTICE, "tracer is %s",
				 pv->disable_tracer ? "disabled" : "enabled");
		else if (strcasecmp(pragma_str, "OTHER_WARNINGS") == 0)
			elog(NOTICE, "other_warnings is %s",
				 pv->disable_other_warnings ? "disabled" : "enabled");
		else if (strcasecmp(pragma_str, "PERFORMANCE_WARNINGS") == 0)
			elog(NOTICE, "performance_warnings is %s",
				 pv->disable_performance_warnings ? "disabled" : "enabled");
		else if (strcasecmp(pragma_str, "EXTRA_WARNINGS") == 0)
			elog(NOTICE, "extra_warnings is %s",
				 pv->disable_extra_warnings ? "disabled" : "enabled");
		else if (strcasecmp(pragma_str, "SECURITY_WARNINGS") == 0)
			elog(NOTICE, "security_warnings is %s",
				 pv->disable_security_warnings ? "disabled" : "enabled");
		else if (strcasecmp(pragma_str, "COMPATIBILITY_WARNINGS") == 0)
			elog(NOTICE, "compatibility_warnings is %s",
				 pv->disable_compatibility_warnings ? "disabled" : "enabled");
		else if (strcasecmp(pragma_str, "CONSTANTS_TRANCING") == 0)
			elog(NOTICE, "constants_traising is %s",
				 pv->disable_constants_tracing ? "disabled" : "enabled");
		else
		{
			elog(WARNING, "unsuported pragma: %s", pragma_str);
			is_valid = false;
		}
	}
	else if (strncasecmp(pragma_str, "ENABLE:", 7) == 0)
	{
		pragma_str += 7;

		while (scanner_isspace(*pragma_str))
			pragma_str++;

		if (strcasecmp(pragma_str, "CHECK") == 0)
			pv->disable_check = false;
		else if (strcasecmp(pragma_str, "TRACER") == 0)
			pv->disable_tracer = false;
		else if (strcasecmp(pragma_str, "OTHER_WARNINGS") == 0)
			pv->disable_other_warnings = false;
		else if (strcasecmp(pragma_str, "PERFORMANCE_WARNINGS") == 0)
			pv->disable_performance_warnings = false;
		else if (strcasecmp(pragma_str, "EXTRA_WARNINGS") == 0)
			pv->disable_extra_warnings = false;
		else if (strcasecmp(pragma_str, "SECURITY_WARNINGS") == 0)
			pv->disable_security_warnings = false;
		else if (strcasecmp(pragma_str, "COMPATIBILITY_WARNINGS") == 0)
			pv->disable_compatibility_warnings = false;
		else if (strcasecmp(pragma_str, "CONSTANTS_TRACING") == 0)
			pv->disable_constants_tracing = false;
		else
		{
			elog(WARNING, "unsuported pragma: %s", pragma_str);
			is_valid = false;
		}
	}
	else if (strncasecmp(pragma_str, "DISABLE:", 8) == 0)
	{
		pragma_str += 8;

		while (scanner_isspace(*pragma_str))
			pragma_str++;

		if (strcasecmp(pragma_str, "CHECK") == 0)
			pv->disable_check = true;
		else if (strcasecmp(pragma_str, "TRACER") == 0)
			pv->disable_tracer = true;
		else if (strcasecmp(pragma_str, "OTHER_WARNINGS") == 0)
			pv->disable_other_warnings = true;
		else if (strcasecmp(pragma_str, "PERFORMANCE_WARNINGS") == 0)
			pv->disable_performance_warnings = true;
		else if (strcasecmp(pragma_str, "EXTRA_WARNINGS") == 0)
			pv->disable_extra_warnings = true;
		else if (strcasecmp(pragma_str, "SECURITY_WARNINGS") == 0)
			pv->disable_security_warnings = true;
		else if (strcasecmp(pragma_str, "COMPATIBILITY_WARNINGS") == 0)
			pv->disable_compatibility_warnings = true;
		else if (strcasecmp(pragma_str, "CONSTANTS_TRACING") == 0)
			pv->disable_constants_tracing = true;
		else
			elog(WARNING, "unsuported pragma: %s", pragma_str);
	}
	else if (strncasecmp(pragma_str, "TYPE:", 5) == 0)
	{
		is_valid = plpgsql_check_pragma_type(cstate, pragma_str + 5, ns, lineno);
	}
	else if (strncasecmp(pragma_str, "TABLE:", 6) == 0)
	{
		is_valid = plpgsql_check_pragma_table(cstate, pragma_str + 6, lineno);
	}
	else if (strncasecmp(pragma_str, "SEQUENCE:", 6) == 0)
	{
		is_valid = plpgsql_check_pragma_sequence(cstate, pragma_str + 9, lineno);
	}
	else if (strncasecmp(pragma_str, "ASSERT-SCHEMA:", 14) == 0)
	{
		is_valid = plpgsql_check_pragma_assert(cstate,
											   PLPGSQL_CHECK_PRAGMA_ASSERT_SCHEMA,
											   pragma_str + 14, ns, lineno);
	}
	else if (strncasecmp(pragma_str, "ASSERT-TABLE:", 13) == 0)
	{
		is_valid = plpgsql_check_pragma_assert(cstate,
											   PLPGSQL_CHECK_PRAGMA_ASSERT_TABLE,
											   pragma_str + 13, ns, lineno);
	}
	else if (strncasecmp(pragma_str, "ASSERT-COLUMN:", 14) == 0)
	{
		is_valid = plpgsql_check_pragma_assert(cstate,
											   PLPGSQL_CHECK_PRAGMA_ASSERT_COLUMN,
											   pragma_str + 14, ns, lineno);
	}
	else
	{
		elog(WARNING, "unsupported pragma: %s", pragma_str);
		is_valid = false;
	}

	return is_valid;
}


/*
 * Implementation of pragma function. There are two different
 * use cases - 1) it is used for static analyze by plpgsql_check,
 * where arguments are read from parse tree.
 * 2) it is used for controlling of code tracing in runtime.
 * arguments, are processed as usual for variadic text function.
 */
Datum
plpgsql_check_pragma(PG_FUNCTION_ARGS)
{
	ArrayType  *array;
	ArrayIterator iter;
	bool		isnull;
	Datum		value;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT32(0);

	array = PG_GETARG_ARRAYTYPE_P(0);

	iter = array_create_iterator(array, 0, NULL);

	while (array_iterate(iter, &value, &isnull))
	{
		char	   *pragma_str;

		if (isnull)
			continue;

		pragma_str = TextDatumGetCString(value);

		runtime_pragma_apply(pragma_str);
		pfree(pragma_str);
	}

	array_free_iterator(iter);

	PG_RETURN_INT32(1);
}

void
plpgsql_check_pragma_apply(PLpgSQL_checkstate *cstate, char *pragma_str, PLpgSQL_nsitem *ns, int lineno)
{
	if (pragma_apply(cstate, &(cstate->pragma_vector), pragma_str, ns, lineno))
		cstate->was_pragma = true;
}
