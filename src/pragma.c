/*-------------------------------------------------------------------------
 *
 * praga.c
 *
 *			  pragma related code
 *
 * by Pavel Stehule 2013-2020
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

PG_FUNCTION_INFO_V1(plpgsql_check_pragma);


/*
 * Implementation of pragma function. plpgsql_check can process
 * a arguments for some data for static analyzes, but it returns
 * just zero for runtime (it does nothing).
 */
Datum
plpgsql_check_pragma(PG_FUNCTION_ARGS)
{
	(void) fcinfo;

	PG_RETURN_INT32(1);
}

void
plpgsql_check_pragma_apply(PLpgSQL_checkstate *cstate, char *pragma_str)
{
	while (*pragma_str == ' ')
		pragma_str++;

	if (strncasecmp(pragma_str, "ECHO:", 5) == 0)
	{
		elog(NOTICE, "%s", pragma_str + 5);
	}
	else if (strncasecmp(pragma_str, "STATUS:", 7) == 0)
	{
		pragma_str += 7;

		while (*pragma_str == ' ')
			pragma_str++;

		if (strcasecmp(pragma_str, "CHECK") == 0)
			elog(NOTICE, "check is %s",
					cstate->pragma_vector.disable_check ? "disabled" : "enabled");
		else if (strcasecmp(pragma_str, "TRACER") == 0)
			elog(NOTICE, "tracer is %s",
					cstate->pragma_vector.disable_tracer ? "disabled" : "enabled");
		else if (strcasecmp(pragma_str, "OTHER_WARNINGS") == 0)
			cstate->pragma_vector.disable_other_warnings = false;
		else if (strcasecmp(pragma_str, "PERFORMANCE_WARNINGS") == 0)
			cstate->pragma_vector.disable_performance_warnings = false;
		else if (strcasecmp(pragma_str, "EXTRA_WARNINGS") == 0)
			cstate->pragma_vector.disable_extra_warnings = false;
		else if (strcasecmp(pragma_str, "SECURITY_WARNINGS") == 0)
			cstate->pragma_vector.disable_security_warnings = false;
		else
			elog(WARNING, "unsuported pragma: %s", pragma_str);
	}
	else if (strncasecmp(pragma_str, "ENABLE:", 7) == 0)
	{
		pragma_str += 7;

		while (*pragma_str == ' ')
			pragma_str++;

		if (strcasecmp(pragma_str, "CHECK") == 0)
			cstate->pragma_vector.disable_check = false;
		else if (strcasecmp(pragma_str, "TRACER") == 0)
			cstate->pragma_vector.disable_tracer = false;
		else if (strcasecmp(pragma_str, "OTHER_WARNINGS") == 0)
			cstate->pragma_vector.disable_other_warnings = false;
		else if (strcasecmp(pragma_str, "PERFORMANCE_WARNINGS") == 0)
			cstate->pragma_vector.disable_performance_warnings = false;
		else if (strcasecmp(pragma_str, "EXTRA_WARNINGS") == 0)
			cstate->pragma_vector.disable_extra_warnings = false;
		else if (strcasecmp(pragma_str, "SECURITY_WARNINGS") == 0)
			cstate->pragma_vector.disable_security_warnings = false;
		else
			elog(WARNING, "unsuported pragma: %s", pragma_str);
	}
	else if (strncasecmp(pragma_str, "DISABLE:", 8) == 0)
	{
		pragma_str += 8;

		while (*pragma_str == ' ')
			pragma_str++;

		if (strcasecmp(pragma_str, "CHECK") == 0)
			cstate->pragma_vector.disable_check = true;
		else if (strcasecmp(pragma_str, "TRACER") == 0)
			cstate->pragma_vector.disable_tracer = true;
		else if (strcasecmp(pragma_str, "OTHER_WARNINGS") == 0)
			cstate->pragma_vector.disable_other_warnings = true;
		else if (strcasecmp(pragma_str, "PERFORMANCE_WARNINGS") == 0)
			cstate->pragma_vector.disable_performance_warnings = true;
		else if (strcasecmp(pragma_str, "EXTRA_WARNINGS") == 0)
			cstate->pragma_vector.disable_extra_warnings = true;
		else if (strcasecmp(pragma_str, "SECURITY_WARNINGS") == 0)
			cstate->pragma_vector.disable_security_warnings = true;
		else
			elog(WARNING, "unsuported pragma: %s", pragma_str);
	}
	else
		elog(WARNING, "unsupported pragma: %s", pragma_str);

	cstate->was_pragma = true;
}
