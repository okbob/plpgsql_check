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
	if (strncasecmp(pragma_str, "ECHO:", 5) == 0)
	{
		elog(NOTICE, "%s", pragma_str + 5);
	}

	elog(NOTICE, "%s %s", pragma_str, plpgsql_check__stmt_typename_p(cstate->estate->err_stmt));
	cstate->was_pragma = true;
}

