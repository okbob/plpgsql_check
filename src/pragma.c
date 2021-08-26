/*-------------------------------------------------------------------------
 *
 * pragma.c
 *
 *			  pragma related code
 *
 * by Pavel Stehule 2013-2021
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "utils/builtins.h"
#include "utils/array.h"
#include "parser/scansup.h"

#ifdef _MSC_VER

#define strcasecmp _stricmp
#define strncasecmp _strnicmp

#endif

PG_FUNCTION_INFO_V1(plpgsql_check_pragma);

plpgsql_check_pragma_vector plpgsql_check_runtime_pragma_vector;

bool plpgsql_check_runtime_pragma_vector_changed = false;

static char *get_record(char *str, bool *iserror, bool istop);



//extern bool plpgsql_check_is_ident_start(unsigned char c);
//extern bool plpgsql_check_is_ident_cont(unsigned char c);

/*
 * Parser of typeset and typecheck pragmas.
 *
 *    Example: 'set_type:"xxx"."yyy":(f1 int, (f2 int, f3 int))'
 *             'set_type_and_check:r:(a int, b int)'
 *
 * These pragmas can be used only for record type. These pragmas
 * will have block scope. The pragma will have higher priority
 * than real type.
 */

#define		TOKEN_IDENT			1


/*
 * Returns
 */
static char *
get_token(char *str,
		  char **name,
		  int *token,
		  bool *iserror)
{
	*iserror = false;

	/* skip leading whitespace */
	while (scanner_isspace(*str))
		str++;

	if (*str == '\0')
	{
		*token = 0;
		return NULL;
	}
	else if (strchr("().,", *str))
	{
		*token = *str;
		return str + 1;
	}
	else if (*str == '"')
	{
		char	   *ptr;

		ptr = *name = palloc(sizeof(str));

		str += 1;
		while (*str)
		{
			if (*str == '"')
			{
				if (str[1] == '"')
				{
					str++;
				}
				else
				{
					if (ptr != *name)
					{
						*ptr = '\0';
						*token = TOKEN_IDENT;
						return str;
					}
					else
					{
						ereport(WARNING,
								 errmsg("string is not a valid identifier"),
								 errdetail("Quoted identifier must not be empty."));
						*iserror = false;
						return NULL;
					}
				}
			}

			*ptr++ = *str++;
		}

		ereport(WARNING,
				errmsg("string is not a valid identifier"),
				errdetail("String has unclosed double quotes."));
		*iserror = false;
		return NULL;
	}
	else if (plpgsql_check_is_ident_start((unsigned char) *str))
	{
		char	   *ptr;

		ptr = *name = palloc(sizeof(str));

		*ptr = *str++;
		while (plpgsql_check_is_ident_cont((unsigned char) *str))
			*ptr++ = *str++;

		*ptr = '\0';
		*token = TOKEN_IDENT;

		return str;
	}

	elog(WARNING, "unexpected string \"%s\"", str);
	*iserror = true;
	return NULL;
}

static char *
get_field_name(char *str, bool *iserror)
{
	char	   *name;
	int		   token;

	str = get_token(str, &name, &token, iserror);
	if (iserror)
		return NULL;

	if (token != TOKEN_IDENT)
	{
		elog(WARNING, "missing identifier");
		*iserror = true;
		return NULL;
	}

	str = get_token(str, &name, &token, iserror);
	if (iserror)
		return NULL;

	if (token == '.')
	{
		str = get_token(str, &name, &token, iserror);
		if (iserror)
			return NULL;

		if (token != TOKEN_IDENT)
		{
			elog(WARNING, "missing identifier");
			*iserror = true;
			return NULL;
		}
	}
	else
		str -= 1;

	*iserror = false;

	return str;
}

static char *
get_type(char *str, bool *iserror)
{
	char	   *name;
	int			token;

	str = get_token(str, &name, &token, iserror);
	if (iserror)
		return NULL;

	if (token == '(')
	{
		str -= 1;
		return get_record(str, iserror, false);
	}

	while (token == TOKEN_IDENT)
	{
		str = get_token(str, &name, &token, iserror);
		if (iserror)
			return NULL;
	}

	str -= 1;
	*iserror = false;

	return str;
}

static char *
get_record(char *str, bool *iserror, bool istop)
{
	char	   *name;
	int			token;

	str = get_token(str, &name, &token, iserror);
	if (iserror)
		return NULL;

	if (token != '(')
	{
		elog(WARNING, "expecting '(')");
		*iserror = true;
		return NULL;
	}

field:

	str = get_field_name(str, iserror);
	if (iserror)
		return NULL;

	str = get_type(str, iserror);
	if (iserror)
		return NULL;

	str = get_token(str, &name, &token, iserror);
	if (token == ',')
		goto field;

	if (token != ')')
	{
		elog(WARNING, "expecting ')'");
		*iserror = true;
		return NULL;
	}

	if (istop)
	{
		str = get_token(str, &name, &token, iserror);
		if (iserror)
			return NULL;
		if (token != 0)
		{
			elog(WARNING, "unexpected content after ')'");
			*iserror = true;
			return NULL;
		}
	}

	*iserror = false;
	return str;
}


static bool
pragma_apply(plpgsql_check_pragma_vector *pv, char *pragma_str)
{
	bool	is_valid = true;

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
					pv->disable_other_warnings ? "disabled" : "enabled");
		else
		{
			elog(WARNING, "unsuported pragma: %s", pragma_str);
			is_valid = false;
		}
	}
	else if (strncasecmp(pragma_str, "ENABLE:", 7) == 0)
	{
		pragma_str += 7;

		while (*pragma_str == ' ')
			pragma_str++;

		if (strcasecmp(pragma_str, "CHECK") == 0)
			pv->disable_check = false;
		else if (strcasecmp(pragma_str, "TRACER") == 0)
		{
			pv->disable_tracer = false;

#if PG_VERSION_NUM < 120000

			elog(WARNING, "pragma ENABLE:TRACER is ignored on PostgreSQL 11 and older");

#endif

		}
		else if (strcasecmp(pragma_str, "OTHER_WARNINGS") == 0)
			pv->disable_other_warnings = false;
		else if (strcasecmp(pragma_str, "PERFORMANCE_WARNINGS") == 0)
			pv->disable_performance_warnings = false;
		else if (strcasecmp(pragma_str, "EXTRA_WARNINGS") == 0)
			pv->disable_extra_warnings = false;
		else if (strcasecmp(pragma_str, "SECURITY_WARNINGS") == 0)
			pv->disable_security_warnings = false;
		else
		{
			elog(WARNING, "unsuported pragma: %s", pragma_str);
			is_valid = false;
		}
	}
	else if (strncasecmp(pragma_str, "DISABLE:", 8) == 0)
	{
		pragma_str += 8;

		while (*pragma_str == ' ')
			pragma_str++;

		if (strcasecmp(pragma_str, "CHECK") == 0)
			pv->disable_check = true;
		else if (strcasecmp(pragma_str, "TRACER") == 0)
		{
			pv->disable_tracer = true;

#if PG_VERSION_NUM < 120000

			elog(WARNING, "pragma DISABLE:TRACER is ignored on PostgreSQL 11 and older");

#endif
		}
		else if (strcasecmp(pragma_str, "OTHER_WARNINGS") == 0)
			pv->disable_other_warnings = true;
		else if (strcasecmp(pragma_str, "PERFORMANCE_WARNINGS") == 0)
			pv->disable_performance_warnings = true;
		else if (strcasecmp(pragma_str, "EXTRA_WARNINGS") == 0)
			pv->disable_extra_warnings = true;
		else if (strcasecmp(pragma_str, "SECURITY_WARNINGS") == 0)
			pv->disable_security_warnings = true;
		else
			elog(WARNING, "unsuported pragma: %s", pragma_str);
	}
	else if ((strncasecmp(pragma_str, "SET_TYPE:", 9) == 0) ||
			 (strncasecmp(pragma_str, "SET_TYPE_AND_CHECK:", 19) == 0))
	{
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
	ArrayType *array;
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

		pragma_apply(&plpgsql_check_runtime_pragma_vector, pragma_str);

		plpgsql_check_runtime_pragma_vector_changed = true;

		pfree(pragma_str);
	}

	array_free_iterator(iter);

	PG_RETURN_INT32(1);
}

void
plpgsql_check_pragma_apply(PLpgSQL_checkstate *cstate, char *pragma_str)
{
	if (pragma_apply(&(cstate->pragma_vector), pragma_str))
		cstate->was_pragma = true;
}
