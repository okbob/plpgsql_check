/*-------------------------------------------------------------------------
 *
 * parse_name.c
 *
 *			  parse function signature
 *
 * by Pavel Stehule 2013-2021
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "catalog/namespace.h"
#include "parser/scansup.h"
#include "utils/builtins.h"

/*
 * Is character a valid identifier start?
 * Must match scan.l's {ident_start} character class.
 */
static bool
is_ident_start(unsigned char c)
{
	/* Underscores and ASCII letters are OK */
	if (c == '_')
		return true;
	if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))
		return true;
	/* Any high-bit-set character is OK (might be part of a multibyte char) */
	if (IS_HIGHBIT_SET(c))
		return true;
	return false;
}

/*
 * Is character a valid identifier continuation?
 * Must match scan.l's {ident_cont} character class.
 */
static bool
is_ident_cont(unsigned char c)
{
	/* Can be digit or dollar sign ... */
	if ((c >= '0' && c <= '9') || c == '$')
		return true;
	/* ... or an identifier start character */
	return is_ident_start(c);
}

/*
 * parse_ident - returns list of Strings when input is valid name.
 * Returns NIL, when input string is signature. Can raise a error,
 * when input is not valid identifier.
 */
static List *
parse_name_or_signature(char *qualname, bool *is_signature)
{
	char	   *nextp;
	char	   *rawname;
	bool		after_dot = false;
	List	   *result = NIL;

	/* We need a modifiable copy of the input string. */
	rawname = pstrdup(qualname);

	/*
	 * The code below scribbles on qualname_str in some cases, so we should
	 * reconvert qualname if we need to show the original string in error
	 * messages.
	 */
	nextp = rawname;

	/* skip leading whitespace */
	while (scanner_isspace(*nextp))
		nextp++;

	for (;;)
	{
		char	   *curname;
		bool		missing_ident = true;

		if (*nextp == '"')
		{
			char	   *endp;

			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '"');
				if (endp == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("string is not a valid identifier: \"%s\"",
									qualname),
							 errdetail("String has unclosed double quotes.")));
				if (endp[1] != '"')
					break;
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			nextp = endp + 1;
			*endp = '\0';

			if (endp - curname == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("string is not a valid identifier: \"%s\"",
								qualname),
						 errdetail("Quoted identifier must not be empty.")));

			truncate_identifier(curname, strlen(curname), true);
			result = lappend(result, makeString(curname));

			missing_ident = false;
		}
		else if (is_ident_start((unsigned char) *nextp))
		{
			char	   *downname;
			int			len;

			curname = nextp++;
			while (is_ident_cont((unsigned char) *nextp))
				nextp++;

			len = nextp - curname;

			/*
			 * We don't implicitly truncate identifiers. This is useful for
			 * allowing the user to check for specific parts of the identifier
			 * being too long. It's easy enough for the user to get the
			 * truncated names by casting our output to name[].
			 */
			downname = downcase_truncate_identifier(curname, len, false);
			result = lappend(result, makeString(downname));
			missing_ident = false;
		}

		if (missing_ident)
		{
			/* Different error messages based on where we failed. */
			if (*nextp == '.')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("string is not a valid identifier: \"%s\"",
								qualname),
						 errdetail("No valid identifier before \".\".")));
			else if (after_dot)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("string is not a valid identifier: \"%s\"",
								qualname),
						 errdetail("No valid identifier after \".\".")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("string is not a valid identifier: \"%s\"",
								qualname)));
		}

		while (scanner_isspace(*nextp))
			nextp++;

		if (*nextp == '.')
		{
			after_dot = true;
			nextp++;
			while (scanner_isspace(*nextp))
				nextp++;
		}
		else if (*nextp == '\0')
		{
			break;
		}
		else if (*nextp == '(')
		{
			*is_signature = true;
			return NIL;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("string is not a valid identifier: \"%s\"",
							qualname)));
	}

	*is_signature = false;

	return result;
}

/*
 * Returns Oid of function specified by name or by signature
 *
 */
Oid
plpgsql_check_parse_name_or_signature(char *name_or_signature)
{
	List	   *names;
	bool		is_signature;

	names = parse_name_or_signature(name_or_signature, &is_signature);

	if (!is_signature)
	{
		FuncCandidateList clist;

		clist = FuncnameGetCandidates(names, -1, NIL, false, false,
#if PG_VERSION_NUM >= 140000 && CATALOG_VERSION_NO >= 202106101
									  false,
#endif
									  true);

		if (clist == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function \"%s\" does not exist", name_or_signature)));
		else if (clist->next != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
					 errmsg("more than one function named \"%s\"",
							name_or_signature)));

		return clist->oid;
	}

	return DatumGetObjectId(DirectFunctionCall1(regprocedurein,
												CStringGetDatum(name_or_signature)));
}
