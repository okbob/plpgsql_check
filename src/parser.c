/*-------------------------------------------------------------------------
 *
 * parse_name.c
 *
 *			  parse function signature
 *			  parse identifier, and type name
 *
 * by Pavel Stehule 2013-2026
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include <string.h>

#include "catalog/namespace.h"
#include "parser/scansup.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"

/*
 * Originaly this structure was named TokenType, but this is in collision
 * with name from NT SDK. So it is renamed to PragmaTokenType.
 */
typedef struct PragmaTokenType
{
	int			value;
	const char *substr;
	size_t		size;
} PragmaTokenType;

typedef struct TokenizerState
{
	const char *str;
	PragmaTokenType saved_token;
	bool		saved_token_is_valid;
} TokenizerState;

static char *make_string(PragmaTokenType *token);

static const char *tagstr = "@plpgsql_check_options:";

#define		PRAGMA_TOKEN_IDENTIF		128
#define		PRAGMA_TOKEN_QIDENTIF		129
#define		PRAGMA_TOKEN_NUMBER			130
#define		PRAGMA_TOKEN_STRING			131

#ifdef _MSC_VER

static void *
memmem(const void *haystack, size_t haystack_len,
	   const void *const needle, const size_t needle_len)
{
	if (haystack == NULL)
		return NULL;
	/* or assert(haystack != NULL); */
	if (haystack_len == 0)
		return NULL;
	if (needle == NULL)
		return NULL;
	/* or assert(needle != NULL); */
	if (needle_len == 0)
		return NULL;

	for (const char *h = haystack;
		 haystack_len >= needle_len;
		 ++h, --haystack_len)
	{
		if (!memcmp(h, needle, needle_len))
		{
			return (void *) h;
		}
	}
	return NULL;
}

#endif

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
				if (!endp)
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
			size_t		len;

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
			downname = downcase_truncate_identifier(curname, (int) len, false);
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

#if PG_VERSION_NUM < 190000

		clist = FuncnameGetCandidates(names, -1, NIL, false, false, false,
									  true);

#else

		int		fgc_flags;

		clist = FuncnameGetCandidates(names, -1, NIL, false, false, false,
									  true, &fgc_flags);

		(void) fgc_flags;

#endif

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

/*
 * Tokenize text. Only one error is possible here - unclosed double quotes.
 * Returns NULL, when found EOL or on error.
 */
static PragmaTokenType *
get_token(TokenizerState *state, PragmaTokenType *token)
{
	if (state->saved_token_is_valid)
	{
		state->saved_token_is_valid = false;
		return &state->saved_token;
	}

	/* skip inital spaces */
	while (scanner_isspace(*state->str))
		state->str++;

	if (!*state->str)
		return NULL;

	if (isdigit(*state->str))
	{
		bool		have_dot = false;

		token->value = PRAGMA_TOKEN_NUMBER;
		token->substr = state->str++;

		while (isdigit(*state->str) || (*state->str == '.'))
		{
			if (*state->str == '.')
			{
				if (!have_dot)
					have_dot = true;
				else
					break;
			}

			state->str += 1;
		}
	}
	else if (*state->str == '"')
	{
		bool		is_error = true;

		token->value = PRAGMA_TOKEN_QIDENTIF;
		token->substr = state->str++;

		is_error = true;

		while (*state->str)
		{
			if (*state->str == '"')
			{
				state->str += 1;
				if (*state->str != '"')
				{
					is_error = false;
					break;
				}
			}

			state->str += 1;
		}

		if (is_error)
			elog(ERROR, "Syntax error (unclosed quoted identifier)");
	}
	else if (*state->str == '\'')
	{
		bool		is_error = true;

		token->value = PRAGMA_TOKEN_STRING;
		token->substr = state->str++;

		is_error = true;

		while (*state->str)
		{
			if (*state->str == '\'')
			{
				state->str += 1;
				if (*state->str != '\'')
				{
					is_error = false;
					break;
				}
			}

			state->str += 1;
		}

		if (is_error)
			elog(ERROR, "Syntax error (unclosed quoted identifier)");
	}
	else if (is_ident_start(*state->str))
	{
		token->value = PRAGMA_TOKEN_IDENTIF;
		token->substr = state->str++;

		while (is_ident_cont(*state->str))
			state->str += 1;
	}
	else
		token->value = *state->str++;

	token->size = state->str - token->substr;

	return token;
}

static void
unget_token(TokenizerState *state, PragmaTokenType *token)
{
	if (token)
	{
		state->saved_token.value = token->value;
		state->saved_token.substr = token->substr;
		state->saved_token.size = token->size;

		state->saved_token_is_valid = true;
	}
	else
		state->saved_token_is_valid = false;
}

static bool
token_is_keyword(PragmaTokenType *token, const char *str)
{
	if (!token)
		return false;

	if (token->value == PRAGMA_TOKEN_IDENTIF &&
		token->size == strlen(str) &&
		strncasecmp(token->substr, str, token->size) == 0)
		return true;

	return false;
}

/*
 * Returns true if all tokens was read
 */
static bool
tokenizer_eol(TokenizerState *state)
{
	if (state->saved_token_is_valid)
		return false;

	while (*state->str)
	{
		if (!isspace(*state->str))
			return false;

		state->str += 1;
	}

	return true;
}

static void
initialize_tokenizer(TokenizerState *state, const char *str)
{
	state->str = str;
	state->saved_token_is_valid = false;
}

static char *
make_ident(PragmaTokenType *token)
{
	if (token->value == PRAGMA_TOKEN_IDENTIF)
	{
		return downcase_truncate_identifier(token->substr,
											(int) token->size,
											false);
	}
	else if (token->value == PRAGMA_TOKEN_QIDENTIF)
	{
		char	   *result = palloc(token->size);
		const char *ptr = token->substr + 1;
		char	   *write_ptr;
		size_t		n = token->size - 2;

		write_ptr = result;

		while (n-- > 0)
		{
			*write_ptr++ = *ptr;
			if (*ptr++ == '"')
			{
				ptr += 1;
				n -= 1;
			}
		}

		*write_ptr = '\0';

		truncate_identifier(result, (int) (write_ptr - result), false);

		return result;
	}
	else if (token->value == PRAGMA_TOKEN_STRING)
	{
		char	   *str = make_string(token);

		/* does same conversion like varchar->name */
		truncate_identifier(str, (int) strlen(str), false);

		return str;
	}

	return NULL;
}

static char *
make_string(PragmaTokenType *token)
{
	if (token->value == PRAGMA_TOKEN_IDENTIF ||
		token->value == PRAGMA_TOKEN_QIDENTIF)
		return make_ident(token);
	else if (token->value == PRAGMA_TOKEN_NUMBER)
		return pnstrdup(token->substr, token->size);
	else if (token->value == PRAGMA_TOKEN_STRING)
	{
		char	   *result = palloc(token->size);
		const char *ptr = token->substr + 1;
		char	   *write_ptr;
		size_t		n = token->size - 2;

		write_ptr = result;

		while (n-- > 0)
		{
			*write_ptr++ = *ptr;
			if (*ptr++ == '\'')
			{
				ptr += 1;
				n -= 1;
			}
		}

		*write_ptr = '\0';

		return result;
	}

	return NULL;
}

/*
 * Returns list of strings used in qualified identifiers
 */
static List *
get_qualified_identifier(TokenizerState *state, List *result)
{
	bool		read_atleast_one = false;

	while (1)
	{
		PragmaTokenType token,
				   *_token;

		_token = get_token(state, &token);
		if (!_token)
			break;

		if (_token->value != PRAGMA_TOKEN_IDENTIF && _token->value != PRAGMA_TOKEN_QIDENTIF)
			elog(ERROR, "Syntax error (expected identifier)");

		result = lappend(result, make_ident(_token));
		read_atleast_one = true;

		_token = get_token(state, &token);
		if (!_token)
			break;

		if (_token->value != '.')
		{
			unget_token(state, _token);
			break;
		}
	}

	if (!read_atleast_one)
		elog(ERROR, "Syntax error (expected identifier)");

	return result;
}

/*
 * Set start position and length of qualified identifier. Returns true,
 * if parsed identifier is valid.
 */
static void
parse_qualified_identifier(TokenizerState *state, const char **startptr, size_t *size)
{
	bool		read_atleast_one = false;
	const char *_startptr = *startptr;
	size_t		_size = 0;

	while (1)
	{
		PragmaTokenType token,
				   *_token;

		_token = get_token(state, &token);
		if (!_token)
			break;

		if (_token->value != PRAGMA_TOKEN_IDENTIF && _token->value != PRAGMA_TOKEN_QIDENTIF)
			elog(ERROR, "Syntax error (expected identifier)");

		if (!_startptr)
		{
			_startptr = _token->substr;
			_size = _token->size;
		}
		else
			_size = _token->substr - _startptr + _token->size;

		read_atleast_one = true;

		_token = get_token(state, &token);
		if (!_token)
			break;

		if (_token->value != '.')
		{
			unget_token(state, _token);
			break;
		}
	}

	if (!read_atleast_one)
		elog(ERROR, "Syntax error (expected identifier)");

	*startptr = _startptr;
	*size = _size;
}

/*
 * When rectype is not allowed, then composite type is allowed only
 * on top level.
 */
static Oid
get_type_internal(TokenizerState *state, int32 *typmod, bool allow_rectype, bool istop)
{
	PragmaTokenType token,
			   *_token;
	const char *typename_start = NULL;
	size_t		typename_length = 0;
	const char *typestr;
	TypeName   *typeName = NULL;
	Oid			typtype;

	_token = get_token(state, &token);
	if (!_token)
		elog(ERROR, "Syntax error (expected identifier)");

	if (_token->value == '(')
	{
		TupleDesc	resultTupleDesc;
		List	   *names = NIL;
		List	   *types = NIL;
		List	   *typmods = NIL;
		List	   *collations = NIL;

		if (!allow_rectype && !istop)
			elog(ERROR, "Cannot to create table with pseudo-type record.");

		_token = get_token(state, &token);
		if (token_is_keyword(_token, "like"))
		{
			typtype = get_type_internal(state, typmod, allow_rectype, false);
			if (!type_is_rowtype(typtype))
				elog(ERROR, "\"%s\" is not composite type",
					 format_type_be(typtype));

			_token = get_token(state, &token);
			if (!_token ||
				_token->value != ')')
				elog(ERROR, "Syntax error (expected \")\")");

			return typtype;
		}
		else
			unget_token(state, _token);

		while (1)
		{
			Oid			_typtype;
			int32		_typmod;

			_token = get_token(state, &token);
			if (!_token ||
				(_token->value != PRAGMA_TOKEN_IDENTIF &&
				 _token->value != PRAGMA_TOKEN_QIDENTIF))
				elog(ERROR, "Syntax error (expected identifier)");

			names = lappend(names, makeString(make_ident(_token)));

			_typtype = get_type_internal(state, &_typmod, allow_rectype, false);

			types = lappend_oid(types, _typtype);
			typmods = lappend_int(typmods, _typmod);
			collations = lappend_oid(collations, InvalidOid);

			_token = get_token(state, &token);
			if (!_token)
				elog(ERROR, "Syntax error (unclosed composite type definition - expected \")\")");

			if (_token->value == ')')
			{
				break;
			}
			else if (_token->value != ',')
				elog(ERROR, "Syntax error (expected \",\")");
		}

		resultTupleDesc = BuildDescFromLists(names, types, typmods, collations);
		resultTupleDesc = BlessTupleDesc(resultTupleDesc);

		*typmod = resultTupleDesc->tdtypmod;

		return resultTupleDesc->tdtypeid;
	}
	else if (_token->value == PRAGMA_TOKEN_QIDENTIF)
	{
		unget_token(state, _token);

		parse_qualified_identifier(state, &typename_start, &typename_length);
	}
	else if (_token->value == PRAGMA_TOKEN_IDENTIF)
	{
		PragmaTokenType token2,
				   *_token2;

		_token2 = get_token(state, &token2);

		if (_token2)
		{
			if (_token2->value == '.')
			{
				typename_start = _token->substr;
				typename_length = _token->size;

				parse_qualified_identifier(state, &typename_start, &typename_length);
			}
			else
			{
				/* multi word type name */
				typename_start = _token->substr;
				typename_length = _token->size;

				while (_token2 && _token2->value == PRAGMA_TOKEN_IDENTIF)
				{
					typename_length = _token2->substr + _token2->size - typename_start;

					_token2 = get_token(state, &token2);
				}

				unget_token(state, _token2);
			}
		}
		else
		{
			typename_start = _token->substr;
			typename_length = _token->size;
		}
	}
	else
		elog(ERROR, "Syntax error (expected identifier)");

	/* get typmod */
	_token = get_token(state, &token);
	if (_token)
	{
		if (_token->value == '(')
		{
			while (1)
			{
				_token = get_token(state, &token);
				if (!_token || _token->value != PRAGMA_TOKEN_NUMBER)
					elog(ERROR, "Syntax error (expected number for typmod specification)");

				_token = get_token(state, &token);
				if (!_token)
					elog(ERROR, "Syntax error (unclosed typmod specification)");

				if (_token->value == ')')
				{
					break;
				}
				else if (_token->value != ',')
					elog(ERROR, "Syntax error (expected \",\" in typmod list)");
			}

			typename_length = _token->substr + _token->size - typename_start;
		}
		else
			unget_token(state, _token);
	}

	/* get array symbols */
	_token = get_token(state, &token);
	if (_token)
	{
		if (_token->value == '[')
		{
			_token = get_token(state, &token);
			if (_token && _token->value == PRAGMA_TOKEN_NUMBER)
				_token = get_token(state, &token);

			if (!_token)
				elog(ERROR, "Syntax error (unclosed array specification)");

			if (_token->value != ']')
				elog(ERROR, "Syntax error (expected \"]\")");

			typename_length = _token->substr + _token->size - typename_start;
		}
		else
			unget_token(state, _token);
	}

	typestr = pnstrdup(typename_start, typename_length);


#if PG_VERSION_NUM >= 160000

	typeName = typeStringToTypeName(typestr, NULL);

#else

	typeName = typeStringToTypeName(typestr);

#endif

	typenameTypeIdAndMod(NULL, typeName, &typtype, typmod);

	return typtype;
}

static Oid
get_type(TokenizerState *state, int32 *typmod, bool allow_rectype)
{
	return get_type_internal(state, typmod, allow_rectype, true);
}

static int
get_varno(PLpgSQL_nsitem *cur_ns, List *names)
{
	char	   *name1 = NULL;
	char	   *name2 = NULL;
	char	   *name3 = NULL;
	int			names_used;
	PLpgSQL_nsitem *nsitem;

	switch (list_length(names))
	{
		case 1:
			{
				name1 = (char *) linitial(names);
				break;
			}
		case 2:
			{
				name1 = (char *) linitial(names);
				name2 = (char *) lsecond(names);
				break;
			}
		case 3:
			{
				name1 = (char *) linitial(names);
				name2 = (char *) lsecond(names);
				name3 = (char *) lthird(names);
				break;
			}
		default:
			return -1;
	}

	nsitem = plpgsql_check__ns_lookup_p(cur_ns, false, name1, name2, name3, &names_used);

	return nsitem ? nsitem->itemno : -1;
}

static char *
get_name(List *names)
{
	bool		is_first = true;
	ListCell   *lc;

	StringInfoData sinfo;

	initStringInfo(&sinfo);

	foreach(lc, names)
	{
		if (is_first)
			is_first = false;
		else
			appendStringInfoChar(&sinfo, '.');

		appendStringInfo(&sinfo, "\"%s\"", (char *) lfirst(lc));
	}

	return sinfo.data;
}

static const char *
pragma_assert_name(PragmaAssertType pat)
{
	switch (pat)
	{
		case PLPGSQL_CHECK_PRAGMA_ASSERT_SCHEMA:
			return "assert-schema";
		case PLPGSQL_CHECK_PRAGMA_ASSERT_TABLE:
			return "assert-table";
		case PLPGSQL_CHECK_PRAGMA_ASSERT_COLUMN:
			return "assert-column";
	}

	return NULL;
}

static Oid
check_var_schema(PLpgSQL_checkstate *cstate, int dno)
{
	return get_namespace_oid(cstate->strconstvars[dno], true);
}

static Oid
check_var_table(PLpgSQL_checkstate *cstate, int dno1, int dno2)
{
	char	   *relname = cstate->strconstvars[dno2];
	Oid			relid = InvalidOid;

	if (dno1 != -1)
		relid = get_relname_relid(relname, check_var_schema(cstate, dno1));
	else
		relid = RelnameGetRelid(relname);

	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("table \"%s\" does not exist", relname)));

	return relid;
}

static AttrNumber
check_var_column(PLpgSQL_checkstate *cstate, int dno1, int dno2, int dno3)
{
	char	   *attname = cstate->strconstvars[dno3];
	Oid			relid = check_var_table(cstate, dno1, dno2);
	AttrNumber	attnum;

	attnum = get_attnum(relid, attname);
	if (attnum == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\".\"%s\" does not exist",
						attname,
						get_namespace_name(get_rel_namespace(relid)),
						get_rel_name(relid))));

	return attnum;
}

bool
plpgsql_check_pragma_assert(PLpgSQL_checkstate *cstate,
							PragmaAssertType pat,
							const char *str,
							PLpgSQL_nsitem *ns,
							int lineno)
{
	MemoryContext oldCxt;
	ResourceOwner oldowner;
	volatile int dno[3];
	volatile int nvars = 0;
	volatile bool result = true;

	/*
	 * namespace is available only in compile check mode, and only in this
	 * mode this pragma can be used.
	 */
	if (!ns || !cstate)
		return true;

	oldCxt = CurrentMemoryContext;

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(cstate->check_cxt);

	PG_TRY();
	{
		TokenizerState tstate;
		int			i;
		List	   *names;

		initialize_tokenizer(&tstate, str);

		for (i = 0; i < 3; i++)
		{
			if (i > 0)
			{
				PragmaTokenType token,
						   *_token;

				_token = get_token(&tstate, &token);
				if (_token->value != ',')
					elog(ERROR, "Syntax error (expected \",\")");
			}

			names = get_qualified_identifier(&tstate, NULL);
			if ((dno[i] = get_varno(ns, names)) == -1)
				elog(ERROR, "Cannot to find variable %s used in \"%s\" pragma",
					 get_name(names),
					 pragma_assert_name(pat));

			if (!cstate->strconstvars || !cstate->strconstvars[dno[i]])
				elog(ERROR, "Variable %s has not assigned constant",
					 get_name(names));

			nvars += 1;

			if (tokenizer_eol(&tstate))
				break;
		}

		if (!tokenizer_eol(&tstate))
			elog(ERROR, "Syntax error (unexpected chars after variable)");

		if ((pat == PLPGSQL_CHECK_PRAGMA_ASSERT_SCHEMA && nvars > 1) ||
			(pat == PLPGSQL_CHECK_PRAGMA_ASSERT_TABLE && nvars > 2) ||
			(pat == PLPGSQL_CHECK_PRAGMA_ASSERT_COLUMN && nvars > 3))
			elog(ERROR, "too much variables for \"%s\" pragma",
				 pragma_assert_name(pat));

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(cstate->check_cxt);
		edata = CopyErrorData();
		FlushErrorState();

		MemoryContextSwitchTo(oldCxt);
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		/* raise warning (errors in pragma can be ignored instead */
		ereport(WARNING,
				(errmsg("\"%s\" on line %d is not processed.", pragma_assert_name(pat), lineno),
				 errdetail("%s", edata->message)));

		result = false;
	}
	PG_END_TRY();

	if (!result)
		return false;

	if (pat == PLPGSQL_CHECK_PRAGMA_ASSERT_SCHEMA)
	{
		(void) check_var_schema(cstate, dno[0]);
	}
	else if (pat == PLPGSQL_CHECK_PRAGMA_ASSERT_TABLE)
	{
		if (nvars == 1)
			(void) check_var_table(cstate, -1, dno[0]);
		else
			(void) check_var_table(cstate, dno[0], dno[1]);
	}
	else if (pat == PLPGSQL_CHECK_PRAGMA_ASSERT_COLUMN)
	{
		if (nvars == 2)
			(void) check_var_column(cstate, -1, dno[0], dno[1]);
		else
			(void) check_var_column(cstate, dno[0], dno[1], dno[2]);
	}

	return result;
}

bool
plpgsql_check_pragma_type(PLpgSQL_checkstate *cstate,
						  const char *str,
						  PLpgSQL_nsitem *ns,
						  int lineno)
{
	MemoryContext oldCxt;
	ResourceOwner oldowner;
	volatile bool result = true;

	/*
	 * namespace is available only in compile check mode, and only in this
	 * mode this pragma can be used.
	 */
	if (!ns || !cstate)
		return true;

	oldCxt = CurrentMemoryContext;

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(cstate->check_cxt);

	PG_TRY();
	{
		TokenizerState tstate;
		int			target_dno;
		PLpgSQL_datum *target;
		List	   *names;
		Oid			typtype;
		int32		typmod;
		TupleDesc	typtupdesc;

		initialize_tokenizer(&tstate, str);

		names = get_qualified_identifier(&tstate, NULL);
		if ((target_dno = get_varno(ns, names)) == -1)
			elog(ERROR, "Cannot to find variable %s used in settype pragma", get_name(names));

		target = cstate->estate->datums[target_dno];
		if (target->dtype != PLPGSQL_DTYPE_REC)
			elog(ERROR, "Pragma \"settype\" can be applied only on variable of record type");

		typtype = get_type(&tstate, &typmod, true);

		if (!tokenizer_eol(&tstate))
			elog(ERROR, "Syntax error (unexpected chars after type specification)");

		typtupdesc = lookup_rowtype_tupdesc_copy(typtype, typmod);
		plpgsql_check_assign_tupdesc_dno(cstate, target_dno, typtupdesc, false);

		cstate->typed_variables = bms_add_member(cstate->typed_variables, target_dno);

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(cstate->check_cxt);
		edata = CopyErrorData();
		FlushErrorState();

		MemoryContextSwitchTo(oldCxt);
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		/* raise warning (errors in pragma can be ignored instead */
		ereport(WARNING,
				(errmsg("Pragma \"type\" on line %d is not processed.", lineno),
				 errdetail("%s", edata->message)));

		result = false;
	}
	PG_END_TRY();

	return result;
}

/*
 * Unfortunately the ephemeral tables introduced in PostgreSQL 10 cannot be
 * used for this purpose, because any DML operations are prohibited, and others
 * DML catalogue operations doesn't calculate with Ephemeral space.
 */
bool
plpgsql_check_pragma_table(PLpgSQL_checkstate *cstate, const char *str, int lineno)
{
	MemoryContext oldCxt;
	ResourceOwner oldowner;
	volatile bool result = true;

	if (!cstate)
		return true;

	oldCxt = CurrentMemoryContext;

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(cstate->check_cxt);

	PG_TRY();
	{
		TokenizerState tstate;
		PragmaTokenType token,
				   *_token;
		PragmaTokenType token2,
				   *_token2;
		StringInfoData query;
		int32		typmod;

		initialize_tokenizer(&tstate, str);

		_token = get_token(&tstate, &token);
		if (!_token || (_token->value != PRAGMA_TOKEN_IDENTIF
						&& _token->value != PRAGMA_TOKEN_QIDENTIF))
			elog(ERROR, "Syntax error (expected identifier)");

		_token2 = get_token(&tstate, &token2);

		if (_token2 && _token2->value == '.')
		{
			char	   *nsname = make_ident(_token);

			if (strcmp(nsname, "pg_temp") != 0)
				elog(ERROR, "schema \"%s\" cannot be used in pragma \"table\" (only \"pg_temp\" schema is allowed)", nsname);

			_token = get_token(&tstate, &token);
			if (!_token || (_token->value != PRAGMA_TOKEN_IDENTIF
							&& _token->value != PRAGMA_TOKEN_QIDENTIF))
				elog(ERROR, "Syntax error (expected identifier)");

			_token2 = get_token(&tstate, &token2);
		}

		if (!_token2 || _token2->value != '(')
			elog(ERROR, "Syntax error (expected table specification)");

		unget_token(&tstate, _token2);

		(void) get_type(&tstate, &typmod, false);

		if (!tokenizer_eol(&tstate))
			elog(ERROR, "Syntax error (unexpected chars after table specification)");

		/* In this case we use parser just for syntax check and security check */
		initStringInfo(&query);
		appendStringInfoString(&query, "CREATE TEMP TABLE ");
		appendStringInfoString(&query, str);

		if (SPI_execute(query.data, false, 0) != SPI_OK_UTILITY)
			elog(NOTICE, "Cannot to create temporary table");

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(cstate->check_cxt);
		edata = CopyErrorData();
		FlushErrorState();

		MemoryContextSwitchTo(oldCxt);
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		/* raise warning (errors in pragma can be ignored instead */
		ereport(WARNING,
				(errmsg("Pragma \"table\" on line %d is not processed.", lineno),
				 errdetail("%s", edata->message)));

		result = false;
	}
	PG_END_TRY();

	return result;
}

/*
 * An sequence can be temporary too, so there should be related PRAGMA
 */
bool
plpgsql_check_pragma_sequence(PLpgSQL_checkstate *cstate, const char *str, int lineno)
{
	MemoryContext oldCxt;
	ResourceOwner oldowner;
	volatile bool result = true;

	if (!cstate)
		return true;

	oldCxt = CurrentMemoryContext;

	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(cstate->check_cxt);

	PG_TRY();
	{
		TokenizerState tstate;
		PragmaTokenType token,
				   *_token;
		PragmaTokenType token2,
				   *_token2;
		StringInfoData query;

		initialize_tokenizer(&tstate, str);

		_token = get_token(&tstate, &token);
		if (!_token || (_token->value != PRAGMA_TOKEN_IDENTIF
						&& _token->value != PRAGMA_TOKEN_QIDENTIF))
			elog(ERROR, "Syntax error (expected identifier)");

		_token2 = get_token(&tstate, &token2);

		if (_token2 && _token2->value == '.')
		{
			char	   *nsname = make_ident(_token);

			if (strcmp(nsname, "pg_temp") != 0)
				elog(ERROR, "schema \"%s\" cannot be used in pragma \"sequence\" (only \"pg_temp\" schema is allowed)", nsname);

			_token = get_token(&tstate, &token);
			if (!_token || (_token->value != PRAGMA_TOKEN_IDENTIF
							&& _token->value != PRAGMA_TOKEN_QIDENTIF))
				elog(ERROR, "Syntax error (expected identifier)");

			(void) get_token(&tstate, &token2);
		}

		if (!tokenizer_eol(&tstate))
			elog(ERROR, "Syntax error (unexpected chars after sequence name)");

		/* In this case we use parser just for syntax check and security check */
		initStringInfo(&query);
		appendStringInfoString(&query, "CREATE TEMP SEQUENCE ");
		appendStringInfoString(&query, str);

		if (SPI_execute(query.data, false, 0) != SPI_OK_UTILITY)
			elog(NOTICE, "Cannot to create temporary sequence");

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(cstate->check_cxt);
		edata = CopyErrorData();
		FlushErrorState();

		MemoryContextSwitchTo(oldCxt);
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		/* raise warning (errors in pragma can be ignored instead */
		ereport(WARNING,
				(errmsg("Pragma \"sequence\" on line %d is not processed.", lineno),
				 errdetail("%s", edata->message)));

		result = false;
	}
	PG_END_TRY();

	return result;
}

static bool
get_boolean_comment_option(TokenizerState *tstate, const char *name, plpgsql_check_info *cinfo)
{
	PragmaTokenType token,
			   *_token;

	_token = get_token(tstate, &token);
	if (!_token)
		return true;

	if (_token->value == ',')
	{
		unget_token(tstate, _token);
		return true;
	}

	if (_token->value == '=')
	{
		_token = get_token(tstate, &token);
		if (!_token)
			elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected boolean value after \"=\")",
				 name, cinfo->fn_oid);
	}

	if (token_is_keyword(_token, "true") ||
		token_is_keyword(_token, "yes") ||
		token_is_keyword(_token, "t") ||
		token_is_keyword(_token, "on"))
		return true;
	else if (token_is_keyword(_token, "false") ||
			 token_is_keyword(_token, "no") ||
			 token_is_keyword(_token, "f") ||
			 token_is_keyword(_token, "off"))
		return false;
	else
		elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected boolean value)",
			 name, cinfo->fn_oid);

	/* fix warning C4715 on msvc */
	Assert(0);
	return false;
}

static char *
get_name_comment_option(TokenizerState *tstate, const char *name, plpgsql_check_info *cinfo)
{
	PragmaTokenType token,
			   *_token;

	_token = get_token(tstate, &token);
	if (!_token)
		elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected option's argument of name type)",
			 name, cinfo->fn_oid);

	if (_token->value == '=')
	{
		_token = get_token(tstate, &token);
		if (!_token)
			elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected name value after \"=\")",
				 name, cinfo->fn_oid);
	}

	if (_token->value == PRAGMA_TOKEN_IDENTIF ||
		_token->value == PRAGMA_TOKEN_QIDENTIF ||
		_token->value == PRAGMA_TOKEN_STRING)
	{
		return pstrdup(make_ident(_token));
	}
	else
		elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected SQL identifier as argument)",
			 name, cinfo->fn_oid);

	/* fix warning C4715 on msvc */
	Assert(0);
	return NULL;
}

static Oid
get_type_comment_option(TokenizerState *tstate, const char *name, plpgsql_check_info *cinfo)
{
	PragmaTokenType token,
			   *_token;

	_token = get_token(tstate, &token);
	if (!_token)
		elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected option's argument of type name)",
			 name, cinfo->fn_oid);

	if (_token->value == '=')
	{
		_token = get_token(tstate, &token);
		if (!_token)
			elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected type name after \"=\")",
				 name, cinfo->fn_oid);
	}

	if (_token->value == PRAGMA_TOKEN_IDENTIF ||
		_token->value == PRAGMA_TOKEN_QIDENTIF)
	{
		const char *typname_start = NULL;
		size_t		typname_length;
		char	   *typestr;
		Oid			typid;
		int32		typmod;

		unget_token(tstate, _token);

		parse_qualified_identifier(tstate, &typname_start, &typname_length);

		typestr = pnstrdup(typname_start, typname_length);
		parseTypeString(typestr, &typid, &typmod, false);

		return typid;
	}
	else
		elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected type identifier)",
			 name, cinfo->fn_oid);

	/* fix warning C4715 on msvc */
	Assert(0);
	return InvalidOid;
}

static Oid
get_table_comment_option(TokenizerState *tstate, const char *name, plpgsql_check_info *cinfo)
{
	PragmaTokenType token,
			   *_token;

	_token = get_token(tstate, &token);
	if (!_token)
		elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected option's argument of table name)",
			 name, cinfo->fn_oid);

	if (_token->value == '=')
	{
		_token = get_token(tstate, &token);
		if (!_token)
			elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected table name after \"=\")",
				 name, cinfo->fn_oid);
	}

	if (_token->value == PRAGMA_TOKEN_IDENTIF ||
		_token->value == PRAGMA_TOKEN_QIDENTIF)
	{
		List	   *names;
		const char *tablename_start = NULL;
		size_t		tablename_length;
		char	   *tablenamestr;
		Oid			result;

		unget_token(tstate, _token);

		parse_qualified_identifier(tstate, &tablename_start, &tablename_length);

		tablenamestr = pnstrdup(tablename_start, tablename_length);

#if PG_VERSION_NUM >= 160000

		names = stringToQualifiedNameList(tablenamestr, NULL);

#else

		names = stringToQualifiedNameList(tablenamestr);

#endif

		/* We might not even have permissions on this relation; don't lock it. */
		result = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, false);

		return result;
	}
	else
		elog(ERROR, "syntax error in comment option \"%s\" (fnoid: %u) (expected table identifier)",
			 name, cinfo->fn_oid);

	/* fix warning C4715 on msvc */
	Assert(0);
	return InvalidOid;
}

static bool
is_keyword(char *str, size_t bytes, const char *keyword)
{
	if (bytes != strlen(keyword))
		return false;

	if (strncasecmp(str, keyword, bytes) != 0)
		return false;

	return true;
}

char *
plpgsql_check_process_echo_string(char *str, plpgsql_check_info *cinfo)
{
	StringInfoData sinfo;

	initStringInfo(&sinfo);

	while (*str)
	{
		if (*str == '@' && str[1] == '@')
		{
			char	   *start;
			size_t		bytes;

			str += 2;
			start = str;

			while (*str && isalpha(*str))
			{
				str += 1;
			}

			bytes = str - start;
			if (is_keyword(start, bytes, "id"))
			{
				appendStringInfo(&sinfo, "%u", cinfo->fn_oid);
			}
			else if (is_keyword(start, bytes, "name"))
			{
				appendStringInfoString(&sinfo, get_func_name(cinfo->fn_oid));
			}
			else if (is_keyword(start, bytes, "signature"))
			{
				appendStringInfoString(&sinfo, format_procedure(cinfo->fn_oid));
			}
			else
				appendStringInfo(&sinfo, "@@%.*s", (int) bytes, start);
		}
		else
			appendStringInfoChar(&sinfo, *str++);
	}

	return sinfo.data;
}

static void
comment_options_parser(char *str, plpgsql_check_info *cinfo)
{
	TokenizerState tstate;
	PragmaTokenType token,
			   *_token;

	initialize_tokenizer(&tstate, str);

	do
	{
		_token = get_token(&tstate, &token);

		if (!_token || (_token->value != PRAGMA_TOKEN_IDENTIF))
			elog(ERROR, "Syntax error (fnoid: %u) (expected option name)", cinfo->fn_oid);

		if (cinfo->incomment_options_usage_warning)
			elog(WARNING, "comment option \"%s\" is used (oid: %u)", make_ident(_token), cinfo->fn_oid);

		if (token_is_keyword(_token, "relid"))
		{
			cinfo->relid = get_table_comment_option(&tstate, "relid", cinfo);
		}
		else if (token_is_keyword(_token, "fatal_errors"))
		{
			cinfo->fatal_errors = get_boolean_comment_option(&tstate, "fatal_errors", cinfo);
		}
		else if (token_is_keyword(_token, "other_warnings"))
		{
			cinfo->other_warnings = get_boolean_comment_option(&tstate, "other_warnings", cinfo);
		}
		else if (token_is_keyword(_token, "extra_warnings"))
		{
			cinfo->extra_warnings = get_boolean_comment_option(&tstate, "extra_warnings", cinfo);
		}
		else if (token_is_keyword(_token, "performance_warnings"))
		{
			cinfo->performance_warnings = get_boolean_comment_option(&tstate, "performance_warnings", cinfo);
		}
		else if (token_is_keyword(_token, "security_warnings"))
		{
			cinfo->security_warnings = get_boolean_comment_option(&tstate, "security_warnings", cinfo);
		}
		else if (token_is_keyword(_token, "compatibility_warnings"))
		{
			cinfo->compatibility_warnings = get_boolean_comment_option(&tstate, "compatibility_warnings", cinfo);
		}
		else if (token_is_keyword(_token, "anyelementtype"))
		{
			cinfo->anyelementoid = get_type_comment_option(&tstate, "anyelementtype", cinfo);
		}
		else if (token_is_keyword(_token, "anyenumtype"))
		{
			cinfo->anyenumoid = get_type_comment_option(&tstate, "anyenumtype", cinfo);
		}
		else if (token_is_keyword(_token, "anyrangetype"))
		{
			cinfo->anyrangeoid = get_type_comment_option(&tstate, "anyrangetype", cinfo);
			if (!type_is_range(cinfo->anyrangeoid))
				elog(ERROR, "the type specified by \"anyrangetype\" comment option is not range (fnoid: %u)",
					 cinfo->fn_oid);
		}
		else if (token_is_keyword(_token, "anycompatibletype"))
		{
			cinfo->anycompatibleoid = get_type_comment_option(&tstate, "anycompatibletype", cinfo);
		}
		else if (token_is_keyword(_token, "anycompatiblerangetype"))
		{
			cinfo->anycompatiblerangeoid = get_type_comment_option(&tstate, "anycompatiblerangetype", cinfo);
			if (!type_is_range(cinfo->anycompatiblerangeoid))
				elog(ERROR, "the type specified by \"anycompatiblerangetype\" comment option is not range (fnoid: %u)",
					 cinfo->fn_oid);
		}
		else if (token_is_keyword(_token, "without_warnings"))
		{
			cinfo->without_warnings = get_boolean_comment_option(&tstate, "without_warnings", cinfo);
		}
		else if (token_is_keyword(_token, "all_warnings"))
		{
			cinfo->all_warnings = get_boolean_comment_option(&tstate, "all_warnings", cinfo);
		}
		else if (token_is_keyword(_token, "newtable"))
		{
			cinfo->newtable = get_name_comment_option(&tstate, "newtable", cinfo);
		}
		else if (token_is_keyword(_token, "oldtable"))
		{
			cinfo->oldtable = get_name_comment_option(&tstate, "oldtable", cinfo);
		}
		else if (token_is_keyword(_token, "echo"))
		{
			_token = get_token(&tstate, &token);
			if (!_token)
				elog(ERROR, "missing argument of option \"echo\"");

			if (_token->value == '=')
			{
				_token = get_token(&tstate, &token);
				if (!_token)
					elog(ERROR, "expected value after \"=\"");
			}

			if (_token->value == PRAGMA_TOKEN_IDENTIF)
				elog(NOTICE, "comment option \"echo\" is %s", plpgsql_check_process_echo_string(make_string(_token), cinfo));
			else if (_token->value == PRAGMA_TOKEN_QIDENTIF)
				elog(NOTICE, "comment option \"echo\" is \"%s\"", plpgsql_check_process_echo_string(make_string(_token), cinfo));
			else if (_token->value == PRAGMA_TOKEN_NUMBER)
				elog(NOTICE, "comment option \"echo\" is %s", plpgsql_check_process_echo_string(make_string(_token), cinfo));
			else if (_token->value == PRAGMA_TOKEN_STRING)
				elog(NOTICE, "comment option \"echo\" is '%s'", plpgsql_check_process_echo_string(make_string(_token), cinfo));
			else
				elog(NOTICE, "comment option \"echo\" is '%c'", _token->value);
		}
		else
			elog(ERROR, "unsupported option \"%.*s\" specified by \"@plpgsql_check_options\" (fnoid: %u)",
				 (int) _token->size, _token->substr, cinfo->fn_oid);

		_token = get_token(&tstate, &token);
		if (!_token)
			break;

		if (_token->value != ',')
			elog(ERROR, "expected \",\" or end of line on line with \"@plpgsql_check_options\" options (fnoid: %u)",
				 cinfo->fn_oid);
	}
	while (_token);
}


static void
comment_options_parsecontent(char *str, size_t bytes, plpgsql_check_info *cinfo)
{
	char	   *endchar = str + bytes;

	do
	{
		char	   *ptr,
				   *optsline;
		bool		found_eol;

		str += strlen(tagstr);

		Assert(str <= endchar);

		/* find end of line */
		ptr = str;
		found_eol = false;

		while (ptr < endchar && *ptr)
		{
			if (*ptr == '\n')
			{
				found_eol = true;
				break;
			}

			ptr += 1;
		}

		optsline = pnstrdup(str, found_eol ? ptr - str : endchar - str);

		comment_options_parser(optsline, cinfo);

		pfree(optsline);

		if (!found_eol || ptr >= endchar)
			break;

		str = memmem(ptr + 1, endchar - (ptr + 1),
					 tagstr, strlen(tagstr));
	}
	while (str);
}

static char *
search_comment_options_linecomment(char *src, plpgsql_check_info *cinfo)
{
	char	   *start = src;

	while (*src)
	{
		if (*src == '\n')
		{
			char	   *tag;

			tag = memmem(start, src - start,
						 tagstr, strlen(tagstr));
			if (tag)
				comment_options_parsecontent(tag, src - tag, cinfo);

			return src + 1;
		}

		src += 1;
	}

	return src;
}

static char *
search_comment_options_multilinecomment(char *src, plpgsql_check_info *cinfo)
{
	char	   *start = src;

	while (*src)
	{
		if (*src == '*' && src[1] == '/')
		{
			char	   *tag;

			tag = memmem(start, src - start,
						 tagstr, strlen(tagstr));
			if (tag)
				comment_options_parsecontent(tag, src - tag, cinfo);

			return src + 1;
		}

		src += 1;
	}

	return src;
}

/*
 * Try to read plpgsql_check options.
 *
 */
void
plpgsql_check_search_comment_options(plpgsql_check_info *cinfo)
{
	char	   *src = plpgsql_check_get_src(cinfo->proctuple);

	cinfo->all_warnings = false;
	cinfo->without_warnings = false;

	while (*src)
	{
		if (*src == '-' && src[1] == '-')
			src = search_comment_options_linecomment(src + 2, cinfo);

		else if (*src == '/' && src[1] == '*')
			src = search_comment_options_multilinecomment(src + 2, cinfo);

		else if (*src == '\'')
		{
			src++;

			while (*src)
			{
				if (*src++ == '\'')
				{
					if (*src == '\'')
						src += 1;
					else
						break;
				}
			}
		}

		else if (*src == '"')
		{
			src++;

			while (*src)
			{
				if (*src++ == '"')
				{
					if (*src == '"')
						src += 1;
					else
						break;
				}
			}
		}

		else if (*src == '$')
		{
			char	   *start = src++;
			bool		is_custom_string = false;

			while (*src)
			{
				if (isblank(*src))
				{
					is_custom_string = false;
					break;
				}
				else if (*src == '$')
				{
					is_custom_string = true;
					break;
				}

				src += 1;
			}

			if (is_custom_string)
			{
				size_t		cust_str_length = 0;

				cust_str_length = src - start + 1;

		next_char:

				src += 1;

				if (*src)
				{
					size_t		i;

					for (i = 0; i < cust_str_length; i++)
					{
						if (src[i] != start[i])
							goto next_char;
					}

					/* found complete custom string separator */
					src += cust_str_length;
				}
			}
			else
				src = start + 1;
		}
		else
			src += 1;
	}

	if (cinfo->all_warnings && cinfo->without_warnings)
		elog(ERROR, "all_warnings and without_warnings cannot be used together (fnoid: %u)",
			 cinfo->fn_oid);

	if (cinfo->all_warnings)
		plpgsql_check_set_all_warnings(cinfo);
	else if (cinfo->without_warnings)
		plpgsql_check_set_without_warnings(cinfo);
}
