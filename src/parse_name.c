/*-------------------------------------------------------------------------
 *
 * parse_name.c
 *
 *			  parse function signature
 *			  parse identifier, and type name
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
#if PG_VERSION_NUM >= 140000
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

#define		TOKEN_IDENTIF		128
#define		TOKEN_QIDENTIF		129
#define		TOKEN_NUMBER		130

typedef struct
{
	int		value;
	const char *start_token;
	int		size;
} TokenType;

typedef struct
{
	const char *str;
	const char *laststr;
	const char *fullstr;
	int start;
	TokenType	saved_token;
	bool		saved_token_is_valid;
	bool	is_error;
} TokenizerState;

static void
pragma_tokenizer_error(TokenizerState *state, const char *errdetail)
{
	/* Raise warning only when it was not yet */
	if (!state->is_error)
	{
		if (state->laststr && *state->laststr)
		{
			/*
			 * 20 is length of name "plpgsql_check_pragma",
			 * This is little bit smutty, because we have not
			 * real initial position of pragma expression
			 */
			ereport(WARNING,
				(errmsg("Syntax error in pragma (%s)", errdetail),
				 errposition(state->laststr - state->fullstr + 20 + state->start)));
		}
		else if (state->str && *state->str)
		{
			ereport(WARNING,
				(errmsg("Syntax error in pragma (%s)", errdetail),
				 errposition(state->str - state->fullstr + 20 + state->start)));
		}
		else
			elog(WARNING, "Unexpected end of pragma (%s)", errdetail);

		state->is_error = true;
	}
}

/*
 * Tokenize text. Only one error is possible here - unclosed double quotes.
 * Returns NULL, when found EOL or on error.
 */
static TokenType *
get_token(TokenizerState *state, TokenType *token)
{
	if (state->is_error)
		return NULL;

	if (state->saved_token_is_valid)
	{
		state->saved_token_is_valid = false;
		return &state->saved_token;
	}

	/* skip inital spaces */
	while (*state->str == ' ')
		state->str++;

	state->laststr = state->str;

	if (!*state->str)
		return NULL;

	if (isdigit(*state->str))
	{
		bool	have_dot = false;

		token->value = TOKEN_NUMBER;
		token->start_token = state->str++;

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

		token->size = state->str - token->start_token;
	}
	else if (*state->str == '"')
	{
		bool	is_error = true;

		token->value = TOKEN_QIDENTIF;
		token->start_token = state->str++;

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
		{
			pragma_tokenizer_error(state, "unclosed quoted identifier");
			return NULL;
		}
	}
	else if (is_ident_start(*state->str))
	{
		token->value = TOKEN_IDENTIF;
		token->start_token = state->str++;

		while (is_ident_cont(*state->str))
			state->str += 1;
	}
	else
		token->value = *state->str++;

	token->size = state->str - token->start_token;


	return token;
}

static void
unget_token(TokenizerState *state, TokenType *token)
{
	if (!state->is_error)
	{
		if (token)
		{
			state->saved_token.value = token->value;
			state->saved_token.start_token = token->start_token;
			state->saved_token.size = token->size;

			state->laststr = token->start_token;

			state->saved_token_is_valid = true;
		}
		else
			state->saved_token_is_valid = false;
	}
}

static void
initialize_tokenizer(TokenizerState *state, const char *str, int start)
{
	state->str = str + start;
	state->fullstr = str;
	state->laststr = NULL;
	state->start = start;
	state->is_error = false;
	state->saved_token_is_valid = false;
}

static List *
get_qualified_identifier(TokenizerState *state, List *result)
{
	TokenType	token, *_token;
	bool	read_atleast_one = false;

	while (1)
	{
		_token = get_token(state, &token);
		if (!_token)
			break;

		if (_token->value != TOKEN_IDENTIF &&
			_token->value != TOKEN_QIDENTIF)
		{
			pragma_tokenizer_error(state, "expected identifier");
			return NULL;
		}

		result = lappend(result, pnstrdup(_token->start_token, _token->size));
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

	if (!read_atleast_one || state->is_error)
	{
		pragma_tokenizer_error(state, "expected identifier");
		return NULL;
	}

	return result;
}

static bool
get_type(TokenizerState *state, Oid *typtype, int32 *typmod)
{
	TokenType	token, *_token;
	const char	   *typename_start = NULL;
	int			typename_length = 0;
	const char	   *typmod_start = NULL;
	int			typmod_length = 0;
	List	   *names = NULL;

	_token = get_token(state, &token);
	if (!_token)
	{
		pragma_tokenizer_error(state, "expected identifier");
		return false;
	}

	if (_token->value == '(')
	{
		while (1)
		{
			Oid		_typtype;
			int32	_typmod;

			_token = get_token(state, &token);
			if (!_token ||
				(_token->value != TOKEN_IDENTIF &&
				 _token->value != TOKEN_QIDENTIF))
			{
				pragma_tokenizer_error(state, "expected identifier");
				return false;
			}

			if (get_type(state, &_typtype, &_typmod))
			{
			}
			else
				return false;

			_token = get_token(state, &token);
			if (!_token)
			{
				pragma_tokenizer_error(state, "unclosed composite type definition - expected \")\"");
				return false;
			}
			else if (_token->value == ')')
			{
				return true;
			}
			else if (_token->value != ',')
			{
				pragma_tokenizer_error(state, "expected \",\"");
				return false;
			}
		}
	}
	else if (_token->value == TOKEN_QIDENTIF)
	{
		unget_token(state, _token);

		names = get_qualified_identifier(state, NULL);
		if (state->is_error)
			return false;
	}
	else if (_token->value == TOKEN_IDENTIF)
	{
		TokenType	token2, *_token2;

		_token2 = get_token(state, &token2);
		if (state->is_error)
			return false;

		if (_token2)
		{
			if (_token2->value == '.')
			{
				names = list_make1(pnstrdup(_token->start_token, _token->size));

				names = get_qualified_identifier(state, names);
				if (state->is_error)
					return false;
			}
			else
			{
				/* multi word type name */
				typename_start = _token->start_token;
				typename_length = _token->size;

				while (_token2 && _token2->value == TOKEN_IDENTIF)
				{
					typename_length = _token2->start_token + _token2->size - typename_start;

					_token2 = get_token(state, &token2);
					if (state->is_error)
						return false;
				}

				unget_token(state, _token2);
			}
		}
	}
	else
	{
		pragma_tokenizer_error(state, "expected identifier");
		return false;
	}

	/* get typmod */
	typmod_start = state->str;

	_token = get_token(state, &token);
	if (state->is_error)
		return InvalidOid;

	if (_token)
	{
		if (_token->value == '(')
		{
			while (1)
			{
				_token = get_token(state, &token);
				if (!_token || _token->value != TOKEN_NUMBER)
				{
					pragma_tokenizer_error(state, "expected number for typmod specification");
					return false;
				}

				_token = get_token(state, &token);
				if (!_token)
				{
					pragma_tokenizer_error(state, "unclosed typmod specification");
					return false;
				}

				if (_token->value == ')')
				{
					typmod_length = _token->start_token + _token->size - typmod_start;
					break;
				}
				else if (_token->value != ',')
				{
					pragma_tokenizer_error(state, "expected \",\" in typmod list");
					return false;
				}
			}
		}
		else
			unget_token(state, _token);
	}

	return true;
}

bool
plpgsql_check_parse_pragma_settype(const char *str, int start, int *varno, Oid *typtype, int32 *typmod)
{
	List	   *names;
	TokenizerState state;

	initialize_tokenizer(&state, str, start);

	names = get_qualified_identifier(&state, NULL);
	if (state.is_error)
		return false;

	if (!get_type(&state, typtype, typmod))
		return false;

	if (state.saved_token_is_valid)
	{
		pragma_tokenizer_error(&state, "unexpected chars after type specification");
		return false;
	}

	state.laststr = NULL;

	while (*state.str)
	{
		if (!isspace(*state.str))
		{
			pragma_tokenizer_error(&state, "unexpected chars after type specification");
			return false;
		}

		state.str += 1;
	}

	return true;
}
