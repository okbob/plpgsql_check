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
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#if PG_VERSION_NUM < 110000

#include "utils/typcache.h"

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
	const char *substr;
	size_t		size;
} TokenType;

typedef struct
{
	const char *str;
	TokenType	saved_token;
	bool		saved_token_is_valid;
} TokenizerState;

/*
 * Tokenize text. Only one error is possible here - unclosed double quotes.
 * Returns NULL, when found EOL or on error.
 */
static TokenType *
get_token(TokenizerState *state, TokenType *token)
{
	if (state->saved_token_is_valid)
	{
		state->saved_token_is_valid = false;
		return &state->saved_token;
	}

	/* skip inital spaces */
	while (*state->str == ' ')
		state->str++;

	if (!*state->str)
		return NULL;

	if (isdigit(*state->str))
	{
		bool	have_dot = false;

		token->value = TOKEN_NUMBER;
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

		token->size = state->str - token->substr;
	}
	else if (*state->str == '"')
	{
		bool	is_error = true;

		token->value = TOKEN_QIDENTIF;
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
	else if (is_ident_start(*state->str))
	{
		token->value = TOKEN_IDENTIF;
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
unget_token(TokenizerState *state, TokenType *token)
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
token_is_keyword(TokenType *token, const char *str)
{
	if (!token)
		return false;

	if (token->value == TOKEN_IDENTIF &&
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
make_ident(TokenType *token)
{
	if (token->value == TOKEN_IDENTIF)
	{
		return downcase_truncate_identifier(token->substr,
											token->size,
											false);
	}
	else if (token->value == TOKEN_QIDENTIF)
	{
		char	   *result = palloc(token->size);
		const char *ptr = token->substr + 1;
		char	   *write_ptr;
		int			n = token->size - 2;

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

		truncate_identifier(result, write_ptr - result, false);

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
	TokenType	token, *_token;
	bool	read_atleast_one = false;

	while (1)
	{
		_token = get_token(state, &token);
		if (!_token)
			break;

		if (_token->value != TOKEN_IDENTIF && _token->value != TOKEN_QIDENTIF)
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
parse_qualified_identifier(TokenizerState *state, const char **startptr, int *size)
{
	TokenType	token, *_token;
	bool		read_atleast_one = false;
	const char	   *_startptr = *startptr;
	int			_size = *size;

	while (1)
	{
		_token = get_token(state, &token);
		if (!_token)
			break;

		if (_token->value != TOKEN_IDENTIF && _token->value != TOKEN_QIDENTIF)
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
	TokenType	token, *_token;
	const char	   *typename_start = NULL;
	int			typename_length = 0;
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
			Oid		_typtype;
			int32	_typmod;

			_token = get_token(state, &token);
			if (!_token ||
				(_token->value != TOKEN_IDENTIF &&
				 _token->value != TOKEN_QIDENTIF))
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
	else if (_token->value == TOKEN_QIDENTIF)
	{
		unget_token(state, _token);

		parse_qualified_identifier(state, &typename_start, &typename_length);
	}
	else if (_token->value == TOKEN_IDENTIF)
	{
		TokenType	token2, *_token2;

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

				while (_token2 && _token2->value == TOKEN_IDENTIF)
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
				if (!_token || _token->value != TOKEN_NUMBER)
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

	typestr = pnstrdup(typename_start, typename_length);
	typeName = typeStringToTypeName(typestr);
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
	bool	is_first = true;
	ListCell *lc;

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
	 * namespace is available only in compile check mode, and only in this mode
	 * this pragma can be used.
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
		int		target_dno;
		PLpgSQL_datum *target;
		List	   *names;
		Oid			typtype;
		int32		typmod;
		TupleDesc	typtupdesc;

		initialize_tokenizer(&tstate, str);

		names = get_qualified_identifier(&tstate, NULL);
		if ((target_dno = get_varno(ns, names)) == -1)
			elog(ERROR, "Cannot to find variable \"%s\" used in settype pragma", get_name(names));

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

		SPI_restore_connection();
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

		/* reconnect spi */
		SPI_restore_connection();

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
		TokenType token, *_token;
		StringInfoData query;
		int32		typmod;

		initialize_tokenizer(&tstate, str);

		_token = get_token(&tstate, &token);
		if (!_token || (_token->value != TOKEN_IDENTIF
				&& _token->value != TOKEN_QIDENTIF))
			elog(ERROR, "Syntax error (expected identifier)");

		_token = get_token(&tstate, &token);
		if (!_token || _token->value != '(')
			elog(ERROR, "Syntax error (expected table specification)");

		unget_token(&tstate, _token);

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

		SPI_restore_connection();
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

		/* reconnect spi */
		SPI_restore_connection();

		/* raise warning (errors in pragma can be ignored instead */
		ereport(WARNING,
				(errmsg("Pragma \"table\" on line %d is not processed.", lineno),
				 errdetail("%s", edata->message)));

		result = false;
	}
	PG_END_TRY();

	return result;
}
