/*-------------------------------------------------------------------------
 *
 * report.c
 *
 *			  last stage checks
 *
 * by Pavel Stehule 2013-2024
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"

static bool datum_is_used(PLpgSQL_checkstate *cstate, int dno, bool write);
static bool datum_is_explicit(PLpgSQL_checkstate *cstate, int dno);

/*
 * Returns true, when variable is internal (automatic)
 *
 */
static bool
is_internal(char *refname, int lineno)
{
	if (lineno <= 0)
		return true;
	if (refname == NULL)
		return true;
	if (strcmp(refname, "*internal*") == 0)
		return true;
	if (strcmp(refname, "(unnamed row)") == 0)
		return true;
	return false;
}

bool
is_internal_variable(PLpgSQL_checkstate *cstate, PLpgSQL_variable *var)
{
	if (bms_is_member(var->dno, cstate->auto_variables))
		return true;

	return is_internal(var->refname, var->lineno);
}

/*
 * returns refname of PLpgSQL_datum. When refname is generated,
 * then return null too, although refname is not null.
 */
char *
plpgsql_check_datum_get_refname(PLpgSQL_checkstate *cstate, PLpgSQL_datum *d)
{
	char	   *refname;
	int			lineno;

	switch (d->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			refname = ((PLpgSQL_var *) d)->refname;
			lineno = ((PLpgSQL_var *) d)->lineno;
			break;

		case PLPGSQL_DTYPE_ROW:
			refname = ((PLpgSQL_row *) d)->refname;
			lineno = ((PLpgSQL_row *) d)->lineno;
			break;

		case PLPGSQL_DTYPE_REC:
			refname = ((PLpgSQL_rec *) d)->refname;
			lineno = ((PLpgSQL_rec *) d)->lineno;
			break;

		default:
			refname = NULL;
			lineno = -1;
	}

	/*
	 * This routine is used for shadowing check.
	 * We would to check auto variables too
	 */
	if (bms_is_member(d->dno, cstate->auto_variables))
		return refname;

	/*
	 * PostgreSQL 12 started use "(unnamed row)" name for internal
	 * variables. Hide this name too (lineno is -1).
	 */
	if (is_internal(refname, lineno))
		return NULL;

	return refname;
}

/*
 * Returns true if dno is explicitly declared. It should not be used
 * for arguments.
 */
bool
datum_is_explicit(PLpgSQL_checkstate *cstate, int dno)
{
	PLpgSQL_execstate *estate = cstate->estate;

	if (bms_is_member(dno, cstate->auto_variables))
		return false;

	switch (estate->datums[dno]->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[dno];
				return !is_internal(var->refname, var->lineno);
			}

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) estate->datums[dno];
				return !is_internal(row->refname, row->lineno);
			}
		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) estate->datums[dno];
				return !is_internal(rec->refname, rec->lineno);
			}

		default:
			return false;
	}
}

/*
 * returns true, when datum or some child is used
 */
static bool
datum_is_used(PLpgSQL_checkstate *cstate, int dno, bool write)
{
	PLpgSQL_execstate *estate = cstate->estate;

	switch (estate->datums[dno]->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				return bms_is_member(dno,
						write ? cstate->modif_variables : cstate->used_variables);
			}
			break;

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) estate->datums[dno];
				int	     i;

				if (bms_is_member(dno,
						  write ? cstate->modif_variables : cstate->used_variables))
					return true;

				for (i = 0; i < row->nfields; i++)
				{
					if (row->varnos[i] < 0)
						continue;

					if (datum_is_used(cstate, row->varnos[i], write))
						return true;
				}

				return false;
			}
			break;

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) estate->datums[dno];
				int	     i;

				if (bms_is_member(dno,
						  write ? cstate->modif_variables : cstate->used_variables))
					return true;

				/* search any used recfield with related recparentno */
				for (i = 0; i < estate->ndatums; i++)
				{
					if (estate->datums[i]->dtype == PLPGSQL_DTYPE_RECFIELD)
					{
						PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) estate->datums[i];

						if (recfield->recparentno == rec->dno
								    && datum_is_used(cstate, i, write))
							return true;
					}
				}
			}
			break;

		case PLPGSQL_DTYPE_RECFIELD:
			return bms_is_member(dno,
					write ? cstate->modif_variables : cstate->used_variables);

		default:
			return false;
	}

	return false;
}

/*
 * Reports all unused variables explicitly DECLAREd by the user.  Ignores
 * special variables created by PL/PgSQL.
 */
void
plpgsql_check_report_unused_variables(PLpgSQL_checkstate *cstate)
{
	int i;
	PLpgSQL_execstate *estate = cstate->estate;

	/* now, there are no active plpgsql statement */
	estate->err_stmt = NULL;

	for (i = 0; i < estate->ndatums; i++)
		if (datum_is_explicit(cstate, i) &&
			!(datum_is_used(cstate, i, false) || datum_is_used(cstate, i, true)))
		{
			PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[i];
			StringInfoData message;

			initStringInfo(&message);

			appendStringInfo(&message, UNUSED_VARIABLE_TEXT, var->refname);
			plpgsql_check_put_error(cstate,
					  0, var->lineno,
					  message.data,
					  NULL,
					  NULL,
					  PLPGSQL_CHECK_WARNING_OTHERS,
					  0, NULL, NULL);

			pfree(message.data);
			message.data = NULL;
		}

	if (cstate->cinfo->extra_warnings)
	{
		PLpgSQL_function *func = estate->func;

		/* check never read variables */
		for (i = 0; i < estate->ndatums; i++)
		{
			if (datum_is_explicit(cstate, i)
				 && !datum_is_used(cstate, i, false) && datum_is_used(cstate, i, true))
			{
				PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[i];
				StringInfoData message;

				initStringInfo(&message);

				appendStringInfo(&message, NEVER_READ_VARIABLE_TEXT, var->refname);
				plpgsql_check_put_error(cstate,
						  0, var->lineno,
						  message.data,
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);

				pfree(message.data);
				message.data = NULL;
			}
		}

		/* check IN parameters */
		for (i = 0; i < func->fn_nargs; i++)
		{
			int		varno = func->fn_argvarnos[i];

			bool	is_read = datum_is_used(cstate, varno, false);
			bool	is_write = datum_is_used(cstate, varno, true);

			if (!is_read && !is_write)
			{
				PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[varno];
				StringInfoData message;

				initStringInfo(&message);

				appendStringInfo(&message, UNUSED_PARAMETER_TEXT, var->refname);
				plpgsql_check_put_error(cstate,
						  0, 0,
						  message.data,
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);

				pfree(message.data);
				message.data = NULL;
			}
			else if (!is_read)
			{
				bool	is_inout_procedure_param = false;

				/*
				 * procedure doesn't support only OUT parameters. Don't raise
				 * warning if INOUT parameter is just modified in procedures.
				 */
				is_inout_procedure_param = cstate->cinfo->is_procedure
											&& bms_is_member(varno, cstate->out_variables);

				if (!is_inout_procedure_param)
				{
					PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[varno];
					StringInfoData message;

					initStringInfo(&message);

					appendStringInfo(&message, NEVER_READ_PARAMETER_TEXT, var->refname);
					plpgsql_check_put_error(cstate,
							  0, 0,
							  message.data,
							  NULL,
							  NULL,
							  PLPGSQL_CHECK_WARNING_EXTRA,
							  0, NULL, NULL);

					pfree(message.data);
					message.data = NULL;
				}
			}
		}

		/* are there some OUT parameters (expect modification)? */
		if (func->out_param_varno != -1 && !cstate->found_return_query)
		{
			int		varno = func->out_param_varno;
			PLpgSQL_variable *var = (PLpgSQL_variable *) estate->datums[varno];

			if (var->dtype == PLPGSQL_DTYPE_ROW && is_internal_variable(cstate, var))
			{
				/* this function has more OUT parameters */
				PLpgSQL_row *row = (PLpgSQL_row*) var;
				int		fnum;

				for (fnum = 0; fnum < row->nfields; fnum++)
				{
					int		varno2 = row->varnos[fnum];
					PLpgSQL_variable *var2 = (PLpgSQL_variable *) estate->datums[varno2];
					StringInfoData message;

					if (var2->dtype == PLPGSQL_DTYPE_ROW ||
						  var2->dtype == PLPGSQL_DTYPE_REC)
					{
						/*
						 * The result of function with more OUT variables (and one
						 * should be an composite), is not possible simply assign to
						 * outer variables. The related expression cannot be "simple"
						 * expression, and then an evaluation is 10x slower. So there
						 * is warning 
						 */
						initStringInfo(&message);
						appendStringInfo(&message,
									  OUT_COMPOSITE_IS_NOT_SINGLE_TEXT, var2->refname);
						plpgsql_check_put_error(cstate,
								  0, 0,
								  message.data,
								  NULL,
								  NULL,
								  PLPGSQL_CHECK_WARNING_EXTRA,
								  0, NULL, NULL);

						pfree(message.data);
						message.data = NULL;
					}

					if (!datum_is_used(cstate, varno2, true))
					{
						const char *fmt = cstate->found_return_dyn_query ?
								  MAYBE_UNMODIFIED_VARIABLE_TEXT : UNMODIFIED_VARIABLE_TEXT;

						const char *detail = cstate->found_return_dyn_query ?
								  "cannot to determine result of dynamic SQL" : NULL;

						initStringInfo(&message);
						appendStringInfo(&message, fmt, var2->refname);
						plpgsql_check_put_error(cstate,
								  0, 0,
								  message.data,
								  detail,
								  NULL,
								  PLPGSQL_CHECK_WARNING_EXTRA,
								  0, NULL, NULL);

						pfree(message.data);
						message.data = NULL;
					}
				}
			}
			else
			{
				if (!datum_is_used(cstate, varno, true))
				{
					StringInfoData message;

					const char *fmt = cstate->found_return_dyn_query ?
							  MAYBE_UNMODIFIED_VARIABLE_TEXT : UNMODIFIED_VARIABLE_TEXT;

					const char *detail = cstate->found_return_dyn_query ?
							  "cannot to determine result of dynamic SQL" : NULL;

					initStringInfo(&message);

					appendStringInfo(&message, fmt, var->refname);
					plpgsql_check_put_error(cstate,
							  0, 0,
							  message.data,
							  detail,
							  NULL,
							  PLPGSQL_CHECK_WARNING_EXTRA,
							  0, NULL, NULL);

					pfree(message.data);
					message.data = NULL;
				}
			}
		}
	}
}

/*
 * Report too high volatility
 *
 */
void
plpgsql_check_report_too_high_volatility(PLpgSQL_checkstate *cstate)
{
	if (cstate->cinfo->performance_warnings && !cstate->skip_volatility_check)
	{
		char	   *current = NULL;
		char	   *should_be = NULL;
		bool 		raise_warning = false;

		if (cstate->volatility == PROVOLATILE_IMMUTABLE &&
				(cstate->decl_volatility == PROVOLATILE_VOLATILE ||
				 cstate->decl_volatility == PROVOLATILE_STABLE))
		{
			should_be = "IMMUTABLE";
			current = cstate->decl_volatility == PROVOLATILE_VOLATILE ?
						"VOLATILE" : "STABLE";
			raise_warning = true;
		}
		else if (cstate->volatility == PROVOLATILE_STABLE &&
				(cstate->decl_volatility == PROVOLATILE_VOLATILE))
		{
			if (cstate->cinfo->rettype != VOIDOID)
			{
				should_be = "STABLE";
				current = "VOLATILE";
				raise_warning = true;
			}
		}

		if (raise_warning)
		{
			StringInfoData message;

			initStringInfo(&message);

			appendStringInfo(&message, "routine is marked as %s, should be %s", current, should_be);
			plpgsql_check_put_error(cstate,
					  0, -1,
					  message.data,
					  cstate->has_execute_stmt ? "attention: cannot to determine volatility of used dynamic SQL" : NULL,
					  "When you fix this issue, please, recheck other functions that uses this function.",
					  PLPGSQL_CHECK_WARNING_PERFORMANCE,
					  0, NULL, NULL);

			pfree(message.data);
			message.data = NULL;
		}
	}
}
