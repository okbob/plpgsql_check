/*-------------------------------------------------------------------------
 *
 * assign.c
 *
 *			  assign types to record variables
 *
 * by Pavel Stehule 2013-2019
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM >= 110000

#define get_eval_mcontext(estate) \
	((estate)->eval_econtext->ecxt_per_tuple_memory)
#define eval_mcontext_alloc(estate, sz) \
	MemoryContextAlloc(get_eval_mcontext(estate), sz)
#define eval_mcontext_alloc0(estate, sz) \
	MemoryContextAllocZero(get_eval_mcontext(estate), sz)

static bool compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc);

#endif

/*
 * Mark variable as used
 */
void
plpgsql_check_record_variable_usage(PLpgSQL_checkstate *cstate, int dno, bool write)
{
	if (dno >= 0)
	{
		if (!write)
			cstate->used_variables = bms_add_member(cstate->used_variables, dno);
		else
		{
			cstate->modif_variables = bms_add_member(cstate->modif_variables, dno);

			/* raise extra warning when protected variable is modified */
			if (bms_is_member(dno, cstate->protected_variables))
			{
				PLpgSQL_variable *var = (PLpgSQL_variable *) cstate->estate->datums[dno];
				StringInfoData message;

				initStringInfo(&message);

				appendStringInfo(&message, "auto varible \"%s\" should not be modified by user", var->refname);
				plpgsql_check_put_error(cstate,
						  0, var->lineno,
						  message.data,
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_EXTRA,
						  0, NULL, NULL);
				pfree(message.data);
			}
		}
	}
}

void
plpgsql_check_row_or_rec(PLpgSQL_checkstate *cstate, PLpgSQL_row *row, PLpgSQL_rec *rec)
{
	int			fnum;

	if (row != NULL)
	{

		for (fnum = 0; fnum < row->nfields; fnum++)
		{
			/* skip dropped columns */
			if (row->varnos[fnum] < 0)
				continue;

			plpgsql_check_target(cstate, row->varnos[fnum], NULL, NULL);
		}
		plpgsql_check_record_variable_usage(cstate, row->dno, true);
	}
	else if (rec != NULL)
	{
		/*
		 * There are no checks done on records currently; just record that the
		 * variable is not unused.
		 */
		plpgsql_check_record_variable_usage(cstate, rec->dno, true);
	}
}

/*
 * Verify lvalue It doesn't repeat a checks that are done. Checks a subscript
 * expressions, verify a validity of record's fields.
 */
void
plpgsql_check_target(PLpgSQL_checkstate *cstate, int varno, Oid *expected_typoid, int *expected_typmod)
{
	PLpgSQL_datum *target = cstate->estate->datums[varno];

	plpgsql_check_record_variable_usage(cstate, varno, true);

	switch (target->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) target;
				PLpgSQL_type *tp = var->datatype;

				if (expected_typoid != NULL)
					*expected_typoid = tp->typoid;
				if (expected_typmod != NULL)
					*expected_typmod = tp->atttypmod;
			}
			break;

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) target;

				plpgsql_check_recvar_info(rec, expected_typoid, expected_typmod);
			}
			break;

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) target;

				if (row->rowtupdesc != NULL)
				{
					if (expected_typoid != NULL)
						*expected_typoid = row->rowtupdesc->tdtypeid;
					if (expected_typmod != NULL)
						*expected_typmod = row->rowtupdesc->tdtypmod;
				}
				else
				{
					if (expected_typoid != NULL)
						*expected_typoid = RECORDOID;
					if (expected_typmod != NULL)
						*expected_typmod = -1;
				}

				plpgsql_check_row_or_rec(cstate, row, NULL);

			}
			break;

		case PLPGSQL_DTYPE_RECFIELD:
			{
				PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) target;
				PLpgSQL_rec *rec;
				int			fno;

				rec = (PLpgSQL_rec *) (cstate->estate->datums[recfield->recparentno]);

				/*
				 * Check that there is already a tuple in the record. We need
				 * that because records don't have any predefined field
				 * structure.
				 */
				if (!HeapTupleIsValid(recvar_tuple(rec)))
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("record \"%s\" is not assigned to tuple structure",
						   rec->refname)));

				/*
				 * Get the number of the records field to change and the
				 * number of attributes in the tuple.  Note: disallow system
				 * column names because the code below won't cope.
				 */
				fno = SPI_fnumber(recvar_tupdesc(rec), recfield->fieldname);
				if (fno <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("record \"%s\" has no field \"%s\"",
									rec->refname, recfield->fieldname)));

				if (expected_typoid)
					*expected_typoid = SPI_gettypeid(recvar_tupdesc(rec), fno);

				if (expected_typmod)
					*expected_typmod = TupleDescAttr(recvar_tupdesc(rec), fno - 1)->atttypmod;
			}
			break;

		case PLPGSQL_DTYPE_ARRAYELEM:
			{
				/*
				 * Target is an element of an array
				 */
				int			nsubscripts;
				Oid			arrayelemtypeid;
				Oid			arraytypeid;

				/*
				 * To handle constructs like x[1][2] := something, we have to
				 * be prepared to deal with a chain of arrayelem datums. Chase
				 * back to find the base array datum, and save the subscript
				 * expressions as we go.  (We are scanning right to left here,
				 * but want to evaluate the subscripts left-to-right to
				 * minimize surprises.)
				 */
				nsubscripts = 0;
				do
				{
					PLpgSQL_arrayelem *arrayelem = (PLpgSQL_arrayelem *) target;

					if (nsubscripts++ >= MAXDIM)
						ereport(ERROR,
								(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
								 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
										nsubscripts + 1, MAXDIM)));

					plpgsql_check_expr(cstate, arrayelem->subscript);

					target = cstate->estate->datums[arrayelem->arrayparentno];
				} while (target->dtype == PLPGSQL_DTYPE_ARRAYELEM);

				/*
				 * If target is domain over array, reduce to base type
				 */

				arraytypeid = plpgsql_check__exec_get_datum_type_p(cstate->estate, target);
				arraytypeid = getBaseType(arraytypeid);

				arrayelemtypeid = get_element_type(arraytypeid);

				if (!OidIsValid(arrayelemtypeid))
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("subscripted object is not an array")));

				if (expected_typoid)
					*expected_typoid = arrayelemtypeid;

				if (expected_typmod)
					*expected_typmod = ((PLpgSQL_var *) target)->datatype->atttypmod;

				plpgsql_check_record_variable_usage(cstate, target->dno, true);
			}
			break;

		default:
			;		/* nope */
	}
}

/*
 * Check so target can accept typoid value
 *
 */
void
plpgsql_check_assign_to_target_type(PLpgSQL_checkstate *cstate,
									Oid target_typoid,
									int32 target_typmod,
									Oid value_typoid,
									bool isnull)
{
	/* not used yet */
	(void) target_typmod;

	/* the overhead UNKONWNOID --> TEXT is low */
	if (target_typoid == TEXTOID && value_typoid == UNKNOWNOID)
		return;

	if (type_is_rowtype(value_typoid))
		plpgsql_check_put_error(cstate,
					  ERRCODE_DATATYPE_MISMATCH, 0,
					  "cannot cast composite value to a scalar type",
					  NULL,
					  NULL,
					  PLPGSQL_CHECK_ERROR,
					  0, NULL, NULL);

	else if (target_typoid != value_typoid && !isnull)
	{
		StringInfoData	str;

		initStringInfo(&str);
		appendStringInfo(&str, "cast \"%s\" value to \"%s\" type",
									format_type_be(value_typoid),
									format_type_be(target_typoid));

		/* accent warning when cast is without supported explicit casting */
		if (!can_coerce_type(1, &value_typoid, &target_typoid, COERCION_EXPLICIT))
			plpgsql_check_put_error(cstate,
						  ERRCODE_DATATYPE_MISMATCH, 0,
						  "target type is different type than source type",
						  str.data,
						  "There are no possible explicit coercion between those types, possibly bug!",
						  PLPGSQL_CHECK_WARNING_OTHERS,
						  0, NULL, NULL);
		else if (!can_coerce_type(1, &value_typoid, &target_typoid, COERCION_ASSIGNMENT))
			plpgsql_check_put_error(cstate,
						  ERRCODE_DATATYPE_MISMATCH, 0,
						  "target type is different type than source type",
						  str.data,
						  "The input expression type does not have an assignment cast to the target type.",
						  PLPGSQL_CHECK_WARNING_OTHERS,
						  0, NULL, NULL);
		else
		{
			/* highly probably only performance issue */
			if (!isnull)
				plpgsql_check_put_error(cstate,
							  ERRCODE_DATATYPE_MISMATCH, 0,
							  "target type is different type than source type",
							  str.data,
							  "Hidden casting can be a performance issue.",
							  PLPGSQL_CHECK_WARNING_PERFORMANCE,
							  0, NULL, NULL);
		}

		pfree(str.data);
	}
}

/*
 * Assign a tuple descriptor to variable specified by dno
 */
void
plpgsql_check_assign_tupdesc_dno(PLpgSQL_checkstate *cstate, int varno, TupleDesc tupdesc, bool isnull)
{
	PLpgSQL_datum *target = cstate->estate->datums[varno];

	switch (target->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) target;

				plpgsql_check_assign_to_target_type(cstate,
									 var->datatype->typoid, var->datatype->atttypmod,
									 TupleDescAttr(tupdesc, 0)->atttypid,
									 isnull);
			}
			break;

		case PLPGSQL_DTYPE_ROW:
			plpgsql_check_assign_tupdesc_row_or_rec(cstate, (PLpgSQL_row *) target, NULL, tupdesc, isnull);
			break;

		case PLPGSQL_DTYPE_REC:
			plpgsql_check_assign_tupdesc_row_or_rec(cstate, NULL, (PLpgSQL_rec *) target, tupdesc, isnull);
			break;

		case PLPGSQL_DTYPE_RECFIELD:
			{
				Oid		typoid;
				int		typmod;

				plpgsql_check_target(cstate, varno, &typoid, &typmod);

				plpgsql_check_assign_to_target_type(cstate,
									 typoid, typmod,
									 TupleDescAttr(tupdesc, 0)->atttypid,
									 isnull);
			}
			break;

		case PLPGSQL_DTYPE_ARRAYELEM:
			{
				Oid expected_typoid;
				int expected_typmod;

				plpgsql_check_target(cstate, varno, &expected_typoid, &expected_typmod);

				/* When target is composite type, then source is expanded already */
				if (type_is_rowtype(expected_typoid))
				{
					PLpgSQL_rec rec;

					plpgsql_check_recval_init(&rec);

					PG_TRY();
					{
						plpgsql_check_recval_assign_tupdesc(cstate, &rec,
											  lookup_rowtype_tupdesc_noerror(expected_typoid,
																			 expected_typmod,
																			 true),
																			 isnull);

						plpgsql_check_assign_tupdesc_row_or_rec(cstate, NULL, &rec, tupdesc, isnull);
						plpgsql_check_recval_release(&rec);
					}
					PG_CATCH();
					{
						plpgsql_check_recval_release(&rec);

						PG_RE_THROW();
					}
					PG_END_TRY();
				}
				else
					plpgsql_check_assign_to_target_type(cstate,
									    expected_typoid, expected_typmod,
									    TupleDescAttr(tupdesc, 0)->atttypid,
									    isnull);
			}
			break;

		default:
			;		/* nope */
	}
}

/*
 * We have to assign TupleDesc to all used record variables step by step. We
 * would to use a exec routines for query preprocessing, so we must to create
 * a typed NULL value, and this value is assigned to record variable.
 */
void
plpgsql_check_assign_tupdesc_row_or_rec(PLpgSQL_checkstate *cstate,
								  PLpgSQL_row *row,
								  PLpgSQL_rec *rec,
								  TupleDesc tupdesc,
								  bool isnull)
{
	if (tupdesc == NULL)
	{
		plpgsql_check_put_error(cstate,
					  0, 0,
					  "tuple descriptor is empty", NULL, NULL,
					  PLPGSQL_CHECK_WARNING_OTHERS,
					  0, NULL, NULL);
		return;
	}

	/*
	 * row variable has assigned TupleDesc already, so don't be processed here
	 */
	if (rec != NULL)
	{
		PLpgSQL_rec *target = (PLpgSQL_rec *) (cstate->estate->datums[rec->dno]);

		plpgsql_check_recval_release(target);
		plpgsql_check_recval_assign_tupdesc(cstate, target, tupdesc, isnull);
	}

	else if (row != NULL && tupdesc != NULL)
	{
		int			td_natts = tupdesc->natts;
		int			fnum;
		int			anum;

		anum = 0;
		for (fnum = 0; fnum < row->nfields; fnum++)
		{
			if (row->varnos[fnum] < 0)
				continue;		/* skip dropped column in row struct */

			while (anum < td_natts && TupleDescAttr(tupdesc, anum)->attisdropped)
				anum++;			/* skip dropped column in tuple */

			if (anum < td_natts)
			{
				Oid	valtype = SPI_gettypeid(tupdesc, anum + 1);
				PLpgSQL_datum *target = cstate->estate->datums[row->varnos[fnum]];

				switch (target->dtype)
				{
					case PLPGSQL_DTYPE_VAR:
						{
							PLpgSQL_var *var = (PLpgSQL_var *) target;

							plpgsql_check_assign_to_target_type(cstate,
												 var->datatype->typoid,
												 var->datatype->atttypmod,
														 valtype,
														 isnull);
						}
						break;

					case PLPGSQL_DTYPE_RECFIELD:
						{
							Oid	expected_typoid;
							int	expected_typmod;

							plpgsql_check_target(cstate, target->dno, &expected_typoid, &expected_typmod);
							plpgsql_check_assign_to_target_type(cstate,
												 expected_typoid,
												 expected_typmod,
														valtype,
														isnull);
						}
						break;
					default:
						;		/* nope */
				}

				anum++;
			}
		}
	}
}

/*
 * recval_init, recval_release, recval_assign_tupdesc
 *
 *   a set of functions designed to better portability between PostgreSQL 11
 *   with expanded records support and older PostgreSQL versions.
 */
void
plpgsql_check_recval_init(PLpgSQL_rec *rec)
{
	Assert(rec->dtype == PLPGSQL_DTYPE_REC);

#if PG_VERSION_NUM >= 110000

	rec->erh = NULL;

#else

	rec->tup = NULL;
	rec->freetup = false;
	rec->freetupdesc = false;

#endif
}

void
plpgsql_check_recval_release(PLpgSQL_rec *rec)
{

#if PG_VERSION_NUM >= 110000

	Assert(rec->dtype == PLPGSQL_DTYPE_REC);

	if (rec->erh)
		DeleteExpandedObject(ExpandedRecordGetDatum(rec->erh));
	rec->erh = NULL;

#else

	if (rec->freetup)
		heap_freetuple(rec->tup);

	if (rec->freetupdesc)
		FreeTupleDesc(rec->tupdesc);

	rec->freetup = false;
	rec->freetupdesc = false;

#endif

}

/*
 * is_null is true, when we assign NULL expression and type should not be checked.
 */
void
plpgsql_check_recval_assign_tupdesc(PLpgSQL_checkstate *cstate, PLpgSQL_rec *rec, TupleDesc tupdesc, bool is_null)
{

#if PG_VERSION_NUM >= 110000

	PLpgSQL_execstate	   *estate = cstate->estate;
	ExpandedRecordHeader   *newerh;
	MemoryContext			mcontext;
	TupleDesc	var_tupdesc;
	Datum	   *newvalues;
	bool	   *newnulls;
	char	   *chunk;
	int			vtd_natts;
	int			i;

	mcontext = get_eval_mcontext(estate);
	plpgsql_check_recval_release(rec);

	/*
	 * code is reduced version of make_expanded_record_for_rec
	 */
	if (rec->rectypeid != RECORDOID)
	{
		newerh = make_expanded_record_from_typeid(rec->rectypeid, -1,
													  mcontext);
	}
	else
	{
		if (!tupdesc)
			return;

		newerh = make_expanded_record_from_tupdesc(tupdesc,
													   mcontext);
	}

	/*
	 * code is reduced version of exec_move_row_from_field
	 */
	var_tupdesc = expanded_record_get_tupdesc(newerh);
	vtd_natts = var_tupdesc->natts;

	if (!is_null && tupdesc != NULL && !compatible_tupdescs(var_tupdesc, tupdesc))
	{
		int		i = 0;
		int		j = 0;
		int		target_nfields = 0;
		int		src_nfields = 0;
		bool	src_field_is_valid = false;
		bool	target_field_is_valid = false;
		Form_pg_attribute sattr = NULL;
		Form_pg_attribute tattr = NULL;

		while (i < var_tupdesc->natts || j < tupdesc->natts)
		{
			if (!target_field_is_valid && i < var_tupdesc->natts)
			{
				tattr = TupleDescAttr(var_tupdesc, i);
				if (tattr->attisdropped)
				{
					i += 1;
					continue;
				}
				target_field_is_valid = true;
				target_nfields += 1;
			}

			if (!src_field_is_valid && j < tupdesc->natts)
			{
				sattr = TupleDescAttr(tupdesc, j);
				if (sattr->attisdropped)
				{
					j += 1;
					continue;
				}
				src_field_is_valid = true;
				src_nfields += 1;
			}

			if (src_field_is_valid && target_field_is_valid)
			{
				plpgsql_check_assign_to_target_type(cstate,
												tattr->atttypid, tattr->atttypmod,
												sattr->atttypid,
												false);

				/* try to search next tuple of fields */
				src_field_is_valid =  false;
				target_field_is_valid = false;
				i += 1;
				j += 1;
			}
			else
				break;
		}

		if (src_nfields < target_nfields)
			plpgsql_check_put_error(cstate,
						  0, 0,
						  "too few attributes for composite variable",
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_OTHERS,
						  0, NULL, NULL);
		else if (src_nfields > target_nfields)
			plpgsql_check_put_error(cstate,
						  0, 0,
						  "too many attributes for composite variable",
						  NULL,
						  NULL,
						  PLPGSQL_CHECK_WARNING_OTHERS,
						  0, NULL, NULL);
	}

	chunk = eval_mcontext_alloc(estate,
								vtd_natts * (sizeof(Datum) + sizeof(bool)));
	newvalues = (Datum *) chunk;
	newnulls = (bool *) (chunk + vtd_natts * sizeof(Datum));

	for (i = 0; i < vtd_natts; i++)
	{
		newvalues[i] = (Datum) 0;
		newnulls[i] = true;
	}

	expanded_record_set_fields(newerh, newvalues, newnulls, true);

	TransferExpandedRecord(newerh, estate->datum_context);
	rec->erh = newerh;

#else

	bool	   *nulls;
	HeapTuple	tup;

	(void) cstate;
	(void) is_null;

	plpgsql_check_recval_release(rec);

	if (!tupdesc)
		return;

	/* initialize rec by NULLs */
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	rec->tupdesc = CreateTupleDescCopy(tupdesc);
	rec->freetupdesc = true;

	tup = heap_form_tuple(tupdesc, NULL, nulls);
	if (HeapTupleIsValid(tup))
	{
		rec->tup = tup;
		rec->freetup = true;
	}
	else
		elog(ERROR, "cannot to build valid composite value");

#endif

}

#if PG_VERSION_NUM >= 110000

/*
 * compatible_tupdescs: detect whether two tupdescs are physically compatible
 *
 * TRUE indicates that a tuple satisfying src_tupdesc can be used directly as
 * a value for a composite variable using dst_tupdesc.
 */
static bool
compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc)
{
	int			i;

	/* Possibly we could allow src_tupdesc to have extra columns? */
	if (dst_tupdesc->natts != src_tupdesc->natts)
		return false;

	for (i = 0; i < dst_tupdesc->natts; i++)
	{
		Form_pg_attribute dattr = TupleDescAttr(dst_tupdesc, i);
		Form_pg_attribute sattr = TupleDescAttr(src_tupdesc, i);

		if (dattr->attisdropped != sattr->attisdropped)
			return false;
		if (!dattr->attisdropped)
		{
			/* Normal columns must match by type and typmod */
			if (dattr->atttypid != sattr->atttypid ||
				(dattr->atttypmod >= 0 &&
				 dattr->atttypmod != sattr->atttypmod))
				return false;
		}
		else
		{
			/* Dropped columns are OK as long as length/alignment match */
			if (dattr->attlen != sattr->attlen ||
				dattr->attalign != sattr->attalign)
				return false;
		}
	}
	return true;
}

#endif
