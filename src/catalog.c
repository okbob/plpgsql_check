/*-------------------------------------------------------------------------
 *
 * catalog.c
 *
 *			  routines for working with Postgres's catalog and caches
 *
 * by Pavel Stehule 2013-2018
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "access/htup_details.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#if PG_VERSION_NUM >= 100000

#include "utils/regproc.h"

#endif

#include "utils/syscache.h"

/*
 * Prepare metadata necessary for plpgsql_check
 */
void
plpgsql_check_get_function_info(HeapTuple procTuple,
								Oid *rettype,
								char *volatility,
								PLpgSQL_trigtype *trigtype)
{
	Form_pg_proc proc;
	char		functyptype;

	proc = (Form_pg_proc) GETSTRUCT(procTuple);

	functyptype = get_typtype(proc->prorettype);

	*trigtype = PLPGSQL_NOT_TRIGGER;

	/*
	 * Disallow pseudotype result  except for TRIGGER, RECORD, VOID, or
	 * polymorphic
	 */
	if (functyptype == TYPTYPE_PSEUDO)
	{
		/* we assume OPAQUE with no arguments means a trigger */
		if (proc->prorettype == TRIGGEROID ||
			(proc->prorettype == OPAQUEOID && proc->pronargs == 0))
			*trigtype = PLPGSQL_DML_TRIGGER;
		else if (proc->prorettype == EVTTRIGGEROID)
			*trigtype = PLPGSQL_EVENT_TRIGGER;
		else if (proc->prorettype != RECORDOID &&
				 proc->prorettype != VOIDOID &&
				 !IsPolymorphicType(proc->prorettype))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("PL/pgSQL functions cannot return type %s",
							format_type_be(proc->prorettype))));
	}


	*volatility = ((Form_pg_proc) GETSTRUCT(procTuple))->provolatile;
	*rettype = ((Form_pg_proc) GETSTRUCT(procTuple))->prorettype;
}

char *
plpgsql_check_get_src(HeapTuple procTuple)
{
	Datum	prosrcdatum;
	bool	isnull;

	prosrcdatum = SysCacheGetAttr(PROCOID, procTuple,
								  Anum_pg_proc_prosrc, &isnull);
	if (isnull)
		elog(ERROR, "null prosrc");

	return TextDatumGetCString(prosrcdatum);
}

/*
 * Process necessary checking before code checking
 *     a) disallow other than plpgsql check function,
 *     b) when function is trigger function, then reloid must be defined
 */
void
plpgsql_check_precheck_conditions(plpgsql_check_info *cinfo)
{
	Form_pg_proc proc;
	Form_pg_language languageStruct;
	HeapTuple	languageTuple;
	char	   *funcname;

	proc = (Form_pg_proc) GETSTRUCT(cinfo->proctuple);
	funcname = format_procedure(cinfo->fn_oid);

	/* used language must be plpgsql */
	languageTuple = SearchSysCache1(LANGOID, ObjectIdGetDatum(proc->prolang));
	Assert(HeapTupleIsValid(languageTuple));

	languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);
	if (strcmp(NameStr(languageStruct->lanname), "plpgsql") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s is not a plpgsql function", funcname)));

	ReleaseSysCache(languageTuple);

	/* profiler doesn't require trigger data check */
	if (!cinfo->show_profile)
	{
		/* dml trigger needs valid relid, others not */
		if (cinfo->trigtype == PLPGSQL_DML_TRIGGER)
		{
			if (!OidIsValid(cinfo->relid))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("missing trigger relation"),
						 errhint("Trigger relation oid must be valid")));
		}
		else
		{
			if (OidIsValid(cinfo->relid))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("function is not trigger"),
						 errhint("Trigger relation oid must not be valid for non dml trigger function.")));
		}
	}

	pfree(funcname);
}
