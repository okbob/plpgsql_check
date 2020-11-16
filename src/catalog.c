/*-------------------------------------------------------------------------
 *
 * catalog.c
 *
 *			  routines for working with Postgres's catalog and caches
 *
 * by Pavel Stehule 2013-2020
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "access/genam.h"
#include "access/htup_details.h"

#if PG_VERSION_NUM >= 120000

#include "access/table.h"

#endif

#include "catalog/pg_extension.h"
#include "catalog/indexing.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#if PG_VERSION_NUM >= 100000

#include "utils/regproc.h"

#endif

#if PG_VERSION_NUM >= 110000

#include "catalog/pg_proc.h"

#endif

#if PG_VERSION_NUM < 120000

#include "access/sysattr.h"

#endif


#include "utils/syscache.h"

/*
 * Fix - change of typename in Postgres 14
 */
bool
plpgsql_check_is_eventtriggeroid(Oid typoid)
{

#if PG_VERSION_NUM >= 140000

	return typoid == EVENTTRIGGEROID;

#else

	return typoid == EVTTRIGGEROID;

#endif

}


/*
 * Prepare metadata necessary for plpgsql_check
 */
void
plpgsql_check_get_function_info(HeapTuple procTuple,
								Oid *rettype,
								char *volatility,
								PLpgSQL_trigtype *trigtype,
								bool *is_procedure)
{
	Form_pg_proc proc;
	char		functyptype;

	proc = (Form_pg_proc) GETSTRUCT(procTuple);

	functyptype = get_typtype(proc->prorettype);

	*trigtype = PLPGSQL_NOT_TRIGGER;

#if PG_VERSION_NUM >= 110000

	*is_procedure = proc->prokind == PROKIND_PROCEDURE;

#else

	*is_procedure = false;

#endif

	/*
	 * Disallow pseudotype result  except for TRIGGER, RECORD, VOID, or
	 * polymorphic
	 */
	if (functyptype == TYPTYPE_PSEUDO)
	{
		/* we assume OPAQUE with no arguments means a trigger */
		if (proc->prorettype == TRIGGEROID

#if PG_VERSION_NUM < 130000

			|| (proc->prorettype == OPAQUEOID && proc->pronargs == 0)

#endif

			)
			*trigtype = PLPGSQL_DML_TRIGGER;
		else if (plpgsql_check_is_eventtriggeroid(proc->prorettype))
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

/*
 * plpgsql_check_get_extension_schema - given an extension OID, fetch its extnamespace
 *
 * Returns InvalidOid if no such extension.
 */
static Oid
get_extension_schema(Oid ext_oid)
{
	Oid			result;
	Relation	rel;
	SysScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];

#if PG_VERSION_NUM >= 120000

	rel = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

#else

	rel = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

#endif

	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

#if PG_VERSION_NUM >= 120000

	table_close(rel, AccessShareLock);

#else

	heap_close(rel, AccessShareLock);

#endif

	return result;
}

/*
 * Returns oid of pragma function. It is used for elimination
 * pragma function from volatility tests.
 */
Oid
plpgsql_check_pragma_func_oid(void)
{
	Oid		result = InvalidOid;
	Oid		extoid;

	extoid = get_extension_oid("plpgsql_check", true);

	if (OidIsValid(extoid))
	{
		CatCList   *catlist;
		Oid			schemaoid;
		int			i;

		schemaoid = get_extension_schema(extoid);

		/* Search syscache by name only */
		catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum("plpgsql_check_pragma"));

		for (i = 0; i < catlist->n_members; i++)
		{
			HeapTuple	proctup = &catlist->members[i]->tuple;
			Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);

			/* Consider only procs in specified namespace */
			if (procform->pronamespace != schemaoid)
				continue;

#if PG_VERSION_NUM >= 120000

			result = procform->oid;

#else

			result = HeapTupleGetOid(proctup);

#endif

			break;
		}

		ReleaseSysCacheList(catlist);
	}

	return result;
}
