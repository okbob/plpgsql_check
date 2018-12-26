/*-------------------------------------------------------------------------
 *
 * plpgsql_setup.c
 *
 *			  initialize system structures necessary for using plpgsql rutines
 *
 * by Pavel Stehule 2013-2018
 *
 *-------------------------------------------------------------------------
 */


#include "plpgsql_check.h"

#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/rel.h"

/*
 * Returns true when some fields is polymorphics
 */
static bool
is_polymorphic_tupdesc(TupleDesc tupdesc)
{
	int	i;

	for (i = 0; i < tupdesc->natts; i++)
		if (IsPolymorphicType(TupleDescAttr(tupdesc, i)->atttypid))
			return true;

	return false;
}

/*
 * Set up a fake fcinfo with just enough info to satisfy plpgsql_compile().
 *
 * There should be a different real argtypes for polymorphic params.
 *
 * When output fake_rtd is true, then we should to not compare result fields,
 * because we know nothing about expected result.
 */
void
plpgsql_check_setup_fcinfo(HeapTuple procTuple,
						  FmgrInfo *flinfo,
						  FunctionCallInfoData *fcinfo,
						  ReturnSetInfo *rsinfo,
						  TriggerData *trigdata,
						  Oid relid,
						  EventTriggerData *etrigdata,
						  Oid funcoid,
						  PLpgSQL_trigtype trigtype,
						  Trigger *tg_trigger,
						  bool *fake_rtd)
{
	Form_pg_proc procform;
	Oid		rettype;
	TupleDesc resultTupleDesc;

	*fake_rtd = false;

	procform = (Form_pg_proc) GETSTRUCT(procTuple);
	rettype = procform->prorettype;

	/* clean structures */
	MemSet(fcinfo, 0, sizeof(FunctionCallInfoData));
	MemSet(flinfo, 0, sizeof(FmgrInfo));
	MemSet(rsinfo, 0, sizeof(ReturnSetInfo));

	fcinfo->flinfo = flinfo;
	flinfo->fn_oid = funcoid;
	flinfo->fn_mcxt = CurrentMemoryContext;

	if (trigtype == PLPGSQL_DML_TRIGGER)
	{
		Assert(trigdata != NULL);

		MemSet(trigdata, 0, sizeof(TriggerData));
		MemSet(tg_trigger, 0, sizeof(Trigger));

		trigdata->type = T_TriggerData;
		trigdata->tg_trigger = tg_trigger;

		fcinfo->context = (Node *) trigdata;

		if (OidIsValid(relid))
			trigdata->tg_relation = relation_open(relid, AccessShareLock);
	}
	else if (trigtype == PLPGSQL_EVENT_TRIGGER)
	{
		MemSet(etrigdata, 0, sizeof(etrigdata));
		etrigdata->type = T_EventTriggerData;
		fcinfo->context = (Node *) etrigdata;
	}

	/* 
	 * prepare ReturnSetInfo
	 *
	 * necessary for RETURN NEXT and RETURN QUERY
	 *
	 */
	resultTupleDesc = build_function_result_tupdesc_t(procTuple);
	if (resultTupleDesc)
	{
		/* we cannot to solve polymorphic params now */
		if (is_polymorphic_tupdesc(resultTupleDesc))
		{
			FreeTupleDesc(resultTupleDesc);
			resultTupleDesc = NULL;
		}
	}
	else if (rettype == TRIGGEROID || rettype == OPAQUEOID)
	{
		/* trigger - return value should be ROW or RECORD based on relid */
		if (trigdata->tg_relation)
			resultTupleDesc = CreateTupleDescCopy(trigdata->tg_relation->rd_att);
	}
	else if (!IsPolymorphicType(rettype))
	{
		if (get_typtype(rettype) == TYPTYPE_COMPOSITE)
			resultTupleDesc = lookup_rowtype_tupdesc_copy(rettype, -1);
		else
		{
			*fake_rtd = rettype == RECORDOID;

#if PG_VERSION_NUM >= 120000

			resultTupleDesc = CreateTemplateTupleDesc(1);

#else

			resultTupleDesc = CreateTemplateTupleDesc(1, false);

#endif

			TupleDescInitEntry(resultTupleDesc,
							    (AttrNumber) 1, "__result__",
							    rettype, -1, 0);
			resultTupleDesc = BlessTupleDesc(resultTupleDesc);
		}
	}

	if (resultTupleDesc)
	{
		fcinfo->resultinfo = (Node *) rsinfo;

		rsinfo->type = T_ReturnSetInfo;
		rsinfo->expectedDesc = resultTupleDesc;
		rsinfo->allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
		rsinfo->returnMode = SFRM_ValuePerCall;

		/*
		 * ExprContext is created inside CurrentMemoryContext,
		 * without any additional source allocation. It is released
		 * on end of transaction.
		 */
		rsinfo->econtext = CreateStandaloneExprContext();
	}
}

/* ----------
 * Initialize a plpgsql fake execution state
 * ----------
 */
void
plpgsql_check_setup_estate(PLpgSQL_execstate *estate,
					 PLpgSQL_function *func,
					 ReturnSetInfo *rsi)
{
	/* this link will be restored at exit from plpgsql_call_handler */
	func->cur_estate = estate;

	estate->func = func;

	estate->retval = (Datum) 0;
	estate->retisnull = true;
	estate->rettype = InvalidOid;

	estate->fn_rettype = func->fn_rettype;

	estate->retistuple = func->fn_retistuple;
	estate->retisset = func->fn_retset;

	estate->readonly_func = func->fn_readonly;

#if PG_VERSION_NUM < 110000

	estate->rettupdesc = NULL;
	estate->eval_econtext = NULL;

#else

	estate->eval_econtext = makeNode(ExprContext);
	estate->eval_econtext->ecxt_per_tuple_memory = AllocSetContextCreate(CurrentMemoryContext,
													"ExprContext",
													ALLOCSET_DEFAULT_SIZES);
	estate->datum_context = CurrentMemoryContext;

#endif

	estate->exitlabel = NULL;
	estate->cur_error = NULL;

	estate->tuple_store = NULL;
	if (rsi)
	{
		estate->tuple_store_cxt = rsi->econtext->ecxt_per_query_memory;
		estate->tuple_store_owner = CurrentResourceOwner;

#if PG_VERSION_NUM >= 110000

		estate->tuple_store_desc = rsi->expectedDesc;

#else

		if (estate->retisset)
			estate->rettupdesc = rsi->expectedDesc;

#endif

	}
	else
	{
		estate->tuple_store_cxt = NULL;
		estate->tuple_store_owner = NULL;
	}
	estate->rsi = rsi;

	estate->found_varno = func->found_varno;
	estate->ndatums = func->ndatums;
	estate->datums = palloc(sizeof(PLpgSQL_datum *) * estate->ndatums);
	/* caller is expected to fill the datums array */

	estate->eval_tuptable = NULL;
	estate->eval_processed = 0;

#if PG_VERSION_NUM < 120000

	estate->eval_lastoid = InvalidOid;


#if PG_VERSION_NUM < 90500

	estate->cur_expr = NULL;

#endif

#endif


	estate->err_stmt = NULL;
	estate->err_text = NULL;

	estate->plugin_info = NULL;
}
