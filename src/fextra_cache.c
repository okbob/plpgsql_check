/*-------------------------------------------------------------------------
 *
 * fextra_cache
 *
 *			cache with function extra informations - mostly AST based
 *
 * by Pavel Stehule 2013-2026
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "plpgsql.h"
#include "plpgsql_check.h"

#if PG_VERSION_NUM >= 190000

#include "utils/hsearch.h"

#endif

#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static HTAB *fextra_ht = NULL;
static MemoryContext fextra_mcxt = NULL;

typedef struct
{
	plch_fextra *fextra;
	int			parentid;
	int			*naturalid;
	int			current_deep;		/* starts from zero */
} fextra_init_context;

static void
init_fextra_stmt_walker(PLpgSQL_stmt *stmt, fextra_init_context *context)
{
	fextra_init_context loccontext;
	plch_fextra *fextra = context->fextra;
	int			stmtid = stmt->stmtid;

	/*
	 * statement ids are starts by one. For simplicity don't
	 * change base to zero.
	 */
	fextra->parentids[stmtid] = context->parentid;
	fextra->naturalids[stmtid] = ++(*context->naturalid);
	fextra->levels[stmtid] = context->current_deep + 1;

	if (context->current_deep > fextra->max_deep)
		fextra->max_deep = context->current_deep;

	loccontext.fextra = fextra;
	loccontext.parentid = stmtid;
	loccontext.naturalid = context->naturalid;
	loccontext.current_deep = context->current_deep + 1;

	plch_statement_tree_walker(stmt, init_fextra_stmt_walker, NULL, &loccontext);
}

void
plch_init_fidentity_hk(plch_fidentity_hk *hk, PLpgSQL_function *func)
{
	memset(hk, 0, sizeof(plch_fidentity_hk));

	hk->fn_oid = func->fn_oid;
	hk->db_oid = MyDatabaseId;

	hk->fn_xmin = plch_func_xmin(func);
	hk->fn_tid = plch_func_tid(func);
}

static void
pin_func(PLpgSQL_function *func)
{
	/*
	 * We cannot to pin inline block due two reasons:
	 *
	 * 1. After error, the Assert(func->cfunc.use_count == 0);
	 *    is executed before exec memory is released and our
	 *    abort callback is raised.
	 *
	 * 2. There is not any reason, why to do it. inline block
	 *    uses short life contexts for everything - including
	 *    fextra, and then fextra cannot be corrupted. Unfortunately
	 *    inside plpgsql_free_function_memory is free_stmt
	 *    called before MemoryContextDelete(func->fn_cxt)
	 */
	Assert(OidIsValid(func->fn_oid));

	plch_use_count(func)++;
}

static void
unpin_func(PLpgSQL_function *func)
{
	Assert(OidIsValid(func->fn_oid));

	plch_use_count(func)--;
}


static void
fextra_CacheObjectCallback(Datum arg, int cacheid, uint32 hashValue)
{
	HASH_SEQ_STATUS status;
	plch_fextra *fextra;

	if (!fextra_ht)
		return;

	/* Currently we just flush all entries; hard to be smarter ... */
	hash_seq_init(&status, fextra_ht);

	while ((fextra = (plch_fextra *) hash_seq_search(&status)) != NULL)
	{
		if (hashValue == 0 || fextra->hashValue == hashValue)
			fextra->is_valid = false;

		if (!fextra->is_valid && fextra->use_count == 0)
		{
			MemoryContextDelete(fextra->mcxt);
			if (hash_search(fextra_ht,
							&fextra->hk,
							HASH_REMOVE, NULL) == NULL)
				elog(ERROR, "hash table corrupted");
		}
	}
}

plch_fextra *
plch_get_fextra(PLpgSQL_function *func)
{
	plch_fextra *fextra;

	if (OidIsValid(func->fn_oid))
	{
		HASHCTL		ctl;
		plch_fidentity_hk hk;
		bool		found;

		/*
		 * Prepare persistent cache - don't do this for anonymous block
		 */
		if (!fextra_mcxt)
		{
			fextra_mcxt = AllocSetContextCreate(TopMemoryContext,
												"plpgsql_check - fextra cache context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

			Assert(fextra_ht == NULL);

			memset(&ctl, 0, sizeof(ctl));
			ctl.keysize = sizeof(plch_fidentity_hk);
			ctl.entrysize = sizeof(plch_fextra);
			ctl.hcxt = fextra_mcxt;

			fextra_ht = hash_create("plpgsql_check function fextra cache",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

			CacheRegisterSyscacheCallback(PROCOID, fextra_CacheObjectCallback, (Datum) 0);
		}

		plch_init_fidentity_hk(&hk, func);
		fextra = (plch_fextra *) hash_search(fextra_ht, (void *) &hk, HASH_ENTER, &found);

		if (found && !fextra->is_valid && fextra->use_count == 0)
		{
			MemoryContextReset(fextra->mcxt);
		}

		if (!found)
		{
			fextra->mcxt = AllocSetContextCreate(fextra_mcxt,
								  "PLpgSQL fextra entry context",
								  ALLOCSET_DEFAULT_SIZES);

			fextra->hashValue = GetSysCacheHashValue1(PROCOID,
														 ObjectIdGetDatum(func->fn_oid));

			fextra->use_count = 0;
			fextra->is_valid = false;
		}
	}
	else
	{
		/* one shot fextra for anonymous blocks */
		fextra = palloc0(sizeof(plch_fextra));
		fextra->mcxt = CurrentMemoryContext;
		fextra->use_count = 0;
		fextra->is_valid = false;
	}

	if (!fextra->is_valid && fextra->use_count == 0)
	{
		MemoryContext oldcxt;
		char	   *fn_name = NULL;
		char	   *fn_namespacename = NULL;
		int			naturalid = 0;
		fextra_init_context context;

		if (func->fn_oid)
		{
			fn_name = get_func_name(func->fn_oid);
			if (!fn_name)
				fn_name = func->fn_signature;

			fn_namespacename = get_namespace_name_or_temp(get_func_namespace(func->fn_oid));
		}

		oldcxt = MemoryContextSwitchTo(fextra->mcxt);

		fextra->fn_oid = func->fn_oid;
		fextra->fn_name = fn_name ? pstrdup(fn_name) : NULL;
		fextra->fn_namespacename = fn_namespacename ? pstrdup(fn_namespacename) : NULL;
		fextra->fn_signature = func->fn_signature ? pstrdup(func->fn_signature) : NULL;
		fextra->nstatements = func->nstatements;

		fextra->parentids = palloc(sizeof(int) * (func->nstatements + 1));
		fextra->naturalids = palloc(sizeof(int) * (func->nstatements + 1));
		fextra->levels = palloc(sizeof(int) * (func->nstatements + 1));

		MemoryContextSwitchTo(oldcxt);

		fextra->max_deep = 0;

		context.fextra = fextra;
		context.parentid = 0;
		context.current_deep = 0;
		context.naturalid = &naturalid;

		init_fextra_stmt_walker((PLpgSQL_stmt *) func->action, &context);

		fextra->func = func;

		fextra->is_valid = true;
	}

	if (OidIsValid(func->fn_oid))
		pin_func(fextra->func);

	fextra->use_count++;

	return fextra;
}

void
plch_release_fextra(plch_fextra *fextra)
{
	Assert(fextra->use_count > 0);

	/* until now, referenced PLpgSQL_function should be still valid */
	Assert(fextra->hk.fn_oid == fextra->func->fn_oid);

	if (OidIsValid(fextra->fn_oid))
	{
		/*
		 * until now, referenced PLpgSQL_function should be still valid.
		 * Attention - it is not true, for inline block. Pointer fextra->func
		 * when func is inline block is already invalid.
		 */
		Assert(fextra->hk.fn_oid == fextra->func->fn_oid);

		unpin_func(fextra->func);
	}

	fextra->use_count--;
}


#if PG_VERSION_NUM < 150000

void
plch_fextra_deinit()
{
	if (fextra_mcxt)
	{
		MemoryContextDelete(fextra_mcxt);

		fextra_mcxt = NULL;
		fextra_ht = NULL;
	}
}

#endif
