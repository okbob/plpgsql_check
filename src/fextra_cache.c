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

#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static HTAB *fextra_ht = NULL;
static MemoryContext fextra_mcxt = NULL;

static void
init_fextra_stmt(plch_fextra *fextra,
				int parentid, int *naturalid, int level, int cur_deep,
				PLpgSQL_stmt *stmt);

/*
 * Iterate over all plpgsql statements in the list
 */
static void
init_fextra_stmts(plch_fextra *fextra,
				  int parentid, int *naturalid, int level, int cur_deep,
				  List *stmts)
{
	ListCell *lc;

	foreach(lc, stmts)
	{
		init_fextra_stmt(fextra,
						parentid, naturalid, level, cur_deep,
						(PLpgSQL_stmt *) lfirst(lc));
	}
}

/*
 * Iterate over statements tree, and set fextra fields
 */
static void
init_fextra_stmt(plch_fextra *fextra,
				int parentid, int *naturalid, int level, int cur_deep,
				PLpgSQL_stmt *stmt)
{
	int			stmtid = stmt->stmtid;

	/*
	 * statement ids are starts by one. For simplicity don't
	 * change base to zero.
	 */
	fextra->parentids[stmtid] = parentid;
	fextra->naturalids[stmtid] = ++(*naturalid);
	fextra->levels[stmtid] = level;
	fextra->containers[stmtid] = true;

	fextra->natural_to_ids[fextra->naturalids[stmtid]] = stmtid;
	fextra->stmt_typenames[stmtid] = plpgsql_check__stmt_typename_p(stmt);
	fextra->invisible[stmtid] = stmt->lineno < 1;

	/*
	 * When this statement is visible, then nested
	 * statements will be in higher levels.
	 */
	if (stmt->lineno < 1)
		level++;

	if (cur_deep > fextra->max_deep)
		fextra->max_deep = cur_deep;

	cur_deep++;

	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			{
				PLpgSQL_stmt_block *s = (PLpgSQL_stmt_block *) stmt;

				init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
								  s->body);

				if (s->exceptions)
				{
					ListCell *lc;

					foreach(lc, s->exceptions->exc_list)
					{
						init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
										  ((PLpgSQL_exception *) lfirst(lc))->action);
					}
				}
			}
			break;

		case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *s = (PLpgSQL_stmt_if *) stmt;
				ListCell *lc;

				init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
								  s->then_body);

				foreach(lc, s->elsif_list)
				{
					init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
									  ((PLpgSQL_if_elsif *) lfirst(lc))->stmts);
				}

				init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
								  s->else_body);
			}
			break;

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *s = (PLpgSQL_stmt_case *) stmt;
				ListCell *lc;

				foreach(lc, s->case_when_list)
				{
					init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
									  ((PLpgSQL_case_when *) lfirst(lc))->stmts);
				}

				init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
								  s->else_stmts);
			}
			break;

		case PLPGSQL_STMT_LOOP:
			init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
							  ((PLpgSQL_stmt_loop *) stmt)->body);
			break;

		case PLPGSQL_STMT_FORI:
			init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
							  ((PLpgSQL_stmt_fori *) stmt)->body);
			break;

		case PLPGSQL_STMT_FORS:
			init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
							  ((PLpgSQL_stmt_fors *) stmt)->body);
			break;

		case PLPGSQL_STMT_FORC:
			init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
							  ((PLpgSQL_stmt_forc *) stmt)->body);
			break;

		case PLPGSQL_STMT_DYNFORS:
			init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
							  ((PLpgSQL_stmt_dynfors *) stmt)->body);
			break;

		case PLPGSQL_STMT_FOREACH_A:
			init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
							  ((PLpgSQL_stmt_foreach_a *) stmt)->body);
			break;

		case PLPGSQL_STMT_WHILE:
			init_fextra_stmts(fextra, stmtid, naturalid, level, cur_deep,
							  ((PLpgSQL_stmt_while *) stmt)->body);
			break;

		default:
			/* all container statements are handled up */
			fextra->containers[stmtid] = false;
			break;
	}
}

static void
fextra_init_hk(plch_fextra_hk *hk, PLpgSQL_function *func)
{
	memset(hk, 0, sizeof(plch_fextra_hk));

	hk->fn_oid = func->fn_oid;

#if PG_VERSION_NUM >= 180000

	hk->fn_xmin = func->cfunc.fn_xmin;
	hk->fn_tid = func->cfunc.fn_tid;

#else

	hk->fn_xmin = func->fn_xmin;
	hk->fn_tid = func->fn_tid;

#endif

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
		plch_fextra_hk hk;
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
			ctl.keysize = sizeof(plch_fextra_hk);
			ctl.entrysize = sizeof(plch_fextra);
			ctl.hcxt = fextra_mcxt;

			fextra_ht = hash_create("plpgsql_check function fextra cache",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

			CacheRegisterSyscacheCallback(PROCOID, fextra_CacheObjectCallback, (Datum) 0);
		}

		fextra_init_hk(&hk, func);
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
		int			naturalid;

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
		fextra->invisible = palloc(sizeof(bool) * (func->nstatements + 1));
		fextra->naturalids = palloc(sizeof(int) * (func->nstatements + 1));
		fextra->natural_to_ids = palloc(sizeof(int) * (func->nstatements + 1));
		fextra->stmt_typenames = palloc(sizeof(char *) * (func->nstatements + 1));
		fextra->levels = palloc(sizeof(int) * (func->nstatements + 1));
		fextra->containers = palloc(sizeof(bool) * (func->nstatements + 1));

		MemoryContextSwitchTo(oldcxt);

		init_fextra_stmt(fextra, 0, &naturalid, 0, 0, (PLpgSQL_stmt *) func->action);

		fextra->is_valid = true;
	}

	fextra->use_count++;

	return fextra;
}

void
plch_release_fextra(plch_fextra *fextra)
{
	Assert(fextra->use_count > 0);

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
