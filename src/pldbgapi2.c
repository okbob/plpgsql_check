/*-------------------------------------------------------------------------
 *
 * pldbgapi2
 *
 *			  enhanced debug API for plpgsql
 *
 * by Pavel Stehule 2013-2023
 *
 *-------------------------------------------------------------------------
 *
 * Notes:
 *    PL debug API has few issues related to plpgsql profiler and tracer:
 *
 *    1. Only one extension that use this API can be active
 *
 *    2. Doesn't catch an application's exceptions, and cannot to handle an
 *       exceptions in applications.
 *
 * pldbgapi2 does new interfaces based on pl debug API and fmgr API, and try
 * to solve these issues. It can be used by more at the same time plugins,
 * and allows to set hooks on end of execution of statement or function at
 * aborted state.
 *
 */

#include "postgres.h"
#include "plpgsql.h"
#include "fmgr.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_language.h"
#include "commands/proclang.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "plpgsql_check.h"

#define MAX_PLDBGAPI2_PLUGINS					10
#define INITIAL_PLDBGAPI2_STMT_STACK_SIZE		32

#define FMGR_CACHE_MAGIC		2023071110
#define PLUGIN_INFO_MAGIC		2023071111

static Oid PLpgSQLlanguageId = InvalidOid;
static Oid PLpgSQLinlineFunc = InvalidOid;

typedef struct func_info_hashkey
{
	Oid			fn_oid;
	TransactionId fn_xmin;
	ItemPointerData fn_tid;
} func_info_hashkey;

typedef struct func_info_entry
{
	func_info_hashkey	key;
	uint32		hashValue;
	char	   *fn_name;
	char	   *fn_signature;
	plpgsql_check_plugin2_stmt_info *stmts_info;
	int		   *stmtid_map;
	int			nstatements;

	int			use_count;
	bool		is_valid;
} func_info_entry;

static HTAB *func_info_HashTable = NULL;

typedef struct fmgr_cache
{
	int			magic;

	Oid			funcid;
	bool		is_plpgsql;
	Datum		arg;
} fmgr_cache;

typedef struct fmgr_plpgsql_cache
{
	int			magic;

	Oid			funcid;
	bool		is_plpgsql;
	Datum		arg;

	void	   *plugin2_info[MAX_PLDBGAPI2_PLUGINS];

	MemoryContext fn_mcxt;
	int		   *stmtid_stack;
	int			stmtid_stack_size;
	int			current_stmtid_stack_size;

	func_info_entry *func_info;
} fmgr_plpgsql_cache;

static fmgr_plpgsql_cache *last_fmgr_plpgsql_cache = NULL;

static needs_fmgr_hook_type prev_needs_fmgr_hook = NULL;
static fmgr_hook_type prev_fmgr_hook = NULL;

static plpgsql_check_plugin2 *plpgsql_plugins2[MAX_PLDBGAPI2_PLUGINS];
static int		nplpgsql_plugins2 = 0;

static void pldbgapi2_func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void pldbgapi2_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void pldbgapi2_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void pldbgapi2_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);
static void pldbgapi2_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

static PLpgSQL_plugin pldbgapi2_plugin = { pldbgapi2_func_setup,
										   pldbgapi2_func_beg, pldbgapi2_func_end,
										   pldbgapi2_stmt_beg, pldbgapi2_stmt_end,

#if PG_VERSION_NUM >= 150000

										   NULL, NULL, NULL, NULL, NULL };

#else

										   NULL, NULL };

#endif

static PLpgSQL_plugin  *prev_plpgsql_plugin = NULL;

MemoryContext		pldbgapi2_mcxt = NULL;

typedef struct pldbgapi2_plugin_info
{
	int			magic;

	fmgr_plpgsql_cache *fcache_plpgsql;
	void	   *prev_plugin_info;
} pldbgapi2_plugin_info;

static func_info_entry *get_func_info(PLpgSQL_function *func);

static fmgr_plpgsql_cache *current_fmgr_plpgsql_cache;

plpgsql_check_plugin2_stmt_info *
plpgsql_check_get_current_stmt_info(int stmtid)
{
	Assert(current_fmgr_plpgsql_cache);
	Assert(current_fmgr_plpgsql_cache->func_info);
	Assert(stmtid <= current_fmgr_plpgsql_cache->func_info->nstatements);

	return &(current_fmgr_plpgsql_cache->func_info->stmts_info[stmtid - 1]);
}

plpgsql_check_plugin2_stmt_info *
plpgsql_check_get_current_stmts_info(void)
{
	Assert(current_fmgr_plpgsql_cache);
	Assert(current_fmgr_plpgsql_cache->func_info);
	Assert(current_fmgr_plpgsql_cache->func_info->use_count > 0);

	return current_fmgr_plpgsql_cache->func_info->stmts_info;
}

/*
 * It is used outside pldbapi2 plugins. This is used by output functions,
 * so we don't need to solve effectivity too much. Instead handling use_count
 * returns copy.
 */
plpgsql_check_plugin2_stmt_info *
plpgsql_check_get_stmts_info(PLpgSQL_function *func)
{
	func_info_entry *func_info;
	plpgsql_check_plugin2_stmt_info *stmts_info;
	size_t			bytes;

	func_info = get_func_info(func);
	bytes = func->nstatements * sizeof(plpgsql_check_plugin2_stmt_info);
	stmts_info = palloc(bytes);
	memcpy(stmts_info, func_info->stmts_info, bytes);

	return stmts_info;
}

int *
plpgsql_check_get_current_stmtid_map(void)
{
	Assert(current_fmgr_plpgsql_cache);
	Assert(current_fmgr_plpgsql_cache->func_info);
	Assert(current_fmgr_plpgsql_cache->func_info->use_count > 0);

	return current_fmgr_plpgsql_cache->func_info->stmtid_map;
}

int *
plpgsql_check_get_stmtid_map(PLpgSQL_function *func)
{
	func_info_entry *func_info;
	int		   *stmtid_map;
	size_t		bytes;

	func_info = get_func_info(func);
	bytes = func->nstatements * sizeof(int);
	stmtid_map = palloc(bytes);
	memcpy(stmtid_map, func_info->stmtid_map, bytes);

	return stmtid_map;
}

char *
plpgsql_check_get_current_func_info_name(void)
{
	Assert(current_fmgr_plpgsql_cache);
	Assert(current_fmgr_plpgsql_cache->func_info);
	Assert(current_fmgr_plpgsql_cache->func_info->use_count > 0);

	return current_fmgr_plpgsql_cache->func_info->fn_name;
}

char *
plpgsql_check_get_current_func_info_signature(void)
{
	Assert(current_fmgr_plpgsql_cache);
	Assert(current_fmgr_plpgsql_cache->func_info);
	Assert(current_fmgr_plpgsql_cache->func_info->use_count > 0);
	Assert(current_fmgr_plpgsql_cache->func_info->fn_signature);

	return current_fmgr_plpgsql_cache->func_info->fn_signature;
}

static void
func_info_init_hashkey(func_info_hashkey *hk, PLpgSQL_function *func)
{
	memset(hk, 0, sizeof(func_info_hashkey));

	hk->fn_oid = func->fn_oid;
	hk->fn_xmin = func->fn_xmin;
	hk->fn_tid = func->fn_tid;
}

/*
 * Hash table for function profiling metadata.
 */
static void
func_info_HashTableInit(void)
{
	HASHCTL		ctl;

	Assert(func_info_HashTable == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(func_info_hashkey);
	ctl.entrysize = sizeof(func_info_entry);
	ctl.hcxt = pldbgapi2_mcxt;

	func_info_HashTable = hash_create("plpgsql_check function pldbgapi2 statements info cache",
									   FUNCS_PER_USER,
									   &ctl,
									   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
init_hash_tables(void)
{
	if (pldbgapi2_mcxt)
	{
		MemoryContextReset(pldbgapi2_mcxt);
		func_info_HashTable = NULL;
	}
	else
	{
		pldbgapi2_mcxt = AllocSetContextCreate(TopMemoryContext,
											   "plpgsql_check - pldbgapi2 context",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
	}

	func_info_HashTableInit();
}

static void set_stmt_info(PLpgSQL_stmt *stmt, plpgsql_check_plugin2_stmt_info *stmts_info, int *stmtid_map, int level, int *natural_id, int parent_id);

static void
set_stmts_info(List *stmts,
			   plpgsql_check_plugin2_stmt_info *stmts_info,
			   int *stmtid_map,
			   int level,
			   int *natural_id,
			   int parent_id)
{
	ListCell *lc;

	foreach(lc, stmts)
	{
		set_stmt_info((PLpgSQL_stmt *) lfirst(lc),
					  stmts_info,
					  stmtid_map,
					  level,
					  natural_id,
					  parent_id);
	}
}

static void
set_stmt_info(PLpgSQL_stmt *stmt,
			  plpgsql_check_plugin2_stmt_info *stmts_info,
			  int *stmtid_map,
			  int level,
			  int *natural_id,
			  int parent_id)
{
	ListCell *lc;
	int			stmtid_idx = stmt->stmtid - 1;
	bool		is_invisible =  stmt->lineno < 1;

	Assert(stmts_info);

	/* level is used for indentation */
	stmts_info[stmtid_idx].level = level;

	/* natural_id is displayed insted stmtid */
	stmts_info[stmtid_idx].natural_id = ++(*natural_id);

	/*
	 * natural id to parser id map allows to use natural statement order
	 * for saving metrics and their presentation without necessity to
	 * iterate over statement tree
	 */
	stmtid_map[stmts_info[stmtid_idx].natural_id - 1] = stmt->stmtid;

	/*
	 * parent_id is used for synchronization stmts stack
	 * after handled exception
	 */
	stmts_info[stmtid_idx].parent_id = parent_id;

	/*
	 * persistent stmt type name can be used by tracer
	 * when syntax tree can be unaccessable
	 */
	stmts_info[stmtid_idx].typname = plpgsql_check__stmt_typename_p(stmt);

	/* used for skipping printing invisible block statement */
	stmts_info[stmtid_idx].is_invisible = is_invisible;

	/* by default any statements is not a container of other statements */
	stmts_info[stmtid_idx].is_container = false;

	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			{
				PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) stmt;

				set_stmts_info(stmt_block->body,
							   stmts_info,
							   stmtid_map,
							   !is_invisible ? level + 1 : level,
							   natural_id,
							   stmt->stmtid);

				if (stmt_block->exceptions)
				{
					foreach(lc, stmt_block->exceptions->exc_list)
					{
						set_stmts_info(((PLpgSQL_exception *) lfirst(lc))->action,
									   stmts_info,
									   stmtid_map,
									   !is_invisible ? level + 1 : level,
									   natural_id,
									   stmt->stmtid);
					}
				}

				stmts_info[stmtid_idx].is_container = true;
			}
			break;

		case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;

				set_stmts_info(stmt_if->then_body,
							   stmts_info,
							   stmtid_map,
							   level + 1,
							   natural_id,
							   stmt->stmtid);

				foreach(lc, stmt_if->elsif_list)
				{
					set_stmts_info(((PLpgSQL_if_elsif *) lfirst(lc))->stmts,
								   stmts_info,
								   stmtid_map,
								   level + 1,
								   natural_id,
								   stmt->stmtid);
				}

				set_stmts_info(stmt_if->else_body,
							   stmts_info,
							   stmtid_map,
							   level + 1,
							   natural_id,
							   stmt->stmtid);

				stmts_info[stmtid_idx].is_container = true;
			}
			break;

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;

				foreach(lc, stmt_case->case_when_list)
				{
					set_stmts_info(((PLpgSQL_case_when *) lfirst(lc))->stmts,
								   stmts_info,
								   stmtid_map,
								   level + 1,
								   natural_id,
								   stmt->stmtid);
				}

				set_stmts_info(stmt_case->else_stmts,
							   stmts_info,
							   stmtid_map,
							   level + 1,
							   natural_id,
							   stmt->stmtid);

				stmts_info[stmtid_idx].is_container = true;
			}
			break;

		case PLPGSQL_STMT_LOOP:
			set_stmts_info(((PLpgSQL_stmt_loop *) stmt)->body,
						   stmts_info,
						   stmtid_map,
						   level + 1,
						   natural_id,
						   stmt->stmtid);

			stmts_info[stmtid_idx].is_container = true;
			break;

		case PLPGSQL_STMT_FORI:
			set_stmts_info(((PLpgSQL_stmt_fori *) stmt)->body,
						   stmts_info,
						   stmtid_map,
						   level + 1,
						   natural_id,
						   stmt->stmtid);

			stmts_info[stmtid_idx].is_container = true;
			break;

		case PLPGSQL_STMT_FORS:
			set_stmts_info(((PLpgSQL_stmt_fors *) stmt)->body,
						   stmts_info,
						   stmtid_map,
						   level + 1,
						   natural_id,
						   stmt->stmtid);

			stmts_info[stmtid_idx].is_container = true;
			break;

		case PLPGSQL_STMT_FORC:
			set_stmts_info(((PLpgSQL_stmt_forc *) stmt)->body,
						   stmts_info,
						   stmtid_map,
						   level + 1,
						   natural_id,
						   stmt->stmtid);

			stmts_info[stmtid_idx].is_container = true;
			break;

		case PLPGSQL_STMT_DYNFORS:
			set_stmts_info(((PLpgSQL_stmt_dynfors *) stmt)->body,
						   stmts_info,
						   stmtid_map,
						   level + 1,
						   natural_id,
						   stmt->stmtid);
			stmts_info[stmtid_idx].is_container = true;
			break;

		case PLPGSQL_STMT_FOREACH_A:
			set_stmts_info(((PLpgSQL_stmt_foreach_a *) stmt)->body,
						   stmts_info,
						   stmtid_map,
						   level + 1,
						   natural_id,
						   stmt->stmtid);
			stmts_info[stmtid_idx].is_container = true;
			break;

		case PLPGSQL_STMT_WHILE:
			set_stmts_info(((PLpgSQL_stmt_while *) stmt)->body,
						   stmts_info,
						   stmtid_map,
						   level + 1,
						   natural_id,
						   stmt->stmtid);
			stmts_info[stmtid_idx].is_container = true;
			break;

		default:
			stmts_info[stmtid_idx].is_container = false;
			break;
	}
}

/*
 * Returns oid of used language
 */
static Oid
get_func_lang(Oid funcid)
{
	HeapTuple	procTuple;
	Oid			result;

	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(procTuple))
			elog(ERROR, "cache lookup failed for function %u", funcid);

	result = ((Form_pg_proc) GETSTRUCT(procTuple))->prolang;
	ReleaseSysCache(procTuple);

	return result;
}

/*
 * Set PLpgSQLlanguageId and PLpgSQLinlineFunc
 */
static void
set_plpgsql_info(void)
{
	HeapTuple	languageTuple;
	Form_pg_language languageStruct;

	languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum("plpgsql"));
	if (!HeapTupleIsValid(languageTuple))
		elog(ERROR, "language \"plpgsql\" does not exist");

	languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);

	PLpgSQLlanguageId = languageStruct->oid;
	PLpgSQLinlineFunc = languageStruct->laninline;

	ReleaseSysCache(languageTuple);
}

/*
 * All plpgsql functions needs fmgr hook. We need to process abort state.
 */
static bool
pldbgapi2_needs_fmgr_hook(Oid fn_oid)
{
	if (prev_needs_fmgr_hook && prev_needs_fmgr_hook(fn_oid))
		return true;

	/*
	 * We need to delay initialization of PLpgSQLlanguageId. If library
	 * was initialized too early, the system catalog is not accessable.
	 */
	if (!OidIsValid(PLpgSQLlanguageId))
		set_plpgsql_info();

	/*
	 * code of DO statements is executed by execution of function
	 * laninline. We need to fmgr hook for plpgsql_inline_handler too.
	 */
	if (fn_oid == PLpgSQLinlineFunc)
		return true;

	return get_func_lang(fn_oid) == PLpgSQLlanguageId;
}

static void
pldbgapi2_fmgr_hook(FmgrHookEventType event,
					FmgrInfo *flinfo, Datum *private)
{
	fmgr_cache *fcache = (fmgr_cache *) DatumGetPointer(*private);
	bool		is_pldbgapi2_fcache = false;

	switch (event)
	{
		case FHET_START:
			if (!fcache)
			{
				if (!OidIsValid(PLpgSQLlanguageId))
					set_plpgsql_info();

				if (get_func_lang(flinfo->fn_oid) == PLpgSQLlanguageId ||
					flinfo->fn_oid == PLpgSQLinlineFunc)
				{
					MemoryContext oldcxt = MemoryContextSwitchTo(flinfo->fn_mcxt);
					fmgr_plpgsql_cache *fcache_plpgsql = NULL;

					fcache_plpgsql = palloc0(sizeof(fmgr_plpgsql_cache));

					fcache_plpgsql->magic = FMGR_CACHE_MAGIC;

					fcache_plpgsql->funcid = flinfo->fn_oid;

					fcache_plpgsql->is_plpgsql = true;
					fcache_plpgsql->fn_mcxt = flinfo->fn_mcxt;
					fcache_plpgsql->stmtid_stack = palloc_array(int, INITIAL_PLDBGAPI2_STMT_STACK_SIZE);
					fcache_plpgsql->stmtid_stack_size = INITIAL_PLDBGAPI2_STMT_STACK_SIZE;
					fcache_plpgsql->current_stmtid_stack_size = 0;

					MemoryContextSwitchTo(oldcxt);

					fcache = (fmgr_cache *) fcache_plpgsql;
				}
				else
				{
					fcache = MemoryContextAlloc(flinfo->fn_mcxt, sizeof(fmgr_cache));

					fcache->magic = FMGR_CACHE_MAGIC;

					fcache->funcid = flinfo->fn_oid;
					fcache->is_plpgsql = false;
					fcache->arg = (Datum) 0;
				}

				*private = PointerGetDatum(fcache);
			}

			if (fcache && fcache->magic != FMGR_CACHE_MAGIC)
				elog(ERROR, "unexpected fmgr_hook cache magic number");

			is_pldbgapi2_fcache = true;

			if (fcache->is_plpgsql)
			{
				fmgr_plpgsql_cache *fcache_plpgsql = (fmgr_plpgsql_cache *) fcache;

				last_fmgr_plpgsql_cache = fcache_plpgsql;
				fcache_plpgsql->current_stmtid_stack_size = 0;
			}
			else
				last_fmgr_plpgsql_cache = NULL;

			break;

		case FHET_END:
		case FHET_ABORT:
			/*
			 * Unfortunately, the fmgr hook can be redirected inside security definer
			 * function, and then there can be possible to so FHET_END or FHET_ABORT
			 * are called with private for previous plugin. In this case, the best
			 * solution is probably do nothing, and skip processing to previous
			 * plugin.
			 */
			is_pldbgapi2_fcache = (fcache && fcache->magic == FMGR_CACHE_MAGIC);

			if (is_pldbgapi2_fcache && event == FHET_ABORT && fcache->is_plpgsql)
			{
				fmgr_plpgsql_cache *fcache_plpgsql = (fmgr_plpgsql_cache *) fcache;
				int			sp;
				int			i;
				Oid			fn_oid;

				Assert(fcache_plpgsql->funcid == flinfo->fn_oid);

				fn_oid = flinfo->fn_oid != PLpgSQLinlineFunc ? flinfo->fn_oid : InvalidOid;

				current_fmgr_plpgsql_cache = fcache_plpgsql;

				for (sp = fcache_plpgsql->current_stmtid_stack_size; sp > 0; sp--)
				{
					int			stmtid = fcache_plpgsql->stmtid_stack[sp - 1];

					for (i = 0; i < nplpgsql_plugins2; i++)
					{
						if (plpgsql_plugins2[i]->stmt_end2_aborted)
							(plpgsql_plugins2[i]->stmt_end2_aborted)(fn_oid, stmtid,
																	 &fcache_plpgsql->plugin2_info[i]);
					}
				}

				for (i = 0; i < nplpgsql_plugins2; i++)
				{
					if (plpgsql_plugins2[i]->func_end2_aborted)
						(plpgsql_plugins2[i]->func_end2_aborted)(fn_oid,
																 &fcache_plpgsql->plugin2_info[i]);
				}

				current_fmgr_plpgsql_cache = NULL;

				if (fcache_plpgsql->func_info)
				{
					Assert(fcache_plpgsql->func_info->use_count > 0);

					fcache_plpgsql->func_info->use_count--;
				}
			}
			break;
	}

	if (prev_fmgr_hook)
		(*prev_fmgr_hook) (event, flinfo, is_pldbgapi2_fcache ? &fcache->arg : private);
}

static func_info_entry *
get_func_info(PLpgSQL_function *func)
{
	func_info_entry *func_info;
	bool		persistent_func_info;
	bool		found_func_info_entry;
	func_info_hashkey hk;

	if (OidIsValid(func->fn_oid))
	{
		func_info_init_hashkey(&hk, func);
		func_info = (func_info_entry *) hash_search(func_info_HashTable,
													(void *) &hk,
													HASH_ENTER,
													&found_func_info_entry);

		if (found_func_info_entry && !func_info->is_valid)
		{
			pfree(func_info->fn_name);
			pfree(func_info->fn_signature);
			pfree(func_info->stmts_info);
			pfree(func_info->stmtid_map);

			if (hash_search(func_info_HashTable,
							&func_info->key,
							HASH_REMOVE, NULL) == NULL)
				elog(ERROR, "hash table corrupted");

			found_func_info_entry = false;
		}

		persistent_func_info = true;
	}
	else
	{
		/* one shot sie for anonymous blocks */
		func_info = palloc(sizeof(func_info_entry));
		persistent_func_info = false;
		found_func_info_entry = false;
	}

	if (!found_func_info_entry)
	{
		char	   *fn_name;
		MemoryContext oldcxt;
		int			natural_id = 0;

		fn_name = get_func_name(func->fn_oid);

		if (persistent_func_info)
		{
			oldcxt = MemoryContextSwitchTo(pldbgapi2_mcxt);

			Assert(fn_name);

			func_info->hashValue = GetSysCacheHashValue1(PROCOID,
														 ObjectIdGetDatum(func->fn_oid));

			func_info->fn_name = pstrdup(fn_name);
			func_info->fn_signature = pstrdup(func->fn_signature);
			func_info->stmts_info = palloc(func->nstatements *
										   sizeof(plpgsql_check_plugin2_stmt_info));
			func_info->stmtid_map = palloc(func->nstatements * sizeof(int));
			func_info->use_count = 0;

			MemoryContextSwitchTo(oldcxt);
		}
		else
		{
			func_info->fn_name = fn_name;
			func_info->fn_signature = pstrdup(func->fn_signature);
			func_info->stmts_info = palloc(func->nstatements *
										   sizeof(plpgsql_check_plugin2_stmt_info));
			func_info->stmtid_map = palloc(func->nstatements * sizeof(int));
		}

		func_info->nstatements = func->nstatements;
		func_info->use_count = 0;
		func_info->is_valid = true;

		set_stmt_info((PLpgSQL_stmt *) func->action,
					  func_info->stmts_info,
					  func_info->stmtid_map,
					  1, &natural_id, 0);
	}

	func_info->nstatements = func->nstatements;

	return func_info;
}

static void
pldbgapi2_func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	fmgr_plpgsql_cache *fcache_plpgsql = last_fmgr_plpgsql_cache;
	pldbgapi2_plugin_info *plugin_info;
	MemoryContext oldcxt;
	func_info_entry *func_info;
	int			i;

	Assert(fcache_plpgsql->magic == FMGR_CACHE_MAGIC);
	Assert(fcache_plpgsql);
	Assert(fcache_plpgsql->is_plpgsql);

#ifdef USE_ASSERT_CHECKING

	if (fcache_plpgsql->funcid != PLpgSQLinlineFunc)
	{
		Assert(fcache_plpgsql->funcid == func->fn_oid);
		Assert(fcache_plpgsql->funcid == estate->func->fn_oid);
	}
	else
	{
		Assert(!OidIsValid(func->fn_oid));
		Assert(!OidIsValid(estate->func->fn_oid));
	}

#endif

	plugin_info = MemoryContextAlloc(fcache_plpgsql->fn_mcxt, sizeof(pldbgapi2_plugin_info));

	plugin_info->magic = PLUGIN_INFO_MAGIC;

	plugin_info->fcache_plpgsql = fcache_plpgsql;
	plugin_info->prev_plugin_info = NULL;

	func_info = get_func_info(func);
	/* protect func_info against sinval */
	func_info->use_count++;

	fcache_plpgsql->func_info = func_info;

	estate->plugin_info = plugin_info;

	current_fmgr_plpgsql_cache = fcache_plpgsql;

	for (i = 0; i < nplpgsql_plugins2; i++)
	{
		fcache_plpgsql->plugin2_info[i] = NULL;

		plpgsql_plugins2[i]->error_callback = pldbgapi2_plugin.error_callback;
		plpgsql_plugins2[i]->assign_expr = pldbgapi2_plugin.assign_expr;

#if PG_VERSION_NUM >= 150000

		plpgsql_plugins2[i]->assign_value = pldbgapi2_plugin.assign_value;
		plpgsql_plugins2[i]->eval_datum = pldbgapi2_plugin.eval_datum;
		plpgsql_plugins2[i]->cast_value = pldbgapi2_plugin.cast_value;

#else

		plpgsql_plugins2[i]->assign_value = NULL;
		plpgsql_plugins2[i]->eval_datum = NULL;
		plpgsql_plugins2[i]->cast_value = NULL;

#endif

		oldcxt = MemoryContextSwitchTo(fcache_plpgsql->fn_mcxt);

		if (plpgsql_plugins2[i]->func_setup2)
			(plpgsql_plugins2[i]->func_setup2)(estate, func, &fcache_plpgsql->plugin2_info[i]);

		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_plpgsql_plugin)
	{
		prev_plpgsql_plugin->error_callback = pldbgapi2_plugin.error_callback;
		prev_plpgsql_plugin->assign_expr = pldbgapi2_plugin.assign_expr;

#if PG_VERSION_NUM >= 150000

		prev_plpgsql_plugin->assign_value = pldbgapi2_plugin.assign_value;
		prev_plpgsql_plugin->eval_datum = pldbgapi2_plugin.eval_datum;
		prev_plpgsql_plugin->cast_value = pldbgapi2_plugin.cast_value;

#endif

		if (prev_plpgsql_plugin->func_setup)
		{
			PG_TRY();
			{
				(prev_plpgsql_plugin->func_setup)(estate, func);

				plugin_info->prev_plugin_info = estate->plugin_info;
				estate->plugin_info = plugin_info;
			}
			PG_CATCH();
			{
				plugin_info->prev_plugin_info = estate->plugin_info;
				estate->plugin_info = plugin_info;

				PG_RE_THROW();
			}
			PG_END_TRY();
		}
	}

	estate->plugin_info = plugin_info;

	current_fmgr_plpgsql_cache = NULL;
}

static void
pldbgapi2_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	pldbgapi2_plugin_info *plugin_info = estate->plugin_info;
	fmgr_plpgsql_cache *fcache_plpgsql;
	int			i;

	Assert(plugin_info);

	if (plugin_info->magic != PLUGIN_INFO_MAGIC)
		ereport(ERROR,
				(errmsg("bad magic number of pldbgapi2 plpgsql debug api hook"),
				 errdetail("Some extension using pl debug api does not work correctly.")));

	fcache_plpgsql = plugin_info->fcache_plpgsql;

	Assert(fcache_plpgsql->magic == FMGR_CACHE_MAGIC);
	Assert(fcache_plpgsql);
	Assert(fcache_plpgsql->is_plpgsql);

#ifdef USE_ASSERT_CHECKING

	if (fcache_plpgsql->funcid != PLpgSQLinlineFunc)
	{
		Assert(fcache_plpgsql->funcid == func->fn_oid);
		Assert(fcache_plpgsql->funcid == estate->func->fn_oid);
	}
	else
	{
		Assert(!OidIsValid(func->fn_oid));
		Assert(!OidIsValid(estate->func->fn_oid));
	}

#endif

	current_fmgr_plpgsql_cache = fcache_plpgsql;

	for (i = 0; i < nplpgsql_plugins2; i++)
	{
		if (plpgsql_plugins2[i]->func_beg2)
			(plpgsql_plugins2[i]->func_beg2)(estate, func, &fcache_plpgsql->plugin2_info[i]);
	}

	current_fmgr_plpgsql_cache = NULL;

	if (prev_plpgsql_plugin && prev_plpgsql_plugin->func_beg)
	{
		PG_TRY();
		{
			estate->plugin_info = plugin_info->prev_plugin_info;

			(prev_plpgsql_plugin->func_beg)(estate, func);

			plugin_info->prev_plugin_info = estate->plugin_info;
			estate->plugin_info = plugin_info;
		}
		PG_CATCH();
		{
			plugin_info->prev_plugin_info = estate->plugin_info;
			estate->plugin_info = plugin_info;

			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}

static void
pldbgapi2_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	pldbgapi2_plugin_info *plugin_info = estate->plugin_info;
	fmgr_plpgsql_cache *fcache_plpgsql;
	int			i;

	if (!plugin_info)
		return;

	if (plugin_info->magic != PLUGIN_INFO_MAGIC)
	{
		ereport(WARNING,
				(errmsg("bad magic number of pldbgapi2 plpgsql debug api hook"),
				 errdetail("Some extension using pl debug api does not work correctly.")));
		return;
	}

	fcache_plpgsql = plugin_info->fcache_plpgsql;

	Assert(fcache_plpgsql->magic == FMGR_CACHE_MAGIC);
	Assert(fcache_plpgsql);
	Assert(fcache_plpgsql->is_plpgsql);

#ifdef USE_ASSERT_CHECKING

	if (fcache_plpgsql->funcid != PLpgSQLinlineFunc)
	{
		Assert(fcache_plpgsql->funcid == func->fn_oid);
		Assert(fcache_plpgsql->funcid == estate->func->fn_oid);
	}
	else
		Assert(!OidIsValid(estate->func->fn_oid));

#endif

	current_fmgr_plpgsql_cache = fcache_plpgsql;

	for (i = 0; i < nplpgsql_plugins2; i++)
	{
		if (plpgsql_plugins2[i]->func_end2)
			(plpgsql_plugins2[i]->func_end2)(estate, func, &fcache_plpgsql->plugin2_info[i]);
	}

	current_fmgr_plpgsql_cache = NULL;

	Assert(fcache_plpgsql->func_info);
	Assert(fcache_plpgsql->func_info->use_count > 0);
	fcache_plpgsql->func_info->use_count--;

	if (prev_plpgsql_plugin && prev_plpgsql_plugin->func_end)
	{
		PG_TRY();
		{
			estate->plugin_info = plugin_info->prev_plugin_info;

			(prev_plpgsql_plugin->func_end)(estate, func);

			plugin_info->prev_plugin_info = estate->plugin_info;
			estate->plugin_info = plugin_info;
		}
		PG_CATCH();
		{
			plugin_info->prev_plugin_info = estate->plugin_info;
			estate->plugin_info = plugin_info;

			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}

static void
pldbgapi2_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	pldbgapi2_plugin_info *plugin_info = estate->plugin_info;
	fmgr_plpgsql_cache *fcache_plpgsql;
	int			i;
	int			parent_id = 0;

	Assert(plugin_info);

	if (plugin_info->magic != PLUGIN_INFO_MAGIC)
		ereport(ERROR,
				(errmsg("bad magic number of pldbgapi2 plpgsql debug api hook"),
				 errdetail("Some extension using pl debug api does not work correctly.")));

	fcache_plpgsql = plugin_info->fcache_plpgsql;

	Assert(fcache_plpgsql->magic == FMGR_CACHE_MAGIC);
	Assert(fcache_plpgsql);
	Assert(fcache_plpgsql->is_plpgsql);

#ifdef USE_ASSERT_CHECKING

	if (fcache_plpgsql->funcid != PLpgSQLinlineFunc)
		Assert(fcache_plpgsql->funcid == estate->func->fn_oid);
	else
		Assert(!OidIsValid(estate->func->fn_oid));

#endif

	current_fmgr_plpgsql_cache = fcache_plpgsql;

	if (fcache_plpgsql->current_stmtid_stack_size > 0)
	{
		parent_id = fcache_plpgsql->func_info->stmts_info[stmt->stmtid - 1].parent_id;

		/* solve handled exception */
		while (fcache_plpgsql->current_stmtid_stack_size > 0 &&
			   fcache_plpgsql->stmtid_stack[fcache_plpgsql->current_stmtid_stack_size - 1] != parent_id)
		{
			int			stmtid = fcache_plpgsql->stmtid_stack[fcache_plpgsql->current_stmtid_stack_size - 1];

			for (i = 0; i < nplpgsql_plugins2; i++)
			{
				if (plpgsql_plugins2[i]->stmt_end2_aborted)
					(plpgsql_plugins2[i]->stmt_end2_aborted)(estate->func->fn_oid, stmtid,
															 &fcache_plpgsql->plugin2_info[i]);
			}

			fcache_plpgsql->current_stmtid_stack_size -= 1;
		}
	}

	if (parent_id &&
		  fcache_plpgsql->stmtid_stack[fcache_plpgsql->current_stmtid_stack_size - 1] != parent_id)
		elog(ERROR, "cannot find parent statement on pldbgapi2 call stack");

	/*
	 * We want to close broken statements before we start execution of 
	 * exception handler. This needs more work than closing broken statements
	 * after an exception handler, but it simplify calculation of execution times.
	 * We need to chec, if stack has an expected value (should be parent statement,
	 * and if not, then we are in exception handler.
	 */

	if (fcache_plpgsql->current_stmtid_stack_size >= fcache_plpgsql->stmtid_stack_size)
	{
		fcache_plpgsql->stmtid_stack_size *= 2;
		fcache_plpgsql->stmtid_stack = repalloc_array(fcache_plpgsql->stmtid_stack,
													  int, fcache_plpgsql->stmtid_stack_size);
	}

	fcache_plpgsql->stmtid_stack[fcache_plpgsql->current_stmtid_stack_size++] = stmt->stmtid;


	for (i = 0; i < nplpgsql_plugins2; i++)
	{
		if (plpgsql_plugins2[i]->stmt_beg2)
			(plpgsql_plugins2[i]->stmt_beg2)(estate, stmt,
											 &fcache_plpgsql->plugin2_info[i]);
	}

	current_fmgr_plpgsql_cache = NULL;

	if (prev_plpgsql_plugin && prev_plpgsql_plugin->stmt_beg)
	{
		PG_TRY();
		{
			estate->plugin_info = plugin_info->prev_plugin_info;

			(prev_plpgsql_plugin->stmt_beg)(estate, stmt);

			plugin_info->prev_plugin_info = estate->plugin_info;
			estate->plugin_info = plugin_info;
		}
		PG_CATCH();
		{
			plugin_info->prev_plugin_info = estate->plugin_info;
			estate->plugin_info = plugin_info;

			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}

static void
pldbgapi2_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	pldbgapi2_plugin_info *plugin_info = estate->plugin_info;
	fmgr_plpgsql_cache *fcache_plpgsql;
	int			i;

	if (!plugin_info)
		return;

	if (plugin_info->magic != PLUGIN_INFO_MAGIC)
	{
		ereport(WARNING,
				(errmsg("bad magic number of pldbgapi2 plpgsql debug api hook"),
				 errdetail("Some extension using pl debug api does not work correctly.")));
		return;
	}

	fcache_plpgsql = plugin_info->fcache_plpgsql;

	Assert(fcache_plpgsql->magic == FMGR_CACHE_MAGIC);
	Assert(fcache_plpgsql);
	Assert(fcache_plpgsql->is_plpgsql);

#ifdef USE_ASSERT_CHECKING

	if (fcache_plpgsql->funcid != PLpgSQLinlineFunc)
		Assert(fcache_plpgsql->funcid == estate->func->fn_oid);
	else
		Assert(!OidIsValid(estate->func->fn_oid));

#endif

	Assert(fcache_plpgsql->current_stmtid_stack_size > 0);

	fcache_plpgsql->current_stmtid_stack_size -= 1;

	current_fmgr_plpgsql_cache = fcache_plpgsql;

	if (fcache_plpgsql->stmtid_stack[fcache_plpgsql->current_stmtid_stack_size] != stmt->stmtid)
		elog(ERROR, "pldbgapi2 statement call stack is broken");

	for (i = 0; i < nplpgsql_plugins2; i++)
	{
		if (plpgsql_plugins2[i]->stmt_end2)
			(plpgsql_plugins2[i]->stmt_end2)(estate, stmt,
											 &fcache_plpgsql->plugin2_info[i]);
	}

	current_fmgr_plpgsql_cache = NULL;

	if (prev_plpgsql_plugin && prev_plpgsql_plugin->stmt_end)
	{
		PG_TRY();
		{
			estate->plugin_info = plugin_info->prev_plugin_info;

			(prev_plpgsql_plugin->stmt_end)(estate, stmt);

			plugin_info->prev_plugin_info = estate->plugin_info;
			estate->plugin_info = plugin_info;
		}
		PG_CATCH();
		{
			plugin_info->prev_plugin_info = estate->plugin_info;
			estate->plugin_info = plugin_info;

			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}

void
plpgsql_check_register_pldbgapi2_plugin(plpgsql_check_plugin2 *plugin2)
{
	if (nplpgsql_plugins2 < MAX_PLDBGAPI2_PLUGINS)
		plpgsql_plugins2[nplpgsql_plugins2++] = plugin2;
	else
		elog(ERROR, "too much pldbgapi2 plugins");
}

static void
func_info_CacheObjectCallback(Datum arg, int cacheid, uint32 hashValue)
{
	HASH_SEQ_STATUS status;
	func_info_entry *func_info;

	Assert(func_info_HashTable);

	/* Currently we just flush all entries; hard to be smarter ... */
	hash_seq_init(&status, func_info_HashTable);

	while ((func_info = (func_info_entry *) hash_seq_search(&status)) != NULL)
	{
		if (hashValue == 0 || func_info->hashValue == hashValue)
			func_info->is_valid = false;

		if (!func_info->is_valid && func_info->use_count == 0)
		{
			pfree(func_info->fn_name);
			pfree(func_info->fn_signature);
			pfree(func_info->stmts_info);
			pfree(func_info->stmtid_map);

			if (hash_search(func_info_HashTable,
							&func_info->key,
							HASH_REMOVE, NULL) == NULL)
				elog(ERROR, "hash table corrupted");
		}
	}
}

void
plpgsql_check_init_pldbgapi2(void)
{
	PLpgSQL_plugin **plugin_ptr;
	static bool		inited = false;

	if (inited)
		return;

	prev_needs_fmgr_hook = needs_fmgr_hook;
	prev_fmgr_hook = fmgr_hook;

	needs_fmgr_hook = pldbgapi2_needs_fmgr_hook;
	fmgr_hook = pldbgapi2_fmgr_hook;

	plugin_ptr = (PLpgSQL_plugin **)find_rendezvous_variable("PLpgSQL_plugin");
	prev_plpgsql_plugin = *plugin_ptr;
	*plugin_ptr = &pldbgapi2_plugin;

	init_hash_tables();

	CacheRegisterSyscacheCallback(PROCOID, func_info_CacheObjectCallback, (Datum) 0);

	inited = true;
}

#if PG_VERSION_NUM < 150000

void
plpgsql_check_finish_pldbgapi2(void)
{
	PLpgSQL_plugin **plugin_ptr;

	needs_fmgr_hook = prev_needs_fmgr_hook;
	fmgr_hook = prev_fmgr_hook;

	plugin_ptr = (PLpgSQL_plugin **)find_rendezvous_variable("PLpgSQL_plugin");
	*plugin_ptr = prev_plpgsql_plugin;
}

#endif
