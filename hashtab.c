/*
 * Profiler implementation
 */

static void
profiler_init_hashkey(profiler_hashkey *hk, PLpgSQL_function *func)
{
	memset(hk, 0, sizeof(profiler_hashkey));

	hk->db_oid = MyDatabaseId;
	hk->fn_oid = func->fn_oid;
	hk->fn_xmin = func->fn_xmin;
	hk->fn_tid = func->fn_tid;
	hk->chunk_num = 1;
}

/*
 * Hash table for function profiling metadata.
 */
static void
profiler_localHashTableInit(void)
{
	HASHCTL		ctl;

	Assert(profiler_HashTable == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(profiler_hashkey);
	ctl.entrysize = sizeof(profiler_profile);
	ctl.hcxt = profiler_mcxt;
	ctl.hash = tag_hash;
	profiler_HashTable = hash_create("plpgsql_check function profiler local cache",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/*
 * Hash table for local function profiles. When shared memory is not available
 * because plpgsql_check was not loaded by shared_proload_libraries, then function
 * profiles is stored in local profile chunks. A format is same for shared profiles.
 */
static void
profiler_chunks_HashTableInit(void)
{
	HASHCTL		ctl;

	Assert(profiler_chunks_HashTable == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(profiler_hashkey);
	ctl.entrysize = sizeof(profiler_stmt_chunk);
	ctl.hcxt = profiler_mcxt;
	ctl.hash = tag_hash;
	profiler_chunks_HashTable = hash_create("plpgsql_check function profiler local chunks",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}


static void
plpgsql_check_HashTableInit(void)
{
	HASHCTL		ctl;

	/* don't allow double-initialization */
	Assert(plpgsql_check_HashTable == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(PLpgSQL_func_hashkey);
	ctl.entrysize = sizeof(plpgsql_check_HashEnt);
	ctl.hash = tag_hash;
	plpgsql_check_HashTable = hash_create("plpgsql_check function cache",
									FUNCS_PER_USER,
									&ctl,
									HASH_ELEM | HASH_FUNCTION);
}

static bool
is_checked(PLpgSQL_function *func)
{
	plpgsql_check_HashEnt *hentry;

	hentry = (plpgsql_check_HashEnt *) hash_search(plpgsql_check_HashTable,
											 (void *) func->fn_hashkey,
											 HASH_FIND,
											 NULL);

	if (hentry != NULL && hentry->fn_xmin == func->fn_xmin &&
			  ItemPointerEquals(&hentry->fn_tid, &func->fn_tid))
		return hentry->is_checked;

	return false;
}

static void
mark_as_checked(PLpgSQL_function *func)
{
	plpgsql_check_HashEnt *hentry;
	bool		found;

	/* don't try to mark anonymous code blocks */
	if (func->fn_oid != InvalidOid)
	{
		hentry = (plpgsql_check_HashEnt *) hash_search(plpgsql_check_HashTable,
												 (void *) func->fn_hashkey,
												 HASH_ENTER,
												 &found);

		hentry->fn_xmin = func->fn_xmin;
		hentry->fn_tid = func->fn_tid;

		hentry->is_checked = true;
	}
}


static void
profiler_init_hash_tables(void)
{
	if (profiler_mcxt)
	{
		MemoryContextReset(profiler_mcxt);

		profiler_HashTable = NULL;
		profiler_chunks_HashTable = NULL;
	}
	else
	{
		profiler_mcxt = AllocSetContextCreate(TopMemoryContext,
												"plpgsql_check - profiler context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);
	}

	profiler_localHashTableInit();
	profiler_chunks_HashTableInit();
}
