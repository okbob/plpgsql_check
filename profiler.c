/*-------------------------------------------------------------------------
 *
 * profiler.c
 *
 *			  profiler accessories code
 *
 * by Pavel Stehule 2013-2018
 *
 *-------------------------------------------------------------------------
 */

/*
 * This function should to iterate over all plpgsql commands to:
 *   count statements and build statement -> uniq id map,
 *   collect counted metrics.
 */
static void
profiler_touch_stmt(profiler_info *pinfo,
					PLpgSQL_stmt *stmt,
					bool generate_map,
					bool finalize_profile,
					int64 *nested_us_total)
{
	int64		us_total = 0;
	profiler_profile *profile = pinfo->profile;
	profiler_stmt *pstmt = NULL;

	if (generate_map)
		profiler_update_map(profile, stmt);

	if (finalize_profile)
	{
		int stmtid = profiler_get_stmtid(profile, stmt);

		*nested_us_total = 0;

		pstmt = &pinfo->stmts[stmtid];
		pstmt->lineno = stmt->lineno;
	}

	switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			{
				PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) stmt;

				profiler_touch_stmts(pinfo,
									 stmt_block->body,
									 generate_map,
									 finalize_profile,
									 &us_total);

				if (finalize_profile)
					*nested_us_total += us_total;

				if (stmt_block->exceptions)
				{
					ListCell *lc;

					foreach(lc, stmt_block->exceptions->exc_list)
					{
						profiler_touch_stmts(pinfo,
											 ((PLpgSQL_exception *) lfirst(lc))->action,
											 generate_map,
											 finalize_profile,
											 &us_total);

						if (finalize_profile)
							*nested_us_total += us_total;
					}
				}

				if (finalize_profile)
				{
					pstmt->us_total -= *nested_us_total;

					/*
					 * the max time can be calculated only when this node
					 * was executed once!
					 */
					if (pstmt->exec_count == 1)
						pstmt->us_max = pstmt->us_total;
					else
						pstmt->us_max = 0;

					*nested_us_total += pstmt->us_total;
				}
			}
			break;

		case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;
				ListCell *lc;

				profiler_touch_stmts(pinfo,
									 stmt_if->then_body,
									 generate_map,
									 finalize_profile,
									 &us_total);

				if (finalize_profile)
					*nested_us_total += us_total;

				foreach(lc, stmt_if->elsif_list)
				{
					profiler_touch_stmts(pinfo,
										 ((PLpgSQL_if_elsif *) lfirst(lc))->stmts,
										 generate_map,
										 finalize_profile,
										 &us_total);

					if (finalize_profile)
						*nested_us_total += us_total;
				}

				profiler_touch_stmts(pinfo,
									 stmt_if->else_body,
									 generate_map,
									 finalize_profile,
									 &us_total);

				if (finalize_profile)
					*nested_us_total += us_total;

				if (finalize_profile)
				{
					pstmt->us_total -= *nested_us_total;

					/*
					 * the max time can be calculated only when this node
					 * was executed once!
					 */
					if (pstmt->exec_count == 1)
						pstmt->us_max = pstmt->us_total;
					else
						pstmt->us_max = 0;

					*nested_us_total += pstmt->us_total;
				}
			}
			break;

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;
				ListCell *lc;

				foreach(lc, stmt_case->case_when_list)
				{
					profiler_touch_stmts(pinfo,
										 ((PLpgSQL_case_when *) lfirst(lc))->stmts,
										 generate_map,
										 finalize_profile,
										 &us_total);

					if (finalize_profile)
						*nested_us_total += us_total;

				}

				profiler_touch_stmts(pinfo,
									 stmt_case->else_stmts,
									 generate_map,
									 finalize_profile,
									 &us_total);

				if (finalize_profile)
					*nested_us_total += us_total;

				if (finalize_profile)
				{
					pstmt->us_total -= *nested_us_total;

					/*
					 * the max time can be calculated only when this node
					 * was executed once!
					 */
					if (pstmt->exec_count == 1)
						pstmt->us_max = pstmt->us_total;
					else
						pstmt->us_max = 0;

					*nested_us_total += pstmt->us_total;
				}
			}
			break;

		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_FORS:
		case PLPGSQL_STMT_FORC:
		case PLPGSQL_STMT_DYNFORS:
		case PLPGSQL_STMT_FOREACH_A:
			{
				List   *stmts;

				switch (PLPGSQL_STMT_TYPES stmt->cmd_type)
				{
					case PLPGSQL_STMT_LOOP:
						stmts = ((PLpgSQL_stmt_while *) stmt)->body;
						break;
					case PLPGSQL_STMT_FORI:
						stmts = ((PLpgSQL_stmt_fori *) stmt)->body;
						break;
					case PLPGSQL_STMT_FORS:
						stmts = ((PLpgSQL_stmt_fors *) stmt)->body;
						break;
					case PLPGSQL_STMT_FORC:
						stmts = ((PLpgSQL_stmt_forc *) stmt)->body;
						break;
					case PLPGSQL_STMT_DYNFORS:
						stmts = ((PLpgSQL_stmt_dynfors *) stmt)->body;
						break;
					case PLPGSQL_STMT_FOREACH_A:
						stmts = ((PLpgSQL_stmt_foreach_a *) stmt)->body;
						break;
					default:
						stmts = NIL;
						break;
				}

				profiler_touch_stmts(pinfo,
								 stmts,
								 generate_map,
								 finalize_profile,
								 &us_total);

				if (finalize_profile)
					*nested_us_total += us_total;

				if (finalize_profile)
				{
					pstmt->us_total -= *nested_us_total;

					/*
					 * the max time can be calculated only when this node
					 * was executed once!
					 */
					if (pstmt->exec_count == 1)
						pstmt->us_max = pstmt->us_total;
					else
						pstmt->us_max = 0;

					*nested_us_total += pstmt->us_total;
				}
			}
			break;

		default:
			if (finalize_profile)
				*nested_us_total = pstmt->us_total;
			break;
	}
}


/*
 * clean all chunks used by profiler
 */
Datum
plpgsql_profiler_reset_all(PG_FUNCTION_ARGS)
{
	if (shared_profiler_chunks_HashTable)
	{
		HASH_SEQ_STATUS			hash_seq;
		profiler_stmt_chunk    *chunk;

		LWLockAcquire(profiler_ss->lock, LW_EXCLUSIVE);

		hash_seq_init(&hash_seq, shared_profiler_chunks_HashTable);

		while ((chunk = hash_seq_search(&hash_seq)) != NULL)
		{
			hash_search(shared_profiler_chunks_HashTable, &(chunk->key), HASH_REMOVE, NULL);
		}

		LWLockRelease(profiler_ss->lock);
	}
	else
		profiler_init_hash_tables();

	PG_RETURN_VOID();
}

/*
 * Clean chunks related to some function
 */
Datum
plpgsql_profiler_reset(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	profiler_hashkey hk;
	HTAB	   *chunks;
	HeapTuple	procTuple;
	bool		found;
	bool		shared_chunks;

	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);

	/* ensure correct complete content of hash key */
	memset(&hk, 0, sizeof(profiler_hashkey));
	hk.fn_oid = funcoid;
	hk.db_oid = MyDatabaseId;
	hk.fn_xmin = HeapTupleHeaderGetRawXmin(procTuple->t_data);
	hk.fn_tid =  procTuple->t_self;
	hk.chunk_num = 1;

	ReleaseSysCache(procTuple);

	if (shared_profiler_chunks_HashTable)
	{
		LWLockAcquire(profiler_ss->lock, LW_EXCLUSIVE);
		chunks = shared_profiler_chunks_HashTable;
		shared_chunks = true;
	}
	else
	{
		chunks = profiler_chunks_HashTable;
		shared_chunks = false;
	}

	for(;;)
	{
		hash_search(chunks, (void *) &hk, HASH_REMOVE, &found);
		if (!found)
			break;
		hk.chunk_num += 1;
	}

	if (shared_chunks)
		LWLockRelease(profiler_ss->lock);

	PG_RETURN_VOID();
}

static void
update_persistent_profile(profiler_info *pinfo, PLpgSQL_function *func)
{
	profiler_profile *profile = pinfo->profile;
	profiler_hashkey hk;
	profiler_stmt_chunk *chunk;
	profiler_stmt_chunk *first_chunk = NULL;
	bool		found;
	int			i;
	int			stmt_counter = 0;
	HTAB	   *chunks;
	bool		shared_chunks;
	bool		exclusive_lock = false;
	volatile bool unlock_mutex = false;

	if (shared_profiler_chunks_HashTable)
	{
		chunks = shared_profiler_chunks_HashTable;
		LWLockAcquire(profiler_ss->lock, LW_SHARED);
		shared_chunks = true;
	}
	else
	{
		chunks = profiler_chunks_HashTable;
		shared_chunks = false;
	}

	profiler_init_hashkey(&hk, func);

	/* don't need too strong lock for shared memory */
	chunk = (profiler_stmt_chunk *) hash_search(chunks,
											 (void *) &hk,
											 HASH_FIND,
											 &found);

	/* We need exclusive lock */
	if (!found && shared_chunks)
	{
		LWLockRelease(profiler_ss->lock);
		LWLockAcquire(profiler_ss->lock, LW_EXCLUSIVE);
		exclusive_lock = true;

		chunk = (profiler_stmt_chunk *) hash_search(chunks,
												 (void *) &hk,
												 HASH_ENTER,
												 &found);
	}

	if (!found)
	{
		int		i;
		int		stmt_counter;

		/* first shared chunk is prepared already. local chunk should be done */
		if (shared_chunks)
		{
			/* for first chunk we need to initialize mutex */
			SpinLockInit(&chunk->mutex);
			stmt_counter = 0;
		}
		else
			stmt_counter = -1;

		/* we should to enter empty chunks first */
		for (i = 0; i < profile->nstatements; i++)
		{
			profiler_stmt_reduced *prstmt;
			profiler_stmt *pstmt = &pinfo->stmts[i];

			hk.chunk_num = 0;

			if (stmt_counter == -1 || stmt_counter >= STATEMENTS_PER_CHUNK)
			{
				hk.chunk_num += 1;

				chunk = (profiler_stmt_chunk *) hash_search(chunks,
															 (void *) &hk,
															 HASH_ENTER,
															 &found);

				if (found)
					elog(ERROR, "broken consistency of plpgsql_check profiler chunks");

				stmt_counter = 0;
			}

			prstmt = &chunk->stmts[stmt_counter++];

			prstmt->lineno = pstmt->lineno;
			prstmt->us_max = pstmt->us_max;
			prstmt->us_total = pstmt->us_total;
			prstmt->rows = pstmt->rows;
			prstmt->exec_count = pstmt->exec_count;
		}

		/* clean unused stmts in chunk */
		while (stmt_counter < STATEMENTS_PER_CHUNK)
			chunk->stmts[stmt_counter++].lineno = -1;

		if (shared_chunks)
			LWLockRelease(profiler_ss->lock);

		return;
	}

	/* if we have not exclusive lock, we should to lock first chunk */
	PG_TRY();
	{
		if (shared_chunks && !exclusive_lock)
		{
			first_chunk = chunk;
			SpinLockAcquire(&first_chunk->mutex);
			unlock_mutex = true;
		}

		/* there is a profiler chunk already */
		for (i = 0; i < profile->nstatements; i++)
		{
			profiler_stmt_reduced *prstmt;
			profiler_stmt *pstmt = &pinfo->stmts[i];

			if (stmt_counter >= STATEMENTS_PER_CHUNK)
			{
				hk.chunk_num += 1;

				chunk = (profiler_stmt_chunk *) hash_search(chunks,
															 (void *) &hk,
															 HASH_FIND,
															 &found);

				if (!found)
					elog(ERROR, "broken consistency of plpgsql_check profiler chunks");

				stmt_counter = 0;
			}

			prstmt = &chunk->stmts[stmt_counter++];

			if (prstmt->lineno != pstmt->lineno)
				elog(ERROR, "broken consistency of plpgsql_check profiler chunks");

			if (prstmt->us_max < pstmt->us_max)
				prstmt->us_max = pstmt->us_max;

			prstmt->us_total += pstmt->us_total;
			prstmt->rows += pstmt->rows;
			prstmt->exec_count += pstmt->exec_count;
		}
	}
	PG_CATCH();
	{
		if (unlock_mutex)
			SpinLockRelease(&first_chunk->mutex);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (unlock_mutex)
		SpinLockRelease(&first_chunk->mutex);

	if (shared_chunks)
		LWLockRelease(profiler_ss->lock);
}

/*
 * PLpgSQL statements has not unique id. We can assign some unique id
 * that can be used for statements counters. Fast access to this id
 * is implemented via map structure. It is a array of lists structure.
 */
static void
profiler_update_map(profiler_profile *profile, PLpgSQL_stmt *stmt)
{
	int		lineno = stmt->lineno;
	profiler_map_entry *pme;

	if (lineno > profile->stmts_map_max_lineno)
	{
		int		lines;
		int		i;

		/* calculate new size of map */
		for (lines = profile->stmts_map_max_lineno; stmt->lineno < lines;)
			if (lines < 10000)
				lines *= 2;
			else
				lines += 10000;

		profile->stmts_map = realloc(profile->stmts_map,
									 lines * sizeof(profiler_map_entry));

		for (i = profile->stmts_map_max_lineno; i < lines; i++)
			profile->stmts_map[i].stmt = NULL;

		profile->stmts_map_max_lineno = lines;
	}

	pme = &profile->stmts_map[lineno];

	if (!pme->stmt)
	{
		pme->stmt = stmt;
		pme->stmtid = profile->nstatements++;
	}
	else
	{
		profiler_map_entry *new_pme = palloc(sizeof(profiler_map_entry));

		new_pme->stmt = stmt;
		new_pme->stmtid = profile->nstatements++;
		new_pme->next = NULL;

		while (pme->next)
			pme = pme->next;

		pme->next = new_pme;
	}
}

/*
 * Returns statement id assigned to plpgsql statement. Should be
 * fast, because lineno is usually unique.
 */
static int
profiler_get_stmtid(profiler_profile *profile, PLpgSQL_stmt *stmt)
{
	int		lineno = stmt->lineno;
	profiler_map_entry *pme;

	if (lineno > profile->stmts_map_max_lineno)
		elog(ERROR, "broken statement map - too high lineno");

	pme = &profile->stmts_map[lineno];

	/* pme->stmt should not be null */
	if (!pme->stmt)
		elog(ERROR, "broken statement map - broken format");

	while (pme && pme->stmt != stmt)
		pme = pme->next;

	/* we should to find statement */
	if (!pme)
		elog(ERROR, "broken statement map - cannot to find statement");

	return pme->stmtid;
}

static void
profiler_touch_stmts(profiler_info *pinfo,
					 List *stmts,
					 bool generate_map,
					 bool finalize_profile,
					 int64 *nested_us_total)
{
	ListCell *lc;

	*nested_us_total = 0;

	foreach(lc, stmts)
	{
		int64		us_total = 0;

		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *) lfirst(lc);

		profiler_touch_stmt(pinfo,
							stmt,
							generate_map,
							finalize_profile,
							&us_total);

		if (finalize_profile)
			*nested_us_total += us_total;
	}
}



Datum
plpgsql_profiler_function_tb(PG_FUNCTION_ARGS)
{
	Oid			funcoid = PG_GETARG_OID(0);
	profiler_hashkey hk;
	HeapTuple	procTuple;
	bool found;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	TupleDesc	tupdesc;
	char	   *prosrc;
	Datum		prosrcdatum;
	bool		isnull;
	int			lineno = 1;
	int			current_statement = 0;
	profiler_stmt_chunk *chunk = NULL;
	profiler_stmt_chunk *first_chunk = NULL;
	HTAB	   *chunks;
	bool		shared_chunks;
	volatile bool		unlock_mutex = false;

	/* check to see if caller supports us returning a tuplestore */
	SetReturningFunctionCheck(rsinfo);

	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);

	prosrcdatum = SysCacheGetAttr(PROCOID, procTuple,
								  Anum_pg_proc_prosrc, &isnull);
	if (isnull)
		elog(ERROR, "null prosrc");

	prosrc = TextDatumGetCString(prosrcdatum);

	/* ensure correct complete content of hash key */
	memset(&hk, 0, sizeof(profiler_hashkey));
	hk.fn_oid = funcoid;
	hk.db_oid = MyDatabaseId;
	hk.fn_xmin = HeapTupleHeaderGetRawXmin(procTuple->t_data);
	hk.fn_tid =  procTuple->t_self;
	hk.chunk_num = 1;

	ReleaseSysCache(procTuple);

	if (shared_profiler_chunks_HashTable)
	{
		LWLockAcquire(profiler_ss->lock, LW_SHARED);
		chunks = shared_profiler_chunks_HashTable;
		shared_chunks = true;
	}
	else
	{
		chunks = profiler_chunks_HashTable;
		shared_chunks = false;
	}

	chunk = (profiler_stmt_chunk *) hash_search(chunks,
											 (void *) &hk,
											 HASH_FIND,
											 &found);



	PG_TRY();
	{
		if (shared_chunks && chunk)
		{
			first_chunk = chunk;
			SpinLockAcquire(&first_chunk->mutex);
			unlock_mutex = true;
		}

		while (*prosrc)
		{
			char	   *lineend = prosrc;
			char	   *linebeg = prosrc;
			int			stmt_lineno = -1;
			int64		us_total = 0;
			int64		exec_count = 0;
			Datum		max_time_array = (Datum) 0;
			Datum		processed_rows_array = (Datum) 0;
			int			cmds_on_row = 0;

			/* find lineend */
			while (*lineend != '\0' && *lineend != '\n')
				lineend += 1;

			if (*lineend == '\n')
			{
				*lineend = '\0';
				prosrc = lineend + 1;
			}
			else
				prosrc = lineend;

			if (chunk)
			{
				while (chunk->stmts[current_statement].lineno < lineno)
				{
					current_statement += 1;

					if (current_statement >= STATEMENTS_PER_CHUNK)
					{
						hk.chunk_num += 1;

						chunk = (profiler_stmt_chunk *) hash_search(chunks,
														 (void *) &hk,
														 HASH_FIND,
														 &found);

						if (!found)
						{
							chunk = NULL;
							break;
						}

						current_statement = 0;
					}
				}

				if (chunk && chunk->stmts[current_statement].lineno == lineno)
				{
					ArrayBuildState *max_time_abs = NULL;
					ArrayBuildState *processed_rows_abs = NULL;

#if PG_VERSION_NUM >= 90500

					max_time_abs = initArrayResult(FLOAT8OID, CurrentMemoryContext, true);
					processed_rows_abs = initArrayResult(INT8OID, CurrentMemoryContext, true);

#endif

					stmt_lineno = lineno;

					/* try to collect all statements on the line */
					while (chunk->stmts[current_statement].lineno == lineno)
					{
						profiler_stmt_reduced *prstmt;

						if (current_statement >= STATEMENTS_PER_CHUNK)
						{
							hk.chunk_num += 1;

							chunk = (profiler_stmt_chunk *) hash_search(chunks,
															 (void *) &hk,
															 HASH_FIND,
															 &found);

							if (!found)
							{
								chunk = NULL;
								break;
							}

							current_statement = 0;
						}

						if (!chunk)
							break;

						prstmt = &chunk->stmts[current_statement];

						us_total += prstmt->us_total;
						exec_count += prstmt->exec_count;

						cmds_on_row += 1;

						max_time_abs = accumArrayResult(max_time_abs,
														Float8GetDatum(prstmt->us_max / 1000.0), false,
														FLOAT8OID,
														CurrentMemoryContext);

						processed_rows_abs = accumArrayResult(processed_rows_abs,
															 Int64GetDatum(prstmt->rows), false,
															 INT8OID,
															 CurrentMemoryContext);

						current_statement += 1;
					}

					max_time_array = makeArrayResult(max_time_abs, CurrentMemoryContext);
					processed_rows_array = makeArrayResult(processed_rows_abs, CurrentMemoryContext);
				}
			}

			tuplestore_put_profile(tupstore, tupdesc,
								   lineno,
								   stmt_lineno,
								   cmds_on_row,
								   exec_count,
								   us_total,
								   max_time_array,
								   processed_rows_array,
								   linebeg);

			lineno += 1;
		}
	}
	PG_CATCH();
	{
		if (unlock_mutex)
			SpinLockRelease(&first_chunk->mutex);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (unlock_mutex)
		SpinLockRelease(&first_chunk->mutex);

	if (shared_chunks)
		LWLockRelease(profiler_ss->lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	return (Datum) 0;
}
