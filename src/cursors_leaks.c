/*-------------------------------------------------------------------------
 *
 * cursors_leak.c
 *
 *			  detection unclosed cursors code
 *
 * by Pavel Stehule 2013-2025
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#if PG_VERSION_NUM >= 180000

#include "utils/funccache.h"

#endif

bool		plpgsql_check_cursors_leaks = true;
bool		plpgsql_check_cursors_leaks_strict = false;
int			plpgsql_check_cursors_leaks_level = WARNING;


#define MAX_NAMES_PER_STATEMENT			20

typedef struct CursorTrace
{
	int			stmtid;
	int			rec_level;
	char	   *curname;
} CursorTrace;

typedef struct FunctionTraceKey
{
	Oid			fn_oid;
	TransactionId fn_xmin;
} FunctionTraceKey;

typedef struct FunctionTrace
{
	FunctionTraceKey key;

	int			ncursors;
	int			cursors_size;
	CursorTrace *cursors_traces;
} FunctionTrace;

typedef struct CursorLeaksPlugin2Info
{
	FunctionTrace *ftrace;
	LocalTransactionId lxid;
} CursorLeaksPlugin2Info;

static LocalTransactionId traces_lxid = InvalidLocalTransactionId;
static HTAB *traces = NULL;
static MemoryContext traces_mcxt = NULL;

static void func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info);
static void func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info);
static void stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, void **plugin2_info);

static plpgsql_check_plugin2 cursors_leaks_plugin2 =
{
	func_setup, NULL, func_end, NULL,
		NULL, stmt_end, NULL, NULL, NULL, NULL, NULL, NULL
};

#if PG_VERSION_NUM >= 170000

#define CURRENT_LXID	(MyProc->vxid.lxid)

#else

#define CURRENT_LXID	(MyProc->lxid)

#endif

static FunctionTrace *
get_function_trace(PLpgSQL_function *func)
{
	bool		found;
	FunctionTrace *ftrace;
	FunctionTraceKey key;

	if (traces == NULL || traces_lxid != CURRENT_LXID)
	{
		HASHCTL		ctl;

		traces_mcxt = AllocSetContextCreate(TopTransactionContext,
											"plpgsql_check - trace cursors",
											ALLOCSET_DEFAULT_SIZES);

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(FunctionTraceKey);
		ctl.entrysize = sizeof(FunctionTrace);
		ctl.hcxt = traces_mcxt;

		traces = hash_create("plpgsql_checj - cursors leaks detection",
							 FUNCS_PER_USER,
							 &ctl,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		traces_lxid = CURRENT_LXID;
	}

	key.fn_oid = func->fn_oid;

#if PG_VERSION_NUM >= 180000

	key.fn_xmin = func->cfunc.fn_xmin;

#else

	key.fn_xmin = func->fn_xmin;

#endif

	ftrace = (FunctionTrace *) hash_search(traces,
										   (void *) &key,
										   HASH_ENTER,
										   &found);

	if (!found)
	{
		ftrace->key.fn_oid = func->fn_oid;

#if PG_VERSION_NUM >= 180000

		ftrace->key.fn_xmin = func->cfunc.fn_xmin;

#else

		ftrace->key.fn_xmin = func->fn_xmin;

#endif

		ftrace->ncursors = 0;
		ftrace->cursors_size = 0;
		ftrace->cursors_traces = NULL;
	}

	return ftrace;
}


static void
func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func, void **plugin2_info)
{
	if (plpgsql_check_cursors_leaks)
	{
		CursorLeaksPlugin2Info *pinfo;
		MemoryContext fn_mcxt;

		fn_mcxt = plpgsql_check_get_current_fn_mcxt();
		pinfo = MemoryContextAlloc(fn_mcxt, sizeof(CursorLeaksPlugin2Info));

		pinfo->ftrace = get_function_trace(func);
		pinfo->lxid = CURRENT_LXID;

		*plugin2_info = pinfo;
	}
	else
		*plugin2_info = NULL;
}

static void
func_end(PLpgSQL_execstate *estate,
		 PLpgSQL_function *func,
		 void **plugin2_info)
{
	CursorLeaksPlugin2Info *pinfo = *plugin2_info;
	FunctionTrace *ftrace;
	int			i;

	if (!pinfo || pinfo->lxid != CURRENT_LXID)
		return;

	ftrace = pinfo->ftrace;

	for (i = 0; i < ftrace->ncursors; i++)
	{
		CursorTrace *ct = &ftrace->cursors_traces[i];

		/*
		 * Iterate over traced cursors. Remove slots for tracing immediately,
		 * when traced cursor is closed already.
		 */
#if PG_VERSION_NUM >= 180000

		if (ct->curname && ct->rec_level == func->cfunc.use_count)

#else

		if (ct->curname && ct->rec_level == func->use_count)

#endif

		{
			if (SPI_cursor_find(ct->curname))
			{
				if (plpgsql_check_cursors_leaks_strict)
				{
					char	   *context;

					context = GetErrorContextStack();

					ereport(plpgsql_check_cursors_leaks_level,
							errcode(ERRCODE_INVALID_CURSOR_STATE),
							errmsg("cursor is not closed"),
							errdetail("%s", context));
					pfree(context);

					pfree(ct->curname);
					ct->stmtid = -1;
					ct->curname = NULL;
				}
			}
			else
			{
				pfree(ct->curname);
				ct->stmtid = -1;
				ct->curname = NULL;
			}
		}
	}
}

static void
stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, void **plugin2_info)
{
	CursorLeaksPlugin2Info *pinfo = *plugin2_info;
	FunctionTrace *ftrace;

	if (!pinfo)
		return;

	if (traces_lxid != CURRENT_LXID ||
		pinfo->lxid != CURRENT_LXID)
	{
		pinfo->ftrace = get_function_trace(estate->func);
		pinfo->lxid = CURRENT_LXID;
	}

	ftrace = pinfo->ftrace;

	if (stmt->cmd_type == PLPGSQL_STMT_OPEN)
	{
		int			i;
		int			cursors_for_current_stmt = 0;
		int			free_slot = -1;
		PLpgSQL_var *curvar;
		char	   *curname;

		curvar = (PLpgSQL_var *) (estate->datums[((PLpgSQL_stmt_open *) stmt)->curvar]);

		Assert(!curvar->isnull);
		curname = TextDatumGetCString(curvar->value);

		for (i = 0; i < ftrace->ncursors; i++)
		{
			CursorTrace *ct = &ftrace->cursors_traces[i];

			if (ct->curname && ct->stmtid == stmt->stmtid)
			{
				/*
				 * PLpgSQL open statements reuses portal name and does check
				 * already used portal with already used portal name. So when
				 * the traced name and name in cursor variable is same, we
				 * should not to do this check. This eliminate false alarms.
				 */
				if (strcmp(curname, ct->curname) == 0)
				{
					pfree(curname);
					return;
				}

				if (SPI_cursor_find(ct->curname))
				{
#if PG_VERSION_NUM >= 180000

					if (estate->func->cfunc.use_count == 1 && !plpgsql_check_cursors_leaks_strict)

#else

					if (estate->func->use_count == 1 && !plpgsql_check_cursors_leaks_strict)

#endif

					{
						char	   *context;

						context = GetErrorContextStack();

						ereport(plpgsql_check_cursors_leaks_level,
								errcode(ERRCODE_INVALID_CURSOR_STATE),
								errmsg("cursor \"%s\" is not closed", curvar->refname),
								errdetail("%s", context));

						pfree(context);

						pfree(ct->curname);
						ct->stmtid = -1;
						ct->curname = NULL;
					}
					else
					{
						cursors_for_current_stmt += 1;
					}
				}
				else
				{
					pfree(ct->curname);
					ct->stmtid = -1;
					ct->curname = NULL;
				}
			}

			if (ct->stmtid == -1 && free_slot == -1)
				free_slot = i;
		}

		if (cursors_for_current_stmt < MAX_NAMES_PER_STATEMENT)
		{
			MemoryContext oldcxt;
			CursorTrace *ct = NULL;

			oldcxt = MemoryContextSwitchTo(traces_mcxt);

			if (free_slot != -1)
				ct = &ftrace->cursors_traces[free_slot];
			else
			{
				if (ftrace->ncursors == ftrace->cursors_size)
				{
					if (ftrace->cursors_size > 0)
					{
						ftrace->cursors_size += 10;
						ftrace->cursors_traces = repalloc_array(ftrace->cursors_traces,
																CursorTrace,
																ftrace->cursors_size);
					}
					else
					{
						ftrace->cursors_size = 10;
						ftrace->cursors_traces = palloc_array(CursorTrace,
															  ftrace->cursors_size);
					}
				}

				ct = &ftrace->cursors_traces[ftrace->ncursors++];
			}

			ct->stmtid = stmt->stmtid;

#if PG_VERSION_NUM >= 180000

			ct->rec_level = estate->func->cfunc.use_count;

#else

			ct->rec_level = estate->func->use_count;

#endif

			ct->curname = pstrdup(curname);

			MemoryContextSwitchTo(oldcxt);
		}

		pfree(curname);
	}
}

void
plpgsql_check_cursors_leaks_init(void)
{
	plpgsql_check_register_pldbgapi2_plugin(&cursors_leaks_plugin2);
}
