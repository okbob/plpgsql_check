/*-------------------------------------------------------------------------
 *
 * pldbgapi3
 *
 *			  enhanced debug API for plpgsql
 *
 * by Pavel Stehule 2013-2026
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
 * pldbgapi3 enhancing pl debug API and implementation allows to register
 * more plugins. Against previous implementation (pldbgapi2), it doesn't
 * use fmgr API. Cleaning statement stack after an exception is implemented
 * by usage memory context reset callback. The main advantage of this design
 * is possibility to access function's runtime data, that are accesible at this
 * time.
 *
 *
 */

#include "postgres.h"
#include "plpgsql.h"
#include "utils/palloc.h"

#include "plpgsql_check.h"


#define MAX_PLUGINS					10

typedef struct plpgsql_plugin_info
{
	int			magic;

	void	   *prev_plugin_info;

	/* for assertations */
	Oid			fn_oid;
	PLpgSQL_execstate *estate;
	int			use_count;

	plch_fextra *fextra;

	void	   *plugin_info[MAX_PLUGINS];
	bool		is_active[MAX_PLUGINS];

	/*
	 * This array holds a stack of opened statements. It is the base
	 * for calling stmt_abort callback.
	 */
	PLpgSQL_stmt **stmts_stack;
	int			stmts_stack_size;

	/*
	 * This array holds a statements, that should be removed
	 * from stmts_stack. It can be used inside a exception, and
	 * then can be dangerous to allocate memory. So this array
	 * is preallocated.
	 */
	PLpgSQL_stmt **stmts_buf;

	MemoryContextCallback er_mcb;
} plpgsql_plugin_info;

#define PLUGIN_INFO_MAGIC		2026010118

static plch_plugin *plugins[MAX_PLUGINS];
static int nplugins = 0;

static void func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);
static void stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

static PLpgSQL_plugin plpgsql_plugin = {
	func_setup,
	func_beg,
	func_end,
	stmt_beg,
	stmt_end,

#if PG_VERSION_NUM >= 150000

NULL, NULL, NULL, NULL, NULL

#else

NULL, NULL

#endif

};

static PLpgSQL_plugin *prev_plpgsql_plugin = NULL;

/*
 * For all statements from buffer for all plugins calls
 * stmt_abort callback. This can be used on stmts_stack
 * and then it is processed from top, or can be used stmts_buf,
 * and then it is processed from bottom. In this case, the
 * statement order is inverted when statements are copied from
 * stmts_stack to stmts_buf.
 */
static void
abort_statements(PLpgSQL_stmt **stmts, int nstmts,
				 plpgsql_plugin_info *plugin_info,
				 bool from_top)
{
	MemoryContext exec_mcxt = CurrentMemoryContext;
	int			i;
	int			j;

	for (i = 0; i < nplugins; i++)
	{
		if (plugin_info->is_active[i] && plugins[i]->stmt_abort)
		{
			plugin_info->estate->plugin_info = plugin_info->plugin_info[i];

			if (from_top)
			{
				for (j = nstmts - 1; j >= 0; j--)
				{
					MemoryContextSwitchTo(exec_mcxt);
					plugins[i]->stmt_abort(plugin_info->estate,
										   stmts[j],
										   plugin_info->fextra);
				}
			}
			else
			{
				for (j = 0; j < nstmts; j++)
				{
					MemoryContextSwitchTo(exec_mcxt);
					plugins[i]->stmt_abort(plugin_info->estate,
										   stmts[j],
										   plugin_info->fextra);
				}
			}
		}
	}
}

/*
 * This callback function is called when MemoryContext used for
 * function execution state is released. It is called after any
 * type of function ending - end or abort. We don't want to call
 * func_abort after correct ending. The flag is plugin_info->fextra.
 * When is released already, then function was correctly ended.
 */
static void
plugin_info_reset(void *arg)
{
	plpgsql_plugin_info *plugin_info = (plpgsql_plugin_info*) arg;
	MemoryContext exec_mcxt = CurrentMemoryContext;
	int			stmts_stack_size = plugin_info->stmts_stack_size;
	int			i;

	/*
	 * PostgreSQL 19 can remove this callback. But we need to support
	 * previous releases, so when fextra is already released, then
	 * do just nothing here.
	 */
	if (!plugin_info->fextra)
		return;

	plugin_info->stmts_stack_size = 0;

	PG_TRY();
	{
		abort_statements(plugin_info->stmts_stack,
						 stmts_stack_size,
						 plugin_info, true);

		for (i = 0; i < nplugins; i++)
		{
			if (plugin_info->is_active[i] && plugins[i]->func_abort)
			{
				plugin_info->estate->plugin_info = plugin_info->plugin_info[i];

				MemoryContextSwitchTo(exec_mcxt);
				plugins[i]->func_abort(plugin_info->estate,
									   plugin_info->estate->func,
									   plugin_info->fextra);
			}
		}
	}

	PG_CATCH();
	{
		plch_release_fextra(plugin_info->fextra);
		plugin_info->fextra = NULL;

		PG_RE_THROW();
	}
	PG_END_TRY();

	plch_release_fextra(plugin_info->fextra);
	plugin_info->fextra = NULL;
}

/*
 * for all active plugins and prev_plpgsql_plugins calls func_setup,
 * and when some plugin is active, prepares fextra.
 */
static void
func_setup(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	plpgsql_plugin_info *plugin_info =  palloc0(sizeof(plpgsql_plugin_info));
	MemoryContext setup_mcxt = CurrentMemoryContext;
	int			i;

	plugin_info->magic = PLUGIN_INFO_MAGIC;
	plugin_info->fn_oid = func->fn_oid;
	plugin_info->estate = estate;


#if PG_VERSION_NUM >= 180000

	plugin_info->use_count = func->cfunc.use_count;

#else

	plugin_info->use_count = func->use_count;

#endif

	for (i = 0; i < nplugins; i++)
	{
		if (plugins[i]->is_active(estate, func))
		{
			plugin_info->is_active[i] = true;

			if (!plugin_info->fextra)
				plugin_info->fextra = plch_get_fextra(func);

			plugin_info->stmts_stack = palloc((plugin_info->fextra->max_deep + 1) * sizeof(PLpgSQL_stmt *));
			plugin_info->stmts_buf = palloc((plugin_info->fextra->max_deep + 1) * sizeof(PLpgSQL_stmt *));
		}
		else
			plugin_info->is_active[i] = false;

		plugins[i]->error_callback = plpgsql_plugin.error_callback;
		plugins[i]->assign_expr = plpgsql_plugin.assign_expr;

#if PG_VERSION_NUM >= 150000

		plugins[i]->assign_value = plpgsql_plugin.assign_value;
		plugins[i]->eval_datum = plpgsql_plugin.eval_datum;
		plugins[i]->cast_value = plpgsql_plugin.cast_value;

#endif

	}

	if (plugin_info->fextra)
	{
		plugin_info->er_mcb.func = plugin_info_reset;
		plugin_info->er_mcb.arg = plugin_info;

		MemoryContextRegisterResetCallback(CurrentMemoryContext,
								   &plugin_info->er_mcb);
	}

	if (prev_plpgsql_plugin)
	{
		prev_plpgsql_plugin->error_callback = plpgsql_plugin.error_callback;
		prev_plpgsql_plugin->assign_expr = plpgsql_plugin.assign_expr;

#if PG_VERSION_NUM >= 150000

		prev_plpgsql_plugin->assign_value = plpgsql_plugin.assign_value;
		prev_plpgsql_plugin->eval_datum = plpgsql_plugin.eval_datum;
		prev_plpgsql_plugin->cast_value = plpgsql_plugin.cast_value;

#endif
	}

	/* try to call setup function for all plugins */
	PG_TRY();
	{
		for (i = 0; i < nplugins; i++)
		{
			if (plugin_info->is_active[i] && plugins[i]->func_setup)
			{
				estate->plugin_info = NULL;
				MemoryContextSwitchTo(setup_mcxt);
				plugins[i]->func_setup(estate, func, plugin_info->fextra);
				plugin_info->plugin_info[i] = estate->plugin_info;
				
			}
		}

		if (prev_plpgsql_plugin && prev_plpgsql_plugin->func_setup)
		{
			estate->plugin_info = NULL;
			MemoryContextSwitchTo(setup_mcxt);
			(prev_plpgsql_plugin->func_setup) (estate, func);
			plugin_info->prev_plugin_info = estate->plugin_info;
		}
	}
	PG_CATCH();
	{
		estate->plugin_info = plugin_info;

		PG_RE_THROW();
	}
	PG_END_TRY();

	estate->plugin_info = plugin_info;
}

/*
 * for all active plugins and prev_plpgsql_plugins calls func_beg
 */
static void
func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	plpgsql_plugin_info *plugin_info = estate->plugin_info;
	MemoryContext exec_mcxt = CurrentMemoryContext;
	int			i;

	if (!plugin_info || plugin_info->magic != PLUGIN_INFO_MAGIC)
		return;

	Assert(plugin_info->estate == estate);
	Assert(plugin_info->fn_oid == func->fn_oid);

#if PG_VERSION_NUM >= 180000

	Assert(plugin_info->use_count == func->cfunc.use_count);

#else

	Assert(plugin_info->use_count == func->use_count);

#endif

	PG_TRY();
	{
		if (plugin_info->fextra)
		{
			Assert(plugin_info->fextra->fn_oid == plugin_info->fn_oid);

			for (i = 0; i < nplugins; i++)
			{
				if (plugin_info->is_active[i] && plugins[i]->func_beg)
				{
					estate->plugin_info = plugin_info->plugin_info[i];
					plugins[i]->func_beg(estate, func, plugin_info->fextra);
					plugin_info->plugin_info[i] = estate->plugin_info;

					/* force original memory context */
					MemoryContextSwitchTo(exec_mcxt);
				}
			}
		}

		if (prev_plpgsql_plugin && prev_plpgsql_plugin->func_beg)
		{
			estate->plugin_info = plugin_info->prev_plugin_info;
			(prev_plpgsql_plugin->func_beg) (estate, func);
			plugin_info->prev_plugin_info = estate->plugin_info;
		}
	}
	PG_CATCH();
	{
		estate->plugin_info = plugin_info;

		PG_RE_THROW();
	}
	PG_END_TRY();

	estate->plugin_info = plugin_info;
}

/*
 * for all active plugins and prev_plpgsql_plugins calls func_end
 */
static void
func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	plpgsql_plugin_info *plugin_info = estate->plugin_info;
	MemoryContext exec_mcxt = CurrentMemoryContext;
	int			i;

	if (!plugin_info || plugin_info->magic != PLUGIN_INFO_MAGIC)
		return;

	Assert(plugin_info->estate == estate);
	Assert(plugin_info->fn_oid == func->fn_oid);

#if PG_VERSION_NUM >= 180000

	Assert(plugin_info->use_count == func->cfunc.use_count);

#else

	Assert(plugin_info->use_count == func->use_count);

#endif

	PG_TRY();
	{
		if (plugin_info->fextra)
		{
			int naborted_stmts = plugin_info->stmts_stack_size;

			Assert(plugin_info->fextra->fn_oid == plugin_info->fn_oid);

			plugin_info->stmts_stack_size = 0;

			abort_statements(plugin_info->stmts_stack,
							 naborted_stmts,
							 plugin_info, true);

			for (i = 0; i < nplugins; i++)
			{
				if (plugin_info->is_active[i] && plugins[i]->func_end)
				{
					estate->plugin_info = plugin_info->plugin_info[i];
					MemoryContextSwitchTo(exec_mcxt);
					plugins[i]->func_end(estate, func, plugin_info->fextra);
					plugin_info->plugin_info[i] = estate->plugin_info;
				}
			}
		}

		if (prev_plpgsql_plugin && prev_plpgsql_plugin->func_end)
		{
			estate->plugin_info = plugin_info->prev_plugin_info;
			MemoryContextSwitchTo(exec_mcxt);
			(prev_plpgsql_plugin->func_end) (estate, func);
			plugin_info->prev_plugin_info = estate->plugin_info;
		}
	}
	PG_CATCH();
	{
		estate->plugin_info = plugin_info;

		if (plugin_info->fextra)
		{
			plch_release_fextra(plugin_info->fextra);
			plugin_info->fextra = NULL;
		}

		PG_RE_THROW();
	}
	PG_END_TRY();

	estate->plugin_info = plugin_info;

	if (plugin_info->fextra)
	{
		plch_release_fextra(plugin_info->fextra);
		plugin_info->fextra = NULL;
	}
}

/*
 * for all active plugins and prev_plpgsql_plugins calls stmt_beg
 */
static void
stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	plpgsql_plugin_info *plugin_info = estate->plugin_info;
	MemoryContext exec_mcxt = CurrentMemoryContext;
	int			naborted_stmts = 0;
	int			i;

	if (!plugin_info || plugin_info->magic != PLUGIN_INFO_MAGIC)
		return;

	Assert(plugin_info->estate == estate);
	Assert(plugin_info->fn_oid == estate->func->fn_oid);

#if PG_VERSION_NUM >= 180000

	Assert(plugin_info->use_count == estate->func->cfunc.use_count);

#else

	Assert(plugin_info->use_count == estate->func->use_count);

#endif

	if (plugin_info->fextra)
	{
		if (estate->cur_error)
		{
			/*
			 * Only inside error handler we need reduce statements
			 * from stacks, because stmt_end was skipped due some
			 * exception. All statements until parent of current
			 * statements should be closed.
			 */
			int		cur_parentid = plugin_info->fextra->parentids[stmt->stmtid];

			while (plugin_info->stmts_stack_size > 0 &&
				   plugin_info->stmts_stack[plugin_info->stmts_stack_size - 1]->stmtid != cur_parentid)
			{
				plugin_info->stmts_buf[naborted_stmts++] = plugin_info->stmts_stack[plugin_info->stmts_stack_size - 1];
				plugin_info->stmts_stack_size--;
			}
		}

		plugin_info->stmts_stack[plugin_info->stmts_stack_size++] = stmt;
	}

	PG_TRY();
	{
		if (plugin_info->fextra)
		{
			Assert(plugin_info->fextra->fn_oid == plugin_info->fn_oid);

			abort_statements(plugin_info->stmts_buf,
							 naborted_stmts,
							 plugin_info, false);

			for (i = 0; i < nplugins; i++)
			{
				if (plugin_info->is_active[i] && plugins[i]->stmt_beg)
				{
					estate->plugin_info = plugin_info->plugin_info[i];
					MemoryContextSwitchTo(exec_mcxt);
					plugins[i]->stmt_beg(estate, stmt, plugin_info->fextra);
					plugin_info->plugin_info[i] = estate->plugin_info;
				}
			}
		}

		if (prev_plpgsql_plugin && prev_plpgsql_plugin->stmt_beg)
		{
			estate->plugin_info = plugin_info->prev_plugin_info;
			MemoryContextSwitchTo(exec_mcxt);
			(prev_plpgsql_plugin->stmt_beg) (estate, stmt);
			plugin_info->prev_plugin_info = estate->plugin_info;
		}
	}
	PG_CATCH();
	{
		estate->plugin_info = plugin_info;

		PG_RE_THROW();
	}
	PG_END_TRY();

	estate->plugin_info = plugin_info;
}

/*
 * for all active plugins and prev_plpgsql_plugins calls stmt_end
 */
static void
stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	plpgsql_plugin_info *plugin_info = estate->plugin_info;
	MemoryContext exec_mcxt = CurrentMemoryContext;
	int			naborted_stmts = 0;
	int			i;

	if (!plugin_info || plugin_info->magic != PLUGIN_INFO_MAGIC)
		return;

	Assert(plugin_info->estate == estate);
	Assert(plugin_info->fn_oid == estate->func->fn_oid);

#if PG_VERSION_NUM >= 180000

	Assert(plugin_info->use_count == estate->func->cfunc.use_count);

#else

	Assert(plugin_info->use_count == estate->func->use_count);

#endif

	if (plugin_info->fextra)
	{
		Assert(plugin_info->stmts_stack_size > 0);

		/*
		 * After NULL exception handler, we need to close statements in stmt_end
		 */
		while (plugin_info->stmts_stack_size > 0 &&
			   stmt->stmtid != plugin_info->stmts_stack[plugin_info->stmts_stack_size - 1]->stmtid)
		{
			plugin_info->stmts_buf[naborted_stmts++] = plugin_info->stmts_stack[plugin_info->stmts_stack_size - 1];
			plugin_info->stmts_stack_size--;
		}

		Assert(plugin_info->stmts_stack_size > 0);
		Assert(plugin_info->stmts_stack[plugin_info->stmts_stack_size - 1]->stmtid == stmt->stmtid);

		plugin_info->stmts_stack_size--;
	}

	PG_TRY();
	{
		if (plugin_info->fextra)
		{
			Assert(plugin_info->fextra->fn_oid == plugin_info->fn_oid);

			abort_statements(plugin_info->stmts_buf,
							 naborted_stmts,
							 plugin_info, false);

			for (i = 0; i < nplugins; i++)
			{
				if (plugin_info->is_active[i] && plugins[i]->stmt_end)
				{
					estate->plugin_info = plugin_info->plugin_info[i];
					MemoryContextSwitchTo(exec_mcxt);
					plugins[i]->stmt_end(estate, stmt, plugin_info->fextra);
					plugin_info->plugin_info[i] = estate->plugin_info;
				}
			}
		}

		if (prev_plpgsql_plugin && prev_plpgsql_plugin->stmt_end)
		{
			estate->plugin_info = plugin_info->prev_plugin_info;
			MemoryContextSwitchTo(exec_mcxt);
			(prev_plpgsql_plugin->stmt_end) (estate, stmt);
			plugin_info->prev_plugin_info = estate->plugin_info;
		}
	}
	PG_CATCH();
	{
		estate->plugin_info = plugin_info;

		PG_RE_THROW();
	}
	PG_END_TRY();

	estate->plugin_info = plugin_info;
}

void
plch_register_plugin(plch_plugin *plugin)
{
	if (nplugins < MAX_PLUGINS)
		plugins[nplugins++] = plugin;
	else
		elog(ERROR, "too much plpgsql_check pl debug API plugins");
}

void
plch_init_plugin(void)
{
	PLpgSQL_plugin **plugin_ptr;
	static bool inited = false;

	if (inited)
		return;

	plugin_ptr = (PLpgSQL_plugin **) find_rendezvous_variable("PLpgSQL_plugin");
	prev_plpgsql_plugin = *plugin_ptr;
	*plugin_ptr = &plpgsql_plugin;

	inited = true;
}

#if PG_VERSION_NUM < 150000

void
plch_finish_plugin(void)
{
	PLpgSQL_plugin **plugin_ptr;

	plugin_ptr = (PLpgSQL_plugin **) find_rendezvous_variable("PLpgSQL_plugin");
	*plugin_ptr = prev_plpgsql_plugin;
}

#endif
