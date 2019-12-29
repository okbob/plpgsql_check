/*-------------------------------------------------------------------------
 *
 * plpgsql_check.c
 *
 *			  enhanced checks for plpgsql functions
 *
 * by Pavel Stehule 2013-2018
 *
 *-------------------------------------------------------------------------
 *
 * Notes:
 *
 * 1) Secondary hash table for function signature is necessary due holding is_checked
 *    attribute - this protection against unwanted repeated check.
 *
 * 2) Reusing some plpgsql_xxx functions requires full run-time environment. It is
 *    emulated by fake expression context and fake fceinfo (these are created when
 *    active checking is used) - see: setup_fake_fcinfo, setup_cstate.
 *
 * 3) The environment is referenced by stored execution plans. The actual plan should
 *    not be linked with fake environment. All expressions created in checking time
 *    should be relased by release_exprs(cstate.exprs) function.
 *
 */

#include "plpgsql_check.h"
#include "plpgsql_check_builtins.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static PLpgSQL_plugin plugin_funcs = { plpgsql_check_profiler_func_init,
									   plpgsql_check_on_func_beg,
									   plpgsql_check_profiler_func_end,
									   plpgsql_check_profiler_stmt_beg,
									   plpgsql_check_profiler_stmt_end,
									   NULL,
									   NULL};



static const struct config_enum_entry plpgsql_check_mode_options[] = {
	{"disabled", PLPGSQL_CHECK_MODE_DISABLED, false},
	{"by_function", PLPGSQL_CHECK_MODE_BY_FUNCTION, false},
	{"fresh_start", PLPGSQL_CHECK_MODE_FRESH_START, false},
	{"every_start", PLPGSQL_CHECK_MODE_EVERY_START, false},
	{NULL, 0, false}
};

void			_PG_init(void);
void			_PG_fini(void);

shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * Linkage to function in plpgsql module
 */
plpgsql_check__build_datatype_t plpgsql_check__build_datatype_p;
plpgsql_check__compile_t plpgsql_check__compile_p;
plpgsql_check__parser_setup_t plpgsql_check__parser_setup_p;
plpgsql_check__stmt_typename_t plpgsql_check__stmt_typename_p;
plpgsql_check__exec_get_datum_type_t plpgsql_check__exec_get_datum_type_p;
plpgsql_check__recognize_err_condition_t plpgsql_check__recognize_err_condition_p;

/*
 * Module initialization
 *
 * join to PLpgSQL executor
 *
 */
void 
_PG_init(void)
{
	PLpgSQL_plugin **var_ptr;

	/* Be sure we do initialization only once (should be redundant now) */
	static bool inited = false;

	if (inited)
		return;

	AssertVariableIsOfType(&plpgsql_build_datatype, plpgsql_check__build_datatype_t);
	plpgsql_check__build_datatype_p = (plpgsql_check__build_datatype_t)
		load_external_function("$libdir/plpgsql", "plpgsql_build_datatype", true, NULL);

	AssertVariableIsOfType(&plpgsql_compile, plpgsql_check__compile_t);
	plpgsql_check__compile_p = (plpgsql_check__compile_t)
		load_external_function("$libdir/plpgsql", "plpgsql_compile", true, NULL);

	AssertVariableIsOfType(&plpgsql_parser_setup, plpgsql_check__parser_setup_t);
	plpgsql_check__parser_setup_p = (plpgsql_check__parser_setup_t)
		load_external_function("$libdir/plpgsql", "plpgsql_parser_setup", true, NULL);

	AssertVariableIsOfType(&plpgsql_stmt_typename, plpgsql_check__stmt_typename_t);
	plpgsql_check__stmt_typename_p = (plpgsql_check__stmt_typename_t)
		load_external_function("$libdir/plpgsql", "plpgsql_stmt_typename", true, NULL);

#if PG_VERSION_NUM >= 90500

	AssertVariableIsOfType(&plpgsql_exec_get_datum_type, plpgsql_check__exec_get_datum_type_t);
	plpgsql_check__exec_get_datum_type_p = (plpgsql_check__exec_get_datum_type_t)
		load_external_function("$libdir/plpgsql", "plpgsql_exec_get_datum_type", true, NULL);

#else

	AssertVariableIsOfType(&exec_get_datum_type, plpgsql_check__exec_get_datum_type_t);
	plpgsql_check__exec_get_datum_type_p = (plpgsql_check__exec_get_datum_type_t)
		load_external_function("$libdir/plpgsql", "exec_get_datum_type", true, NULL);

#endif

	AssertVariableIsOfType(&plpgsql_recognize_err_condition, plpgsql_check__recognize_err_condition_t);
	plpgsql_check__recognize_err_condition_p = (plpgsql_check__recognize_err_condition_t)
		load_external_function("$libdir/plpgsql", "plpgsql_recognize_err_condition", true, NULL);

	var_ptr = (PLpgSQL_plugin **) find_rendezvous_variable( "PLpgSQL_plugin" );
	*var_ptr = &plugin_funcs;

	DefineCustomEnumVariable("plpgsql_check.mode",
					    "choose a mode for enhanced checking",
					    NULL,
					    &plpgsql_check_mode,
					    PLPGSQL_CHECK_MODE_BY_FUNCTION,
					    plpgsql_check_mode_options,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.show_nonperformance_extra_warnings",
					    "when is true, then extra warning (except performance warnings) are showed",
					    NULL,
					    &plpgsql_check_extra_warnings,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.show_nonperformance_warnings",
					    "when is true, then warning (except performance warnings) are showed",
					    NULL,
					    &plpgsql_check_other_warnings,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.show_performance_warnings",
					    "when is true, then performance warnings are showed",
					    NULL,
					    &plpgsql_check_performance_warnings,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.fatal_errors",
					    "when is true, then plpgsql check stops execution on detected error",
					    NULL,
					    &plpgsql_check_fatal_errors,
					    true,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.profiler",
					    "when is true, then function execution profile is updated",
					    NULL,
					    &plpgsql_check_profiler,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	plpgsql_check_HashTableInit();
	plpgsql_check_profiler_init_hash_tables();

	/* Use shared memory when we can register more for self */
	if (process_shared_preload_libraries_in_progress)
	{

		RequestAddinShmemSpace(plpgsql_check_shmem_size());

#if PG_VERSION_NUM >= 90600

		RequestNamedLWLockTranche("plpgsql_check profiler", 1);

#else

		RequestAddinLWLocks(1);

#endif

		/*
		 * Install hooks.
		 */
		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = plpgsql_check_profiler_shmem_startup;
	}

	inited = true;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;
}

