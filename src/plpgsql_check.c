/*-------------------------------------------------------------------------
 *
 * plpgsql_check.c
 *
 *			  enhanced checks for plpgsql functions
 *
 * by Pavel Stehule 2013-2021
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

PLpgSQL_plugin **plpgsql_check_plugin_var_ptr;

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

static const struct config_enum_entry tracer_verbosity_options[] = {
	{"terse", PGERROR_TERSE, false},
	{"default", PGERROR_DEFAULT, false},
	{"verbose", PGERROR_VERBOSE, false},
	{NULL, 0, false}
};

static const struct config_enum_entry tracer_level_options[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"log", LOG, false},
	{NULL, 0, false}
};

void			_PG_init(void);
void			_PG_fini(void);

shmem_startup_hook_type prev_shmem_startup_hook = NULL;

bool plpgsql_check_regress_test_mode;

/*
 * Links to function in plpgsql module
 */
plpgsql_check__build_datatype_t plpgsql_check__build_datatype_p;
plpgsql_check__compile_t plpgsql_check__compile_p;
plpgsql_check__parser_setup_t plpgsql_check__parser_setup_p;
plpgsql_check__stmt_typename_t plpgsql_check__stmt_typename_p;
plpgsql_check__exec_get_datum_type_t plpgsql_check__exec_get_datum_type_p;
plpgsql_check__recognize_err_condition_t plpgsql_check__recognize_err_condition_p;
plpgsql_check__ns_lookup_t plpgsql_check__ns_lookup_p;


/*
 * load_external_function retursn PGFunctions - we need generic function, so
 * it is not 100% correct, but in used context it is not a problem.
 */
#define LOAD_EXTERNAL_FUNCTION(file, funcname)	((void *) (load_external_function(file, funcname, true, NULL)))

/*
 * Module initialization
 *
 * join to PLpgSQL executor
 *
 */
void 
_PG_init(void)
{

	/* Be sure we do initialization only once (should be redundant now) */
	static bool inited = false;

	if (inited)
		return;

	pg_bindtextdomain(TEXTDOMAIN);

	AssertVariableIsOfType(&plpgsql_build_datatype, plpgsql_check__build_datatype_t);
	plpgsql_check__build_datatype_p = (plpgsql_check__build_datatype_t)
		LOAD_EXTERNAL_FUNCTION("$libdir/plpgsql", "plpgsql_build_datatype");

	AssertVariableIsOfType(&plpgsql_compile, plpgsql_check__compile_t);
	plpgsql_check__compile_p = (plpgsql_check__compile_t)
		LOAD_EXTERNAL_FUNCTION("$libdir/plpgsql", "plpgsql_compile");

	AssertVariableIsOfType(&plpgsql_parser_setup, plpgsql_check__parser_setup_t);
	plpgsql_check__parser_setup_p = (plpgsql_check__parser_setup_t)
		LOAD_EXTERNAL_FUNCTION("$libdir/plpgsql", "plpgsql_parser_setup");

	AssertVariableIsOfType(&plpgsql_stmt_typename, plpgsql_check__stmt_typename_t);
	plpgsql_check__stmt_typename_p = (plpgsql_check__stmt_typename_t)
		LOAD_EXTERNAL_FUNCTION("$libdir/plpgsql", "plpgsql_stmt_typename");

	AssertVariableIsOfType(&plpgsql_exec_get_datum_type, plpgsql_check__exec_get_datum_type_t);
	plpgsql_check__exec_get_datum_type_p = (plpgsql_check__exec_get_datum_type_t)
		LOAD_EXTERNAL_FUNCTION("$libdir/plpgsql", "plpgsql_exec_get_datum_type");

	AssertVariableIsOfType(&plpgsql_recognize_err_condition, plpgsql_check__recognize_err_condition_t);
	plpgsql_check__recognize_err_condition_p = (plpgsql_check__recognize_err_condition_t)
		LOAD_EXTERNAL_FUNCTION("$libdir/plpgsql", "plpgsql_recognize_err_condition");

	AssertVariableIsOfType(&plpgsql_ns_lookup, plpgsql_check__ns_lookup_t);
	plpgsql_check__ns_lookup_p = (plpgsql_check__ns_lookup_t)
		LOAD_EXTERNAL_FUNCTION("$libdir/plpgsql", "plpgsql_ns_lookup");

	plpgsql_check_plugin_var_ptr = (PLpgSQL_plugin **) find_rendezvous_variable( "PLpgSQL_plugin");
	*plpgsql_check_plugin_var_ptr = &plugin_funcs;

	DefineCustomBoolVariable("plpgsql_check.regress_test_mode",
					    "reduces volatile output",
					    NULL,
					    &plpgsql_check_regress_test_mode,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

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

	DefineCustomBoolVariable("plpgsql_check.enable_tracer",
					    "when is true, then tracer's functionality is enabled",
					    NULL,
					    &plpgsql_check_enable_tracer,
					    false,
					    PGC_SUSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.tracer",
					    "when is true, then function is traced",
					    NULL,
					    &plpgsql_check_tracer,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.trace_assert",
					    "when is true, then statement ASSERT is traced",
					    NULL,
					    &plpgsql_check_trace_assert,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomBoolVariable("plpgsql_check.tracer_test_mode",
					    "when is true, then output of tracer is in regress test possible format",
					    NULL,
					    &plpgsql_check_tracer_test_mode,
					    false,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomEnumVariable("plpgsql_check.tracer_verbosity",
					    "sets the verbosity of tracer",
					    NULL,
					    (int *) &plpgsql_check_tracer_verbosity,
					    PGERROR_DEFAULT,
					    tracer_verbosity_options,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomEnumVariable("plpgsql_check.trace_assert_verbosity",
					    "sets the verbosity of trace ASSERT statement",
					    NULL,
					    (int *) &plpgsql_check_trace_assert_verbosity,
					    PGERROR_DEFAULT,
					    tracer_verbosity_options,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomEnumVariable("plpgsql_check.tracer_errlevel",
					    "sets an error level of tracer's messages",
					    NULL,
					    (int *) &plpgsql_check_tracer_errlevel,
					    NOTICE,
					    tracer_level_options,
					    PGC_USERSET, 0,
					    NULL, NULL, NULL);

	DefineCustomIntVariable("plpgsql_check.tracer_variable_max_length",
							"Maximum output length of content of variables in bytes",
							NULL,
							&plpgsql_check_tracer_variable_max_length,
							1024,
							10, 2048,
							PGC_USERSET, 0,
							NULL, NULL, NULL);

	EmitWarningsOnPlaceholders("plpgsql_check");

	plpgsql_check_HashTableInit();
	plpgsql_check_profiler_init_hash_tables();

	/* Use shared memory when we can register more for self */
	if (process_shared_preload_libraries_in_progress)
	{

		DefineCustomIntVariable("plpgsql_check.profiler_max_shared_chunks",
						    "maximum numbers of statements chunks in shared memory",
						    NULL,
						    &plpgsql_check_profiler_max_shared_chunks,
						    15000, 50, 100000,
						    PGC_POSTMASTER, 0,
						    NULL, NULL, NULL);

		RequestAddinShmemSpace(plpgsql_check_shmem_size());

		RequestNamedLWLockTranche("plpgsql_check profiler", 1);
		RequestNamedLWLockTranche("plpgsql_check fstats", 1);

		/*
		 * Install hooks.
		 */
		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = plpgsql_check_profiler_shmem_startup;
	}

	plpgsql_check_next_needs_fmgr_hook = needs_fmgr_hook;
	plpgsql_check_next_fmgr_hook = fmgr_hook;

	needs_fmgr_hook = plpgsql_check_needs_fmgr_hook;
	fmgr_hook = plpgsql_check_fmgr_hook;

	inited = true;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;

	/* Be more correct, and clean rendezvous variable */
	*plpgsql_check_plugin_var_ptr = NULL;

	needs_fmgr_hook = plpgsql_check_next_needs_fmgr_hook;
	fmgr_hook = plpgsql_check_next_fmgr_hook;
}
