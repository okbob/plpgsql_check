project('plpgsql_check', ['c'])

pg_config = find_program('pg_config')

bindir = run_command(pg_config, '--bindir', check: true).stdout().strip()
includedir_server = run_command(pg_config, '--includedir-server', check: true).stdout().strip()
pkglibdir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
sharedir = run_command(pg_config, '--sharedir', check: true).stdout().strip()

plpgsql_check_sources = files(
  'src/assign.c',
  'src/cursors_leaks.c',
  'src/format.c',
  'src/check_function.c',
  'src/pldbgapi2.c',
  'src/pragma.c',
  'src/report.c',
  'src/tablefunc.c',
  'src/typdesc.c',
  'src/catalog.c',
  'src/expr_walk.c',
  'src/check_expr.c',
  'src/parser.c',
  'src/plpgsql_check.c',
  'src/profiler.c',
  'src/stmtwalk.c',
  'src/tracer.c',
)

shared_module('plpgsql_check', plpgsql_check_sources,
              include_directories: [includedir_server],
              install: true,
              install_dir: pkglibdir,
              name_prefix: '',
             )

install_data('plpgsql_check.control',
             'plpgsql_check--2.7.sql',
             install_dir: sharedir / 'extension',
            )

pg_regress = find_program('pg_regress',
                          dirs: [pkglibdir / 'pgxs/src/test/regress']
                         )

regress_tests = ['plpgsql_check_active', 'plpgsql_check_passive']

test('regress',
     pg_regress,
     args: ['--bindir', bindir,
            '--inputdir', meson.current_source_dir(),
           ] + regress_tests,
    )

