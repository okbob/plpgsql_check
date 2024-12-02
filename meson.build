project('plpgsql_check', ['c'], version: '2.7')

pg_config = find_program('pg_config')

bindir = run_command(pg_config, '--bindir', check: true).stdout().strip()
includedir_server = run_command(pg_config, '--includedir-server', check: true).stdout().strip()
includedir = run_command(pg_config, '--includedir', check: true).stdout().strip()
pkglibdir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
sharedir = run_command(pg_config, '--sharedir', check: true).stdout().strip()
libdir = run_command(pg_config, '--libdir', check: true).stdout().strip()

versionstring = run_command(pg_config, '--version', check: true).stdout().strip()
pg_version = versionstring.split(' ')[1]
plpgsql_check_version = meson.project_version()

if pg_version.endswith('devel')
  pg_version_arr = [pg_version.split('devel')[0], '0']
elif pg_version.contains('beta')
  pg_version_arr = [pg_version.split('beta')[0], '0']
elif pg_version.contains('rc')
  pg_version_arr = [pg_version.split('rc')[0], '0']
else
  pg_version_arr = pg_version.split('.')
endif

pg_version_major = pg_version_arr[0].to_int()
pg_version_minor = pg_version_arr[1].to_int()
pg_version_num = (pg_version_major * 10000) + pg_version_minor

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

compilerName = meson.get_compiler('c').get_id()

if meson.get_compiler('c').get_id() == 'msvc'
  incdir = [includedir_server / 'port/win32_msvc',
            includedir_server / 'port/win32',
            includedir_server,
            includedir]
  postgres_lib = meson.get_compiler('c').find_library(
                                           'postgres',
                                            dirs: libdir,
                                            static: true,
                                            required: true
                                         )
else
  incdir = [includedir_server]
  postgres_lib = ''
endif

plpgsql_check_lib = shared_module(
  'plpgsql_check',
  plpgsql_check_sources,
  include_directories: incdir,
  install: true,
  install_dir: pkglibdir,
  name_prefix: '',
  dependencies: postgres_lib,
)

install_data(
  'plpgsql_check.control',
  'plpgsql_check--' + plpgsql_check_version + '.sql',
  install_dir: sharedir / 'extension',
)

pg_regress = find_program(
  'pg_regress',
  dirs: [pkglibdir / 'pgxs/src/test/regress']
)

regress_tests = ['plpgsql_check_active',
                 'plpgsql_check_passive',
                 'plpgsql_check_active-'  + pg_version_major.to_string(),
                 'plpgsql_check_passive-' + pg_version_major.to_string()]

test('regress',
  pg_regress,
  args: ['--bindir', bindir,
         '--inputdir', meson.current_source_dir(),
        ] + regress_tests,
)
