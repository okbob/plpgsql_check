project('plpgsql_check', ['c'], version: '2.7.14')

pg_config = find_program('pg_config')

bindir = run_command(pg_config, '--bindir', check: true).stdout().strip()
includedir_server = run_command(pg_config, '--includedir-server', check: true).stdout().strip()
includedir = run_command(pg_config, '--includedir', check: true).stdout().strip()
pkglibdir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
sharedir = run_command(pg_config, '--sharedir', check: true).stdout().strip()
libdir = run_command(pg_config, '--libdir', check: true).stdout().strip()

versionstring = run_command(pg_config, '--version', check: true).stdout().strip()
pg_version = versionstring.split(' ')[1]

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

module_version_minor = meson.project_version()
module_version_arr = module_version_minor.split('.')
module_version = module_version_arr[0] + '.' + module_version_arr[1]
module_name = meson.project_name()

sources = files(
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

data = [ module_name + '.control',
         module_name + '--' + module_version + '.sql' ]

tests = [ 'plpgsql_check_active',
          'plpgsql_check_passive',
          'plpgsql_check_active-'  + pg_version_major.to_string(),
          'plpgsql_check_passive-' + pg_version_major.to_string() ]

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
  incdir = [ includedir_server ]
  postgres_lib = []
endif

module_lib = shared_module(
  module_name,
  sources,
  include_directories: incdir,
  install: true,
  install_dir: pkglibdir,
  name_prefix: '',
  dependencies: postgres_lib,
)

install_data(data, install_dir: sharedir / 'extension')

fs = import('fs')
bindistdir = fs.expanduser('~' / module_name / module_name + '-' +
  '-'.join( [ module_version_minor,
              target_machine.cpu(),
              target_machine.system() ] ))

lib_name = fs.name(module_lib.full_path())
lib_suffix = lib_name.split('.')[1]

python_exe = find_program('python3', 'python')

custom_target('bindist',
  depends: module_lib,
  input: module_lib,
  output: 'bindist',
  command: [ python_exe,
             meson.current_source_dir() / 'install_bindist.py',
             meson.current_source_dir(),
             bindistdir,
             module_lib.full_path(),
             fs.stem(lib_name) + '_' + pg_version_major.to_string() + '.' + lib_suffix,
             data ],
  install: false,
  build_by_default: false)

pg_regress = find_program(
  'pg_regress',
  dirs: [pkglibdir / 'pgxs/src/test/regress']
)

test('regress',
  pg_regress,
  args: [ '--bindir', bindir,
          '--inputdir', meson.current_source_dir(),
        ]  + tests,
)
