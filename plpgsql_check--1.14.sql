-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION plpgsql_check" to load this file. \quit

CREATE FUNCTION plpgsql_check_function_tb(funcoid regprocedure,
                                       relid regclass DEFAULT 0,
                                       fatal_errors boolean DEFAULT true,
                                       other_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true,
                                       security_warnings boolean DEFAULT false,
                                       oldtable name DEFAULT null,
                                       newtable name DEFAULT null,
                                       anyelememttype regtype DEFAULT 'int',
                                       anyenumtype regtype DEFAULT '-',
                                       anyrangetype regtype DEFAULT 'int4range',
                                       anycompatibletype regtype DEFAULT 'int',
                                       anycompatiblerangetype regtype DEFAULT 'int4range')
RETURNS TABLE(functionid regproc,
              lineno int,
              statement text,
              sqlstate text,
              message text,
              detail text,
              hint text,
              level text,
              "position" int,
              query text,
              context text)
AS 'MODULE_PATHNAME','plpgsql_check_function_tb'
LANGUAGE C;

CREATE FUNCTION plpgsql_check_function(funcoid regprocedure,
                                       relid regclass DEFAULT 0,
                                       format text DEFAULT 'text',
                                       fatal_errors boolean DEFAULT true,
                                       other_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true,
                                       security_warnings boolean DEFAULT false,
                                       oldtable name DEFAULT null,
                                       newtable name DEFAULT null,
                                       anyelememttype regtype DEFAULT 'int',
                                       anyenumtype regtype DEFAULT '-',
                                       anyrangetype regtype DEFAULT 'int4range',
                                       anycompatibletype regtype DEFAULT 'int',
                                       anycompatiblerangetype regtype DEFAULT 'int4range')

RETURNS SETOF text
AS 'MODULE_PATHNAME','plpgsql_check_function'
LANGUAGE C;

CREATE FUNCTION plpgsql_check_function_tb(name text,
                                       relid regclass DEFAULT 0,
                                       fatal_errors boolean DEFAULT true,
                                       other_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true,
                                       security_warnings boolean DEFAULT false,
                                       oldtable name DEFAULT null,
                                       newtable name DEFAULT null,
                                       anyelememttype regtype DEFAULT 'int',
                                       anyenumtype regtype DEFAULT '-',
                                       anyrangetype regtype DEFAULT 'int4range',
                                       anycompatibletype regtype DEFAULT 'int',
                                       anycompatiblerangetype regtype DEFAULT 'int4range')
RETURNS TABLE(functionid regproc,
              lineno int,
              statement text,
              sqlstate text,
              message text,
              detail text,
              hint text,
              level text,
              "position" int,
              query text,
              context text)
AS 'MODULE_PATHNAME','plpgsql_check_function_tb_name'
LANGUAGE C;

CREATE FUNCTION plpgsql_check_function(name text,
                                       relid regclass DEFAULT 0,
                                       format text DEFAULT 'text',
                                       fatal_errors boolean DEFAULT true,
                                       other_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true,
                                       security_warnings boolean DEFAULT false,
                                       oldtable name DEFAULT null,
                                       newtable name DEFAULT null,
                                       anyelememttype regtype DEFAULT 'int',
                                       anyenumtype regtype DEFAULT '-',
                                       anyrangetype regtype DEFAULT 'int4range',
                                       anycompatibletype regtype DEFAULT 'int',
                                       anycompatiblerangetype regtype DEFAULT 'int4range')
RETURNS SETOF text
AS 'MODULE_PATHNAME','plpgsql_check_function_name'
LANGUAGE C;

CREATE FUNCTION __plpgsql_show_dependency_tb(funcoid regprocedure, relid regclass DEFAULT 0)
RETURNS TABLE(type text,
              oid oid,
              schema text,
              name text,
              params text)
AS 'MODULE_PATHNAME','plpgsql_show_dependency_tb'
LANGUAGE C STRICT;

CREATE FUNCTION __plpgsql_show_dependency_tb(name text, relid regclass DEFAULT 0)
RETURNS TABLE(type text,
              oid oid,
              schema text,
              name text,
              params text)
AS 'MODULE_PATHNAME','plpgsql_show_dependency_tb_name'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_show_dependency_tb(funcoid regprocedure, relid regclass DEFAULT 0)
RETURNS TABLE(type text,
              oid oid,
              schema text,
              name text,
              params text)
AS $$
  SELECT *
    FROM @extschema@.__plpgsql_show_dependency_tb($1, $2)
   ORDER BY 1, 3, 4;
$$ LANGUAGE sql;

CREATE FUNCTION plpgsql_show_dependency_tb(fnname text, relid regclass DEFAULT 0)
RETURNS TABLE(type text,
              oid oid,
              schema text,
              name text,
              params text)
AS $$
  SELECT *
    FROM @extschema@.__plpgsql_show_dependency_tb($1, $2)
   ORDER BY 1, 3, 4;
$$ LANGUAGE sql;

CREATE FUNCTION plpgsql_profiler_function_tb(funcoid regprocedure)
RETURNS TABLE(lineno int,
              stmt_lineno int,
              queryids int8[],
              cmds_on_row int,
              exec_stmts int8,
              total_time double precision,
              avg_time double precision,
              max_time double precision[],
              processed_rows int8[],
              source text)
AS 'MODULE_PATHNAME','plpgsql_profiler_function_tb'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_function_tb(name text)
RETURNS TABLE(lineno int,
              stmt_lineno int,
              queryids int8[],
              cmds_on_row int,
              exec_stmts int8,
              total_time double precision,
              avg_time double precision,
              max_time double precision[],
              processed_rows int8[],
              source text)
AS 'MODULE_PATHNAME','plpgsql_profiler_function_tb_name'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_function_statements_tb(funcoid regprocedure)
RETURNS TABLE(stmtid int,
              parent_stmtid int,
              parent_note text,
              block_num int,
              lineno int,
              queryids int8,
              exec_stmts int8,
              total_time double precision,
              avg_time double precision,
              max_time double precision,
              processed_rows int8,
              stmtname text)
AS 'MODULE_PATHNAME','plpgsql_profiler_function_statements_tb'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_function_statements_tb(name text)
RETURNS TABLE(stmtid int,
              parent_stmtid int,
              parent_note text,
              block_num int,
              lineno int,
              queryids int8,
              exec_stmts int8,
              total_time double precision,
              avg_time double precision,
              max_time double precision,
              processed_rows int8,
              stmtname text)
AS 'MODULE_PATHNAME','plpgsql_profiler_function_statements_tb_name'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_install_fake_queryid_hook()
RETURNS void AS 'MODULE_PATHNAME','plpgsql_profiler_install_fake_queryid_hook'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_remove_fake_queryid_hook()
RETURNS void AS 'MODULE_PATHNAME','plpgsql_profiler_remove_fake_queryid_hook'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_reset_all()
RETURNS void AS 'MODULE_PATHNAME','plpgsql_profiler_reset_all'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_reset(funcoid regprocedure)
RETURNS void AS 'MODULE_PATHNAME','plpgsql_profiler_reset'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION plpgsql_coverage_statements(funcoid regprocedure)
RETURNS double precision AS 'MODULE_PATHNAME', 'plpgsql_coverage_statements'
LANGUAGE C;

CREATE OR REPLACE FUNCTION plpgsql_coverage_statements(name text)
RETURNS double precision AS 'MODULE_PATHNAME', 'plpgsql_coverage_statements_name'
LANGUAGE C;

CREATE OR REPLACE FUNCTION plpgsql_coverage_branches(funcoid regprocedure)
RETURNS double precision AS 'MODULE_PATHNAME', 'plpgsql_coverage_branches'
LANGUAGE C;

CREATE OR REPLACE FUNCTION plpgsql_coverage_branches(name text)
RETURNS double precision AS 'MODULE_PATHNAME', 'plpgsql_coverage_branches_name'
LANGUAGE C;

CREATE OR REPLACE FUNCTION plpgsql_check_pragma(VARIADIC name text[])
RETURNS integer AS 'MODULE_PATHNAME', 'plpgsql_check_pragma'
LANGUAGE C VOLATILE;
