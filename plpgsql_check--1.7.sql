LOAD 'plpgsql';

-- falback solution for PostgreSQL 9.4
CREATE OR REPLACE FUNCTION parse_ident(text)
returns regproc AS $$
BEGIN
  RETURN $1::regproc;
  EXCEPTION WHEN undefined_function or invalid_name THEN
    RAISE EXCEPTION invalid_parameter_value;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION __plpgsql_check_getfuncid(text)
RETURNS regprocedure AS $$
BEGIN
  -- raise a exception handler, when input is function signature
  PERFORM parse_ident($1);
  RETURN $1::regproc::regprocedure;

  EXCEPTION WHEN invalid_parameter_value THEN
    -- try to convert it directly
    RETURN $1::regprocedure;
END
$$ LANGUAGE plpgsql STABLE STRICT SET plpgsql_check.profiler TO off;

CREATE FUNCTION __plpgsql_check_function_tb(funcoid regprocedure,
                                       relid regclass,
                                       fatal_errors boolean,
                                       others_warnings boolean,
                                       performance_warnings boolean,
                                       extra_warnings boolean,
                                       sql_injection_check boolean,
                                       oldtable name,
                                       newtable name)
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

CREATE FUNCTION __plpgsql_check_function(funcoid regprocedure,
                                       relid regclass,
                                       format text,
                                       fatal_errors boolean,
                                       others_warnings boolean,
                                       performance_warnings boolean,
                                       extra_warnings boolean,
                                       sql_injection_check boolean,
                                       oldtable name,
                                       newtable name)

RETURNS SETOF text
AS 'MODULE_PATHNAME','plpgsql_check_function'
LANGUAGE C;

CREATE FUNCTION plpgsql_check_function_tb(funcoid regprocedure,
                                       relid regclass DEFAULT 0,
                                       fatal_errors boolean DEFAULT true,
                                       others_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true,
                                       sql_injection_check boolean DEFAULT false,
                                       oldtable name DEFAULT null,
                                       newtable name DEFAULT null)

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
AS $$
BEGIN
  RETURN QUERY SELECT * FROM @extschema@.__plpgsql_check_function_tb(funcoid, relid,
                                      fatal_errors, others_warnings, performance_warnings, extra_warnings);
  RETURN;
END;
$$ LANGUAGE plpgsql SET plpgsql_check.profiler TO off;

CREATE FUNCTION plpgsql_check_function(funcoid regprocedure,
                                       relid regclass DEFAULT 0,
                                       format text DEFAULT 'text',
                                       fatal_errors boolean DEFAULT true,
                                       others_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true,
                                       sql_injection_check boolean DEFAULT false,
                                       oldtable name DEFAULT null,
                                       newtable name DEFAULT null)

RETURNS SETOF text
AS $$
BEGIN
  RETURN QUERY SELECT s FROM @extschema@.__plpgsql_check_function(funcoid, relid,
                                  format, fatal_errors, others_warnings,
                                  performance_warnings, extra_warnings,
                                  sql_injection_check, oldtable, newtable) g(s);
  RETURN;
END;
$$ LANGUAGE plpgsql SET plpgsql_check.profiler TO off;

CREATE FUNCTION plpgsql_check_function_tb(name text,
                                       relid regclass DEFAULT 0,
                                       fatal_errors boolean DEFAULT true,
                                       others_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true,
                                       sql_injection_check boolean DEFAULT false,
                                       oldtable name DEFAULT null,
                                       newtable name DEFAULT null)

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
AS $$
BEGIN
  RETURN QUERY SELECT * FROM @extschema@.__plpgsql_check_function_tb(@extschema@.__plpgsql_check_getfuncid(name), relid,
                                      fatal_errors, others_warnings, performance_warnings, extra_warnings,
                                      sql_injection_check, oldtable, newtable);
  RETURN;
END;
$$ LANGUAGE plpgsql SET plpgsql_check.profiler TO off;

CREATE FUNCTION plpgsql_check_function(name text,
                                       relid regclass DEFAULT 0,
                                       format text DEFAULT 'text',
                                       fatal_errors boolean DEFAULT true,
                                       others_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true,
                                       sql_injection_check boolean DEFAULT false,
                                       oldtable name DEFAULT null,
                                       newtable name DEFAULT null)

RETURNS SETOF text
AS $$
BEGIN
  RETURN QUERY SELECT s FROM @extschema@.__plpgsql_check_function(@extschema@.__plpgsql_check_getfuncid(name), relid,
                                  format, fatal_errors, others_warnings,
                                  performance_warnings, extra_warnings,
                                  sql_injection_check, oldtable, newtable) g(s);
  RETURN;
END;
$$ LANGUAGE plpgsql SET plpgsql_check.profiler TO off;

CREATE FUNCTION plpgsql_show_dependency_tb(funcoid regprocedure, relid regclass DEFAULT 0)
RETURNS TABLE(type text,
              oid oid,
              schema text,
              name text,
              params text)
AS $$
BEGIN
  RETURN QUERY SELECT * 
                  FROM @extschema@.__plpgsql_show_dependency_tb(funcoid, relid)
                 ORDER BY 1, 3, 4;
END;
$$ LANGUAGE plpgsql STRICT SET plpgsql_check.profiler TO off;

CREATE FUNCTION plpgsql_show_dependency_tb(fnname text, relid regclass DEFAULT 0)
RETURNS TABLE(type text,
              oid oid,
              schema text,
              name text,
              params text)
AS $$
BEGIN
  RETURN QUERY SELECT * 
                  FROM @extschema@.__plpgsql_show_dependency_tb(@extschema@.__plpgsql_check_getfuncid(fnname), relid)
                 ORDER BY 1, 3, 4;
END;
$$ LANGUAGE plpgsql STRICT SET plpgsql_check.profiler TO off;

CREATE FUNCTION __plpgsql_show_dependency_tb(funcoid regprocedure, relid regclass DEFAULT 0)
RETURNS TABLE(type text,
              oid oid,
              schema text,
              name text,
              params text)
AS 'MODULE_PATHNAME','plpgsql_show_dependency_tb'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_function_tb(funcoid regprocedure)
RETURNS TABLE(lineno int,
              stmt_lineno int,
              cmds_on_row int,
              exec_stmts int8,
              total_time double precision,
              avg_time double precision,
              max_time double precision[],
              processed_rows int8[],
              source text)
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM @extschema@.__plpgsql_profiler_function_tb(funcoid);
END
$$ LANGUAGE plpgsql STRICT SET plpgsql_check.profiler TO off;

CREATE FUNCTION plpgsql_profiler_function_tb(name text)
RETURNS TABLE(lineno int,
              stmt_lineno int,
              cmds_on_row int,
              exec_stmts int8,
              total_time double precision,
              avg_time double precision,
              max_time double precision[],
              processed_rows int8[],
              source text)
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM @extschema@.__plpgsql_profiler_function_tb(@extschema@.__plpgsql_check_getfuncid(name));
END
$$ LANGUAGE plpgsql STRICT SET plpgsql_check.profiler TO off;

CREATE FUNCTION __plpgsql_profiler_function_tb(funcoid regprocedure)
RETURNS TABLE(lineno int,
              stmt_lineno int,
              cmds_on_row int,
              exec_stmts int8,
              total_time double precision,
              avg_time double precision,
              max_time double precision[],
              processed_rows int8[],
              source text)
AS 'MODULE_PATHNAME','plpgsql_profiler_function_tb'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_function_statements_tb(name text)
RETURNS TABLE(stmtid int,
              parent_stmtid int,
              parent_note text,
              block_num int,
              lineno int,
              exec_stmts int8,
              total_time double precision,
              avg_time double precision,
              max_time double precision,
              processed_rows int8,
              stmtname text)
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM @extschema@.__plpgsql_profiler_function_statements_tb(@extschema@.__plpgsql_check_getfuncid(name));
END
$$ LANGUAGE plpgsql STRICT SET plpgsql_check.profiler TO off;

CREATE FUNCTION __plpgsql_profiler_function_statements_tb(funcoid regprocedure)
RETURNS TABLE(stmtid int,
              parent_stmtid int,
              parent_note text,
              block_num int,
              lineno int,
              exec_stmts int8,
              total_time double precision,
              avg_time double precision,
              max_time double precision,
              processed_rows int8,
              stmtname text)
AS 'MODULE_PATHNAME','plpgsql_profiler_function_statements_tb'
LANGUAGE C STRICT;

CREATE FUNCTION __plpgsql_profiler_reset_all()
RETURNS void AS 'MODULE_PATHNAME','plpgsql_profiler_reset_all'
LANGUAGE C STRICT;

CREATE FUNCTION __plpgsql_profiler_reset(funcoid regprocedure)
RETURNS void AS 'MODULE_PATHNAME','plpgsql_profiler_reset'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_reset_all()
RETURNS void AS $$
BEGIN
  PERFORM @extschema@.__plpgsql_profiler_reset_all();
END;
$$ LANGUAGE plpgsql SET plpgsql_check.profiler TO off;

CREATE FUNCTION plpgsql_profiler_reset(funcoid regprocedure)
RETURNS void AS $$
BEGIN
  PERFORM @extschema@.__plpgsql_profiler_reset(funcoid);
END;
$$ LANGUAGE plpgsql SET plpgsql_check.profiler TO off;
