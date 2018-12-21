load 'plpgsql';

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
