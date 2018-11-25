load 'plpgsql';

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
AS 'MODULE_PATHNAME','plpgsql_profiler_function_tb'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_reset_all()
RETURNS void AS 'MODULE_PATHNAME','plpgsql_profiler_reset_all'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_profiler_reset(funcoid regprocedure)
RETURNS void AS 'MODULE_PATHNAME','plpgsql_profiler_reset'
LANGUAGE C STRICT;
