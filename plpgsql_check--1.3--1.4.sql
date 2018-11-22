load 'plpgsql';

CREATE FUNCTION plpgsql_profiler_function_tb(funcoid regprocedure)
RETURNS TABLE(lineno int,
              stmt_lineno int,
              exec_count int8,
              total_time int8,
              max_time int8[],
              rows int8[],
              source text)
AS 'MODULE_PATHNAME','plpgsql_profiler_function_tb'
LANGUAGE C STRICT;
