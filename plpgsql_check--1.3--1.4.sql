load 'plpgsql';

CREATE FUNCTION plpgsql_profiler_function_tb(funcoid regprocedure)
RETURNS TABLE(lineno int,
              stmt_lineno int,
              exec_count int8,
              total_time numeric(20,2),
              avg_time numeric(20,2),
              max_time numeric(20,2)[],
              processed_rows int8[],
              source text)
AS 'MODULE_PATHNAME','plpgsql_profiler_function_tb'
LANGUAGE C STRICT;
