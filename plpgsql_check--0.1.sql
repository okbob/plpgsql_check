CREATE FUNCTION plpgsql_check_function_tb(funcoid regprocedure,
                                       relid regclass DEFAULT 0,
                                       fatal_errors boolean DEFAULT true,
                                       others_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false)
RETURNS TABLE(functionid regproc,
              lineno int,
              statement text,
              sqlstate text,
              message text,
              detail text,
              hint text,
              level text,
              "position" int,
              query text)
AS 'MODULE_PATHNAME'
LANGUAGE C
RETURNS NULL ON NULL INPUT;
