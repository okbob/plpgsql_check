LOAD 'plpgsql';

CREATE FUNCTION __plpgsql_check_function_tb(funcoid regprocedure,
                                       relid regclass,
                                       fatal_errors boolean,
                                       others_warnings boolean,
                                       performance_warnings boolean,
                                       extra_warnings boolean)
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
LANGUAGE C STRICT;

CREATE FUNCTION __plpgsql_check_function(funcoid regprocedure,
                                       relid regclass,
                                       format text,
                                       fatal_errors boolean,
                                       others_warnings boolean,
                                       performance_warnings boolean,
                                       extra_warnings boolean)
RETURNS SETOF text
AS 'MODULE_PATHNAME','plpgsql_check_function'
LANGUAGE C STRICT;

CREATE FUNCTION plpgsql_check_function_tb(funcoid regprocedure,
                                       relid regclass DEFAULT 0,
                                       fatal_errors boolean DEFAULT true,
                                       others_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true)
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
$$ LANGUAGE plpgsql STRICT;

CREATE FUNCTION plpgsql_check_function(funcoid regprocedure,
                                       relid regclass DEFAULT 0,
                                       format text DEFAULT 'text',
                                       fatal_errors boolean DEFAULT true,
                                       others_warnings boolean DEFAULT true,
                                       performance_warnings boolean DEFAULT false,
                                       extra_warnings boolean DEFAULT true)
RETURNS SETOF text
AS $$
BEGIN
  RETURN QUERY SELECT s FROM @extschema@.__plpgsql_check_function(funcoid, relid,
                                  format, fatal_errors, others_warnings,
                                  performance_warnings, extra_warnings) g(s);
  RETURN;
END;
$$ LANGUAGE plpgsql STRICT;

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
$$ LANGUAGE plpgsql STRICT;

CREATE FUNCTION __plpgsql_show_dependency_tb(funcoid regprocedure, relid regclass DEFAULT 0)
RETURNS TABLE(type text,
              oid oid,
              schema text,
              name text,
              params text)
AS 'MODULE_PATHNAME','plpgsql_show_dependency_tb'
LANGUAGE C STRICT;
