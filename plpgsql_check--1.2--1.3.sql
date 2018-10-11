LOAD 'plpgsql';

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

