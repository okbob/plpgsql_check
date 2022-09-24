CREATE FUNCTION plpgsql_check_custom(funcoid oid DEFAULT NULL::oid, warns boolean DEFAULT false, stop_fatal boolean DEFAULT true, msg_like text DEFAULT '%'::text, proname_regex text DEFAULT '^(.*)$'::text, schema_regex text DEFAULT '^(.*)$'::text)
    RETURNS TABLE(ts character, check_msg text)
    LANGUAGE plpgsql
AS
$$
DECLARE
    -- This cursor drives the process (plpgsql_check_function()) does all the work!
    -- The filters are simple enough to filter down the messages, or the procedure name, and to control the INTENSITY of the LINTING
    -- You get the source... Make it your own!  I wanted something I could use flexibly
    msgs CURSOR (func oid, warnings boolean , fatals boolean) FOR SELECT *
            FROM (SELECT p.oid,
                         p.prokind,
                         n.nspname || '.' || p.proname || '()' AS proname,
                         public.plpgsql_check_function(
                             funcoid                => p.oid::regproc
                             , fatal_errors         := fatals
                             , extra_warnings       := warnings
                             , performance_warnings := warnings /* set these 3 to false for initial pass */
                             , all_warnings         := warnings)::text   AS err
                    FROM pg_catalog.pg_namespace         n
                             JOIN pg_catalog.pg_proc     p ON pronamespace = n.oid
                             JOIN pg_catalog.pg_language l ON p.prolang = l.oid
                   WHERE l.lanname = 'plpgsql'
                     AND p.prorettype <> 2279 /* not a trigger */
                     AND n.nspname <> 'public'
                     AND p.prokind IN ('p', 'f') -- Only function and procedures
                     AND p.oid = COALESCE(func, p.oid)
                     AND p.proname OPERATOR (pg_catalog.~) proname_regex
                     AND n.nspname OPERATOR (pg_catalog.~) schema_regex) q1
           WHERE q1.err LIKE msg_like;
    thisproc text := '';  -- Used so we only waste ONE line outputting what function we are working on, as opposed to a COLUMN
    errmsg   text;        -- The error message: "error:42883:42:assignment:function schem.function(integer, unknown, unknown, unknown, unknown, unknown, unknown) does not exist"
    a_txt    text[];      -- Used to pars errmsg
    fdecl    text;        -- Declaration after parsing
    fname    text;        -- Before the parens
    foid     oid;         -- Function OID to lookup the named parameters
    parm1    text;        -- between the parens
    pos      INT;         -- Simple position of ( for parsing
    a_p1     text[];      -- Array of Params from the users code
    has_in   boolean;     -- is IN/OUT present in any parameters
    names    text;        -- Function Signature with Parameter Names
    a_name   text[];      -- string_to_aarray( names, ', ' )                                                                                                                      -- [IN/OUT/INOUT] FLDNAME type [DEFAULT ...]
    a_pname  text[];      -- Name ONLY of the field name
    n_off    INT;         -- Offset into the array for stuff
    str_out  text;        -- Messages to send out, with Chr(10) separating them!
    flow_def text;        -- Should we default to IN all the time for flow
    flow     text;        -- IN/INOUT/OUT + DEF
BEGIN
    ts := TO_CHAR(NOW(), 'HH24:MI:SS'); -- this is constant (Maybe a waste of the column, but forces a TABLE() return in case you want to add more columns, etc!
    FOR msg IN msgs(funcoid, warns, stop_fatal)
    LOOP
        str_out := ''; -- Start Fresh, and add strings as we go, for one final RETURN NEXT!
        IF thisproc <> msg.proname THEN -- Return a header!
            IF thisproc <> '' THEN
                check_msg := '';
                RETURN NEXT; -- Blank line between different functions!
            END IF;
            thisproc  := msg.proname;
            check_msg := CONCAT('===========>  PROCESSING: ', thisproc); -- While REDUNDANT on 42883 Errors, it separates ALL functions from each other!
            RETURN NEXT;
        END IF;
        check_msg := msg.err;
        RETURN NEXT;
        errmsg := msg.err;
        IF errmsg LIKE 'error:42883:%' THEN
            -- SELECT '{}','{}','{}','{}','{}','{}' INTO a_txt, a_p1, a_p2, a_name, a_pname, a_flow;  -- Produces plpgsql_check() warnings!
            a_txt   := '{}';
            a_p1    := '{}';
            a_name  := '{}';
            a_pname := '{}';
            
            str_out := '#### ';
            -- RETURN NEXT;
            IF RIGHT(errmsg, 14) = 'does not exist' THEN errmsg := LEFT(errmsg, -15); END IF;
            a_txt := STRING_TO_ARRAY(errmsg, ':');
            IF CARDINALITY(a_txt) <> 5 THEN
                check_msg := str_out || chr(10) || '######## ==> details unavailable, parsing error <=== #########'::TEXT;
                RETURN NEXT;
                CONTINUE;
            END IF;
            fdecl := a_txt[5];
            pos := POSITION('(' IN fdecl);
            IF pos = 0 THEN
                check_msg := str_out || chr(10) || '######## ==> details unavailable, parsing error(2) <=== #########'::TEXT;
                RETURN NEXT;
                CONTINUE;
            END IF;
            fname := LEFT(fdecl, pos - 1); -- exclude the paren
            fname := SUBSTR(fname, POSITION(' ' IN fname) + 1);
            parm1 := TRIM(SUBSTR(fdecl, pos, POSITION(')' IN fdecl) - pos + 1));
            --       RETURN NEXT (ts , concat('#### ', fdecl ));  -- Really Just Debug!
            BEGIN
                foid := TO_REGPROC(fname)::oid;  -- This function will not throw an exception, just returns NULL
                -- REPLACES the error block
                IF foid IS NULL THEN
                    check_msg := '#### Either No Such function or No Paramters!';
                    RETURN NEXT;
                    CONTINUE;
                END IF;

                str_out := str_out || chr(10) || CONCAT('#### ', 'Error in: ', thisproc, ' at Line: ', a_txt[3], '       PARAMETER TYPING ISSUE?') || chr(10) || '#### ';
                a_p1    := STRING_TO_ARRAY(SUBSTRING(parm1, 2, LENGTH(parm1) - 2), ', ');  -- These are just the types

                SELECT (POSITION('IN ' IN args) + POSITION('OUT ' IN args) )> 0 as tagged, args into has_in, names FROM
                    (SELECT pg_catalog.PG_GET_FUNCTION_ARGUMENTS(foid) as args) t;

                a_name := STRING_TO_ARRAY(names, ', ');   -- Separate these out!  has_in is set for us

                /* We have an array of [INOUT] varname type [DEFAULT xxx] | And an array of the users param types param1 We will OUTPUT:
                   Parameter Name [35], INOUT+DEF[10], P1_TYPE[15], OUR_TYPE \n  */
                str_out := CONCAT(str_out, chr(10), '#### ', rpad('Param Name',20), ' ', rpad('Flow/DEF',10), rpad('(your code)',15), rpad('Definition',15) );
                str_out := CONCAT(str_out, chr(10), '#### ', rpad('==========',20), ' ', rpad('========',10), rpad('===========',15), rpad('==========',15) );
                IF has_in THEN
                    n_off := 1;
                   flow_def := NULL;
                ELSE
                    n_off := 0;
                   flow_def := 'IN ';  -- We have to force the display of IN, just for consistency.
                END IF;
                FOR x IN 1 .. CARDINALITY(a_name)
                LOOP
                    a_pname := STRING_TO_ARRAY(a_name[x], ' '); -- Parse into an array
--                     RAISE NOTICE 'a_pname 1 %, 2 %, 3 %', a_pname[1], a_pname[2], a_pname[3];
                    flow    := COALESCE(flow_def, a_pname[1]) || CASE WHEN POSITION('DEFAULT' IN a_name[x])=0 THEN '' ELSE ' DEF' END;
                    str_out := CONCAT(str_out, chr(10), '#### ', rpad(a_pname[1+n_off],20), ' ',rpad(flow,10), rpad(coalesce(a_p1[x],'???'),15), rpad(a_pname[2+n_off],15) );
                END LOOP;
            EXCEPTION
                WHEN OTHERS THEN
                    str_out := str_out || chr(10) || CONCAT('==== ERROR: ', SQLERRM, '   Unexpected Exception!');
            END;
            str_out := str_out || chr(10) || '#### ';
        ELSE 
            CONTINUE; -- Nothing to do, not our message
        END IF;
        check_msg := str_out;
        RETURN NEXT;
    END LOOP;
    IF thisproc='' AND funcoid is not null THEN
        check_msg := 'No Messages Returned for: ' || funcoid::regproc;
        RETURN NEXT;
    END IF;
    RETURN;
END
$$;
