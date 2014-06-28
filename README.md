plpgsql_check
=============

I founded this project, because I wanted to publish the code I wrote in the last two years,
when I tried to write enhanced checking for PostgreSQL upstream. It was not fully
successful - integration into upstream requires some larger plpgsql refactoring - probably
it will not be done in next two years (now is Dec 2013). But written code is fully functional
and can be used in production. So, I created this extension to be available for all
plpgsql developers.

If you like it and if you would to join to development of this extension, register
yourself to [postgresql extension hacking](https://groups.google.com/forum/#!forum/postgresql-extensions-hacking)
google group.

I invite any ideas, patches, bugreports

plpgsql_check is next generation of plpgsql_lint. It allows to check source code by explicit call
<i>plpgsql_check_function</i>.

PostgreSQL 9.2 is required, PostgreSQL 9.3 and 9.4 are supported

The SQL statements inside PL/pgSQL functions are checked by validator for semantic errors. These errors
can be found by plpgsql_check_function:

# Active mode

    postgres=# load 'plpgsql';
    LOAD
    postgres=# CREATE EXTENSION plpgsql_check;
    LOAD
    postgres=# CREATE TABLE t1(a int, b int);
    CREATE TABLE

    postgres=#
    CREATE OR REPLACE FUNCTION public.f1()
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
    DECLARE r record;
    BEGIN
      FOR r IN SELECT * FROM t1
      LOOP
        RAISE NOTICE '%', r.c; -- there is bug - table t1 missing "c" column
      END LOOP;
    END;
    $function$;

    CREATE FUNCTION

    postgres=# select f1(); -- execution doesn't find a bug due to empty table t1
      f1 
     ────
       
     (1 row)

    postgres=# \x
    Expanded display is on.
    postgres=# select * from plpgsql_check_function_tb('f1()');
    ─[ RECORD 1 ]───────────────────────────
    functionid │ f1
    lineno     │ 6
    statement  │ RAISE
    sqlstate   │ 42703
    message    │ record "r" has no field "c"
    detail     │ [null]
    hint       │ [null]
    level      │ error
    position   │ 0
    query      │ [null]

    postgres=# \sf+ f1
        CREATE OR REPLACE FUNCTION public.f1()
         RETURNS void
         LANGUAGE plpgsql
    1       AS $function$
    2       DECLARE r record;
    3       BEGIN
    4         FOR r IN SELECT * FROM t1
    5         LOOP
    6           RAISE NOTICE '%', r.c; -- there is bug - table t1 missing "c" column
    7         END LOOP;
    8       END;
    9       $function$


Function plpgsql_check_function() has two possible formats: text or xml

    select * from plpgsql_check_function('f1()', fatal_errors := false);
                             plpgsql_check_function                         
    ------------------------------------------------------------------------
     error:42703:4:SQL statement:column "c" of relation "t1" does not exist
     Query: update t1 set c = 30
     --                   ^
     error:42P01:7:RAISE:missing FROM-clause entry for table "r"
     Query: SELECT r.c
     --            ^
     error:42601:7:RAISE:too few parameters specified for RAISE
    (7 rows)

    postgres=# select * from plpgsql_check_function('fx()', format:='xml');
                     plpgsql_check_function                     
    ────────────────────────────────────────────────────────────────
     <Function oid="16400">                                        ↵
       <Issue>                                                     ↵
         <Level>error</level>                                      ↵
         <Sqlstate>42P01</Sqlstate>                                ↵
         <Message>relation "foo111" does not exist</Message>       ↵
         <Stmt lineno="3">RETURN</Stmt>                            ↵
         <Query position="23">SELECT (select a from foo111)</Query>↵
       </Issue>                                                    ↵
      </Function>
     (1 row)


# Passive mode

Functions should be checked on start - plpgsql_check module must be loaded.

## Configuration

    plpgsql_check.mode = [ disabled | by_function | fresh_start | every_start ]
    plpgsql_check.fatal_errors = [ yes | no ]

    plpgsql_check.show_nonperformance_warnings = false
    plpgsql_check.show_performance_warnings = false

Default mode is <i>by_function</i>, that means that the enhanced check is done only in
active mode - by <i>plpgsql_check_function</i>.

You can enable passive mode by

    load 'plpgsql';
    load 'plpgsql_check';
    set plpgsql_check.mode = 'every_start';

    SELECT fx(10); -- run functions - function is checked before runtime starts it

# Limits

<i>plpgsql_check</i> should find almost all errors on really static code. When developer use some
PLpgSQL's dynamic features like dynamic SQL or record data type, then false positives are
possible. These should be rare - in well written code - and then the affected function
should be redesigned or plpgsql_check should be disabled for this function.


    CREATE OR REPLACE FUNCTION f1()
    RETURNS void AS $$
    DECLARE r record;
    BEGIN
      FOR r IN EXECUTE 'SELECT * FROM t1'
      LOOP
        RAISE NOTICE '%', r.c;
      END LOOP;
    END;
    $$ LANGUAGE plpgsql SET plpgsql.enable_check TO false;

<i>A usage of plpgsql_check adds a small overhead (in enabled passive mode) and you should use
it only in develop or preprod environments.</i>

## Dynamic SQL

This module doesn't check queries that are assembled in runtime. It is not possible
to identify results of dynamic queries - so <i>plpgsql_check</i> cannot to set correct type to record
variables and cannot to check a dependent SQLs and expressions. Don't use record variable
as target for dynamic queries or disable <i>plpgsql_check</i> for functions that use dynamic
queries.

## Temporary tables

<i>plpgsql_check</i> cannot verify queries over temporary tables that are created in plpgsql's function
runtime. For this use case it is necessary to create a fake temp table or disable <i>plpgsql_check</i> for this
function.

# Licence

Copyright (c) Pavel Stehule (pavel.stehule@gmail.com)

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.

# Note

If you like it, send a postcard to address

    Pavel Stehule
    Skalice 12
    256 01 Benesov u Prahy
    Czech Republic


I invite any questions, comments, bug reports, patches on mail address pavel.stehule@gmail.com
