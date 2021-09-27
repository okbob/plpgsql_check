[![Build Status](https://travis-ci.com/okbob/plpgsql_check.svg?branch=master)](https://travis-ci.com/okbob/plpgsql_check)

plpgsql_check
=============

I founded this project, because I wanted to publish the code I wrote in the last two years,
when I tried to write enhanced checking for PostgreSQL upstream. It was not fully
successful - integration into upstream requires some larger plpgsql refactoring - probably
it will not be done in next years (now is Dec 2013). But written code is fully functional
and can be used in production (and it is used in production). So, I created this extension to
be available for all plpgsql developers.

If you like it and if you would to join to development of this extension, register
yourself to [postgresql extension hacking](https://groups.google.com/forum/#!forum/postgresql-extensions-hacking)
google group.

# Features

* check fields of referenced database objects and types inside embedded SQL
* using correct types of function parameters
* unused variables and function argumens, unmodified OUT argumens
* partially detection of dead code (due RETURN command)
* detection of missing RETURN command in function
* try to identify unwanted hidden casts, that can be performance issue like unused indexes
* possibility to collect relations and functions used by function
* possibility to check EXECUTE stmt agaist SQL injection vulnerability

I invite any ideas, patches, bugreports.

plpgsql_check is next generation of plpgsql_lint. It allows to check source code by explicit call
<i>plpgsql_check_function</i>.

PostgreSQL PostgreSQL 10, 11, 12, 13 and 14 are supported.

The SQL statements inside PL/pgSQL functions are checked by validator for semantic errors. These errors
can be found by plpgsql_check_function:

# Active mode

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


Function plpgsql_check_function() has three possible formats: text, json or xml

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

## Arguments

You can set level of warnings via function's parameters:

### Mandatory arguments

* function name or function signature - these functions requires function specification.
  Any function in PostgreSQL can be specified by Oid or by name or by signature. When
  you know oid or complete function's signature, you can use a regprocedure type parameter
  like `'fx()'::regprocedure` or `16799::regprocedure`. Possible alternative is using
  a name only, when function's name is unique - like `'fx'`. When the name is not unique
  or the function doesn't exists it raises a error.

### Optional arguments

* `relid DEFAULT 0` - oid of relation assigned with trigger function. It is necessary for check
   of any trigger function.

* `fatal_errors boolean DEFAULT true` - stop on first error

* `other_warnings boolean DEFAULT true` - show warnings like different attributes number
  in assignmenet on left and right side, variable overlaps function's parameter, unused
  variables, unwanted casting, ..

* `extra_warnings boolean DEFAULT true` - show warnings like missing `RETURN`,
  shadowed variables, dead code, never read (unused) function's parameter,
  unmodified variables, modified auto variables, ..

* `performance_warnings boolean DEFAULT false` - performance related warnings like
  declared type with type modificator, casting, implicit casts in where clause (can be
  reason why index is not used), ..

* `security_warnings boolean DEFAULT false` - security related checks like SQL injection
  vulnerability detection

* `anyelementtype regtype DEFAULT 'int'` - a real type used instead anyelement type

* `anyenumtype regtype DEFAULT '-'` - a real type used instead anyenum type

* `anyrangetype regtype DEFAULT 'int4range'` - a real type used instead anyrange type

* `anycompatibletype DEFAULT 'int'` - a real type used instead anycompatible type

* `anycompatiblerangetype DEFAULT 'int4range'` - a real type used instead anycompatible range type

* `without_warnings DEFAULT false` - disable all warnings

* `all_warnings DEFAULT false` - enable all warnings

* `newtable DEFAULT NULL`, `oldtable DEFAULT NULL` - the names of NEW or OLD transitive
   tables. These parameters are required when transitive tables are used.

## Triggers

When you want to check any trigger, you have to enter a relation that will be
used together with trigger function

    CREATE TABLE bar(a int, b int);

    postgres=# \sf+ foo_trg
        CREATE OR REPLACE FUNCTION public.foo_trg()
             RETURNS trigger
             LANGUAGE plpgsql
    1       AS $function$
    2       BEGIN
    3         NEW.c := NEW.a + NEW.b;
    4         RETURN NEW;
    5       END;
    6       $function$

Missing relation specification

    postgres=# select * from plpgsql_check_function('foo_trg()');
    ERROR:  missing trigger relation
    HINT:  Trigger relation oid must be valid

Correct trigger checking (with specified relation)

    postgres=# select * from plpgsql_check_function('foo_trg()', 'bar');
                     plpgsql_check_function                 
    --------------------------------------------------------
     error:42703:3:assignment:record "new" has no field "c"
    (1 row)

For triggers with transitive tables you can set a `oldtable` or `newtable` parameters:

    create or replace function footab_trig_func()
    returns trigger as $$
    declare x int;
    begin
      if false then
        -- should be ok;
        select count(*) from newtab into x; 

        -- should fail;
        select count(*) from newtab where d = 10 into x;
      end if;
      return null;
    end;
    $$ language plpgsql;

    select * from plpgsql_check_function('footab_trig_func','footab', newtable := 'newtab');


## Mass check

You can use the plpgsql_check_function for mass check functions and mass check
triggers. Please, test following queries:

    -- check all nontrigger plpgsql functions
    SELECT p.oid, p.proname, plpgsql_check_function(p.oid)
       FROM pg_catalog.pg_namespace n
       JOIN pg_catalog.pg_proc p ON pronamespace = n.oid
       JOIN pg_catalog.pg_language l ON p.prolang = l.oid
      WHERE l.lanname = 'plpgsql' AND p.prorettype <> 2279;

or

    SELECT p.proname, tgrelid::regclass, cf.*
       FROM pg_proc p
            JOIN pg_trigger t ON t.tgfoid = p.oid 
            JOIN pg_language l ON p.prolang = l.oid
            JOIN pg_namespace n ON p.pronamespace = n.oid,
            LATERAL plpgsql_check_function(p.oid, t.tgrelid) cf
      WHERE n.nspname = 'public' and l.lanname = 'plpgsql'

or

    -- check all plpgsql functions (functions or trigger functions with defined triggers)
    SELECT
        (pcf).functionid::regprocedure, (pcf).lineno, (pcf).statement,
        (pcf).sqlstate, (pcf).message, (pcf).detail, (pcf).hint, (pcf).level,
        (pcf)."position", (pcf).query, (pcf).context
    FROM
    (
        SELECT
            plpgsql_check_function_tb(pg_proc.oid, COALESCE(pg_trigger.tgrelid, 0)) AS pcf
        FROM pg_proc
        LEFT JOIN pg_trigger
            ON (pg_trigger.tgfoid = pg_proc.oid)
        WHERE
            prolang = (SELECT lang.oid FROM pg_language lang WHERE lang.lanname = 'plpgsql') AND
            pronamespace <> (SELECT nsp.oid FROM pg_namespace nsp WHERE nsp.nspname = 'pg_catalog') AND
            -- ignore unused triggers
            (pg_proc.prorettype <> (SELECT typ.oid FROM pg_type typ WHERE typ.typname = 'trigger') OR
             pg_trigger.tgfoid IS NOT NULL)
        OFFSET 0
    ) ss
    ORDER BY (pcf).functionid::regprocedure::text, (pcf).lineno

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

    load 'plpgsql'; -- 1.1 and higher doesn't need it
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
variables and cannot to check a dependent SQLs and expressions. 

When type of record's variable is not know, you can assign it explicitly with pragma `type`:

    DECLARE r record;
    BEGIN
      EXECUTE format('SELECT * FROM %I', _tablename) INTO r;
      PERFORM plpgsql_check_pragma('type: r (id int, processed bool)');
      IF NOT r.processed THEN
        ...

<b>
Attention: The SQL injection check can detect only some SQL injection vulnerabilities. This tool
cannot be used for security audit! Some issues should not be detected. This check can raise false
alarms too - probably when variable is sanitized by other command or when value is of some compose
type.
</b>

## Refcursors

<i>plpgsql_check</i> should not to detect structure of referenced cursors. A reference on cursor
in PLpgSQL is implemented as name of global cursor. In check time, the name is not known (not in
all possibilities), and global cursor doesn't exist. It is significant break for any static analyse.
PLpgSQL cannot to set correct type for record variables and cannot to check a dependent SQLs and
expressions. A solution is same like dynamic SQL. Don't use record variable
as target when you use <i>refcursor</i> type or disable <i>plpgsql_check</i> for these functions.

    CREATE OR REPLACE FUNCTION foo(refcur_var refcursor)
    RETURNS void AS $$
    DECLARE
      rec_var record;
    BEGIN
      FETCH refcur_var INTO rec_var; -- this is STOP for plpgsql_check
      RAISE NOTICE '%', rec_var;     -- record rec_var is not assigned yet error

In this case a record type should not be used (use known rowtype instead):

    CREATE OR REPLACE FUNCTION foo(refcur_var refcursor)
    RETURNS void AS $$
    DECLARE
      rec_var some_rowtype;
    BEGIN
      FETCH refcur_var INTO rec_var;
      RAISE NOTICE '%', rec_var;

## Temporary tables

<i>plpgsql_check</i> cannot verify queries over temporary tables that are created in plpgsql's function
runtime. For this use case it is necessary to create a fake temp table or disable <i>plpgsql_check</i> for this
function.

In reality temp tables are stored in own (per user) schema with higher priority than persistent
tables. So you can do (with following trick safetly):

    CREATE OR REPLACE FUNCTION public.disable_dml()
    RETURNS trigger
    LANGUAGE plpgsql AS $function$
    BEGIN
      RAISE EXCEPTION SQLSTATE '42P01'
         USING message = format('this instance of %I table doesn''t allow any DML operation', TG_TABLE_NAME),
               hint = format('you should to run "CREATE TEMP TABLE %1$I(LIKE %1$I INCLUDING ALL);" statement',
                             TG_TABLE_NAME);
      RETURN NULL;
    END;
    $function$;
    
    CREATE TABLE foo(a int, b int); -- doesn't hold data ever
    CREATE TRIGGER foo_disable_dml
       BEFORE INSERT OR UPDATE OR DELETE ON foo
       EXECUTE PROCEDURE disable_dml();

    postgres=# INSERT INTO  foo VALUES(10,20);
    ERROR:  this instance of foo table doesn't allow any DML operation
    HINT:  you should to run "CREATE TEMP TABLE foo(LIKE foo INCLUDING ALL);" statement
    postgres=# 
    
    CREATE TABLE
    postgres=# INSERT INTO  foo VALUES(10,20);
    INSERT 0 1

This trick emulates GLOBAL TEMP tables partially and it allows a statical validation.
Other possibility is using a [template foreign data wrapper] (https://github.com/okbob/template_fdw)

You can use pragma `table` and create ephemeral table:

    BEGIN
       CREATE TEMP TABLE xxx(a int);
       PERFORM plpgsql_check_pragma('table: xxx(a int)');
       INSERT INTO xxx VALUES(10);


# Dependency list

A function <i>plpgsql_show_dependency_tb</i> can show all functions and relations used
inside processed function:

    postgres=# select * from plpgsql_show_dependency_tb('testfunc(int,float)');
    ┌──────────┬───────┬────────┬─────────┬────────────────────────────┐
    │   type   │  oid  │ schema │  name   │           params           │
    ╞══════════╪═══════╪════════╪═════════╪════════════════════════════╡
    │ FUNCTION │ 36008 │ public │ myfunc1 │ (integer,double precision) │
    │ FUNCTION │ 35999 │ public │ myfunc2 │ (integer,double precision) │
    │ RELATION │ 36005 │ public │ myview  │                            │
    │ RELATION │ 36002 │ public │ mytable │                            │
    └──────────┴───────┴────────┴─────────┴────────────────────────────┘
    (4 rows)

# Profiler

The plpgsql_check contains simple profiler of plpgsql functions and procedures. It can work with/without
a access to shared memory. It depends on `shared_preload_libraries` config. When plpgsql_check was initialized
by `shared_preload_libraries`, then it can allocate shared memory, and function's profiles are stored there.
When plpgsql_check cannot to allocate shared momory, the profile is stored in session memory.

The profile of any function is updated after successful execution of function. When function was canceled
or it fails, the profiles is not updated.

Due dependencies, `shared_preload_libraries` should to contains `plpgsql` first

    postgres=# show shared_preload_libraries ;
    ┌──────────────────────────┐
    │ shared_preload_libraries │
    ╞══════════════════════════╡
    │ plpgsql,plpgsql_check    │
    └──────────────────────────┘
    (1 row)

The profiler is active when GUC `plpgsql_check.profiler` is on. The profiler doesn't require shared memory,
but if there are not shared memory, then the profile is limmitted just to active session.

When plpgsql_check is initialized by `shared_preload_libraries`, another GUC is
available to configure the amount of shared memory used by the profiler:
`plpgsql_check.profiler_max_shared_chunks`.  This defines the maximum number of
statements chunk that can be stored in shared memory.  For each plpgsql
function (or procedure), the whole content is split into chunks of 30
statements.  If needed, multiple chunks can be used to store the whole content
of a single function.  A single chunk is 1704 bytes.  The default value for
this GUC is 15000, which should be enough for big projects containing hundred
of thousands of statements in plpgsql, and will consume about 24MB of memory.
If your project doesn't require that much number of chunks, you can set this
parameter to a smaller number in order to decrease the memory usage.  The
minimum value is 50 (which should consume about 83kB of memory), and the
maximum value is 100000 (which should consume about 163MB of memory).  Changing
this parameter requires a PostgreSQL restart.

The profiler will also retrieve the query identifier for each instruction that
contains an expression or optimizable statement.  Note that this requires
pg_stat_statements, or another similar third-party extension), to be installed.
There are some limitations to the query identifier retrieval:

* if a plpgsql expression contains underlying statements, only the top level
  query identifier will be retrieved
* the profiler doesn't compute query identifier by itself but relies on
  external extension, such as pg_stat_statements, for that.  It means that
  depending on the external extension behavior, you may not be able to see a
  query identifier for some statements.  That's for instance the case with DDL
  statements, as pg_stat_statements doesn't expose the query identifier for
  such queries.
* a query identifier is retrieved only for instructions containing
  expressions.  This means that plpgsql_profiler_function_tb() function can
  report less query identifier than instructions on a single line.

Attention: A update of shared profiles can decrease performance on servers under higher load.

The profile can be displayed by function `plpgsql_profiler_function_tb`:

    postgres=# select lineno, avg_time, source from plpgsql_profiler_function_tb('fx(int)');
    ┌────────┬──────────┬───────────────────────────────────────────────────────────────────┐
    │ lineno │ avg_time │                              source                               │
    ╞════════╪══════════╪═══════════════════════════════════════════════════════════════════╡
    │      1 │          │                                                                   │
    │      2 │          │ declare result int = 0;                                           │
    │      3 │    0.075 │ begin                                                             │
    │      4 │    0.202 │   for i in 1..$1 loop                                             │
    │      5 │    0.005 │     select result + i into result; select result + i into result; │
    │      6 │          │   end loop;                                                       │
    │      7 │        0 │   return result;                                                  │
    │      8 │          │ end;                                                              │
    └────────┴──────────┴───────────────────────────────────────────────────────────────────┘
    (9 rows)

The profile per statements (not per line) can be displayed by function plpgsql_profiler_function_statements_tb:

            CREATE OR REPLACE FUNCTION public.fx1(a integer)
             RETURNS integer
             LANGUAGE plpgsql
    1       AS $function$
    2       begin
    3         if a > 10 then
    4           raise notice 'ahoj';
    5           return -1;
    6         else
    7           raise notice 'nazdar';
    8           return 1;
    9         end if;
    10      end;
    11      $function$

    postgres=# select stmtid, parent_stmtid, parent_note, lineno, exec_stmts, stmtname
                 from plpgsql_profiler_function_statements_tb('fx1');
    ┌────────┬───────────────┬─────────────┬────────┬────────────┬─────────────────┐
    │ stmtid │ parent_stmtid │ parent_note │ lineno │ exec_stmts │    stmtname     │
    ╞════════╪═══════════════╪═════════════╪════════╪════════════╪═════════════════╡
    │      0 │             ∅ │ ∅           │      2 │          0 │ statement block │
    │      1 │             0 │ body        │      3 │          0 │ IF              │
    │      2 │             1 │ then body   │      4 │          0 │ RAISE           │
    │      3 │             1 │ then body   │      5 │          0 │ RETURN          │
    │      4 │             1 │ else body   │      7 │          0 │ RAISE           │
    │      5 │             1 │ else body   │      8 │          0 │ RETURN          │
    └────────┴───────────────┴─────────────┴────────┴────────────┴─────────────────┘
    (6 rows)

All stored profiles can be displayed by calling function `plpgsql_profiler_functions_all`:

    postgres=# select * from plpgsql_profiler_functions_all();
    ┌───────────────────────┬────────────┬────────────┬──────────┬─────────────┬──────────┬──────────┐
    │        funcoid        │ exec_count │ total_time │ avg_time │ stddev_time │ min_time │ max_time │
    ╞═══════════════════════╪════════════╪════════════╪══════════╪═════════════╪══════════╪══════════╡
    │ fxx(double precision) │          1 │       0.01 │     0.01 │        0.00 │     0.01 │     0.01 │
    └───────────────────────┴────────────┴────────────┴──────────┴─────────────┴──────────┴──────────┘
    (1 row)


There are two functions for cleaning stored profiles: `plpgsql_profiler_reset_all()` and
`plpgsql_profiler_reset(regprocedure)`.

## Coverage metrics

plpgsql_check provides two functions:

* `plpgsql_coverage_statements(name)`
* `plpgsql_coverage_branches(name)`

## Note

There is another very good PLpgSQL profiler - https://bitbucket.org/openscg/plprofiler

My extension is designed to be simple for use and practical. Nothing more or less.

plprofiler is more complex. It build call graphs and from this graph it can creates
flame graph of execution times.

Both extensions can be used together with buildin PostgreSQL's feature - tracking functions.

    set track_functions to 'pl';
    ...
    select * from pg_stat_user_functions;

# Tracer

plpgsql_check provides a tracing possibility - in this mode you can see notices on
start or end functions (terse and default verbosity) and start or end statements
(verbose verbosity). For default and verbose verbosity the content of function arguments
is displayed. The content of related variables are displayed when verbosity is verbose.

    postgres=# do $$ begin perform fx(10,null, 'now', e'stěhule'); end; $$;
    NOTICE:  #0 ->> start of inline_code_block (Oid=0)
    NOTICE:  #2   ->> start of function fx(integer,integer,date,text) (Oid=16405)
    NOTICE:  #2        call by inline_code_block line 1 at PERFORM
    NOTICE:  #2       "a" => '10', "b" => null, "c" => '2020-08-03', "d" => 'stěhule'
    NOTICE:  #4     ->> start of function fx(integer) (Oid=16404)
    NOTICE:  #4          call by fx(integer,integer,date,text) line 1 at PERFORM
    NOTICE:  #4         "a" => '10'
    NOTICE:  #4     <<- end of function fx (elapsed time=0.098 ms)
    NOTICE:  #2   <<- end of function fx (elapsed time=0.399 ms)
    NOTICE:  #0 <<- end of block (elapsed time=0.754 ms)

The number after `#` is a execution frame counter (this number is related to deep of error context stack).
It allows to pair start end and of function.

Tracing is enabled by setting `plpgsql_check.tracer` to `on`. Attention - enabling this behaviour
has significant negative impact on performance (unlike the profiler). You can set a level for output used by
tracer `plpgsql_check.tracer_errlevel` (default is `notice`). The output content is limited by length
specified by `plpgsql_check.tracer_variable_max_length` configuration variable.

In terse verbose mode the output is reduced:

    postgres=# set plpgsql_check.tracer_verbosity TO terse;
    SET
    postgres=# do $$ begin perform fx(10,null, 'now', e'stěhule'); end; $$;
    NOTICE:  #0 start of inline code block (oid=0)
    NOTICE:  #2 start of fx (oid=16405)
    NOTICE:  #4 start of fx (oid=16404)
    NOTICE:  #4 end of fx
    NOTICE:  #2 end of fx
    NOTICE:  #0 end of inline code block

In verbose mode the output is extended about statement details:

    postgres=# do $$ begin perform fx(10,null, 'now', e'stěhule'); end; $$;
    NOTICE:  #0            ->> start of block inline_code_block (oid=0)
    NOTICE:  #0.1       1  --> start of PERFORM
    NOTICE:  #2              ->> start of function fx(integer,integer,date,text) (oid=16405)
    NOTICE:  #2                   call by inline_code_block line 1 at PERFORM
    NOTICE:  #2                  "a" => '10', "b" => null, "c" => '2020-08-04', "d" => 'stěhule'
    NOTICE:  #2.1       1    --> start of PERFORM
    NOTICE:  #2.1                "a" => '10'
    NOTICE:  #4                ->> start of function fx(integer) (oid=16404)
    NOTICE:  #4                     call by fx(integer,integer,date,text) line 1 at PERFORM
    NOTICE:  #4                    "a" => '10'
    NOTICE:  #4.1       6      --> start of assignment
    NOTICE:  #4.1                  "a" => '10', "b" => '20'
    NOTICE:  #4.1              <-- end of assignment (elapsed time=0.076 ms)
    NOTICE:  #4.1                  "res" => '130'
    NOTICE:  #4.2       7      --> start of RETURN
    NOTICE:  #4.2                  "res" => '130'
    NOTICE:  #4.2              <-- end of RETURN (elapsed time=0.054 ms)
    NOTICE:  #4                <<- end of function fx (elapsed time=0.373 ms)
    NOTICE:  #2.1            <-- end of PERFORM (elapsed time=0.589 ms)
    NOTICE:  #2              <<- end of function fx (elapsed time=0.727 ms)
    NOTICE:  #0.1          <-- end of PERFORM (elapsed time=1.147 ms)
    NOTICE:  #0            <<- end of block (elapsed time=1.286 ms)

Special feature of tracer is tracing of `ASSERT` statement when `plpgsql_check.trace_assert` is `on`. When
`plpgsql_check.trace_assert_verbosity` is `DEFAULT`, then all function's or procedure's variables are
displayed when assert expression is false. When this configuration is `VERBOSE` then all variables
from all plpgsql frames are displayed. This behaviour is independent on `plpgsql.check_asserts` value.
It can be used, although the assertions are disabled in plpgsql runtime.

    postgres=# set plpgsql_check.tracer to off;
    postgres=# set plpgsql_check.trace_assert_verbosity TO verbose;

    postgres=# do $$ begin perform fx(10,null, 'now', e'stěhule'); end; $$;
    NOTICE:  #4 PLpgSQL assert expression (false) on line 12 of fx(integer) is false
    NOTICE:   "a" => '10', "res" => null, "b" => '20'
    NOTICE:  #2 PL/pgSQL function fx(integer,integer,date,text) line 1 at PERFORM
    NOTICE:   "a" => '10', "b" => null, "c" => '2020-08-05', "d" => 'stěhule'
    NOTICE:  #0 PL/pgSQL function inline_code_block line 1 at PERFORM
    ERROR:  assertion failed
    CONTEXT:  PL/pgSQL function fx(integer) line 12 at ASSERT
    SQL statement "SELECT fx(a)"
    PL/pgSQL function fx(integer,integer,date,text) line 1 at PERFORM
    SQL statement "SELECT fx(10,null, 'now', e'stěhule')"
    PL/pgSQL function inline_code_block line 1 at PERFORM

    postgres=# set plpgsql.check_asserts to off;
    SET
    postgres=# do $$ begin perform fx(10,null, 'now', e'stěhule'); end; $$;
    NOTICE:  #4 PLpgSQL assert expression (false) on line 12 of fx(integer) is false
    NOTICE:   "a" => '10', "res" => null, "b" => '20'
    NOTICE:  #2 PL/pgSQL function fx(integer,integer,date,text) line 1 at PERFORM
    NOTICE:   "a" => '10', "b" => null, "c" => '2020-08-05', "d" => 'stěhule'
    NOTICE:  #0 PL/pgSQL function inline_code_block line 1 at PERFORM
    DO


## Attention - SECURITY

Tracer prints content of variables or function arguments. For security definer function, this
content can hold security sensitive data. This is reason why tracer is disabled by default and should
be enabled only with super user rights `plpgsql_check.enable_tracer`.

# Pragma

You can configure plpgsql_check behave inside checked function with "pragma" function. This
is a analogy of PL/SQL or ADA language of PRAGMA feature. PLpgSQL doesn't support PRAGMA, but
plpgsql_check detects function named `plpgsql_check_pragma` and get options from parameters of
this function. These plpgsql_check options are valid to end of group of statements.

    CREATE OR REPLACE FUNCTION test()
    RETURNS void AS $$
    BEGIN
      ...
      -- for following statements disable check
      PERFORM plpgsql_check_pragma('disable:check');
      ...
      -- enable check again
      PERFORM plpgsql_check_pragma('enable:check');
      ...
    END;
    $$ LANGUAGE plpgsql;

The function `plpgsql_check_pragma` is immutable function that returns one. It is defined
by `plpgsql_check` extension. You can declare alternative `plpgsql_check_pragma` function
like:

    CREATE OR REPLACE FUNCTION plpgsql_check_pragma(VARIADIC args[])
    RETURNS int AS $$
    SELECT 1
    $$ LANGUAGE sql IMMUTABLE;

Using pragma function in declaration part of top block sets options on function level too.

    CREATE OR REPLACE FUNCTION test()
    RETURNS void AS $$
    DECLARE
      aux int := plpgsql_check_pragma('disable:extra_warnings');
      ...


## Supported pragmas

* `echo:str` - print string (for testing)

* `status:check`,`status:tracer`, `status:other_warnings`, `status:performance_warnings`, `status:extra_warnings`,`status:security_warnings`

* `enable:check`,`enable:tracer`, `enable:other_warnings`, `enable:performance_warnings`, `enable:extra_warnings`,`enable:security_warnings`

* `disable:check`,`disable:tracer`, `disable:other_warnings`, `disable:performance_warnings`, `disable:extra_warnings`,`disable:security_warnings`

* `type:varname typename` or `type:varname (fieldname type, ...)` - set type to variable of record type

* `table: name (column_name type, ...)` or `table: name (like tablename)` - create ephereal table

Pragmas `enable:tracer` and `disable:tracer`are active for Postgres 12 and higher

# Compilation

You need a development environment for PostgreSQL extensions:

    make clean
    make install

result:

    [pavel@localhost plpgsql_check]$ make USE_PGXS=1 clean
    rm -f plpgsql_check.so   libplpgsql_check.a  libplpgsql_check.pc
    rm -f plpgsql_check.o
    rm -rf results/ regression.diffs regression.out tmp_check/ log/
    [pavel@localhost plpgsql_check]$ make USE_PGXS=1 clean
    rm -f plpgsql_check.so   libplpgsql_check.a  libplpgsql_check.pc
    rm -f plpgsql_check.o
    rm -rf results/ regression.diffs regression.out tmp_check/ log/
    [pavel@localhost plpgsql_check]$ make USE_PGXS=1 all
    clang -O2 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fpic -I/usr/local/pgsql/lib/pgxs/src/makefiles/../../src/pl/plpgsql/src -I. -I./ -I/usr/local/pgsql/include/server -I/usr/local/pgsql/include/internal -D_GNU_SOURCE   -c -o plpgsql_check.o plpgsql_check.c
    clang -O2 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fpic -I/usr/local/pgsql/lib/pgxs/src/makefiles/../../src/pl/plpgsql/src -shared -o plpgsql_check.so plpgsql_check.o -L/usr/local/pgsql/lib -Wl,--as-needed -Wl,-rpath,'/usr/local/pgsql/lib',--enable-new-dtags  
    [pavel@localhost plpgsql_check]$ su root
    Password: *******
    [root@localhost plpgsql_check]# make USE_PGXS=1 install
    /usr/bin/mkdir -p '/usr/local/pgsql/lib'
    /usr/bin/mkdir -p '/usr/local/pgsql/share/extension'
    /usr/bin/mkdir -p '/usr/local/pgsql/share/extension'
    /usr/bin/install -c -m 755  plpgsql_check.so '/usr/local/pgsql/lib/plpgsql_check.so'
    /usr/bin/install -c -m 644 plpgsql_check.control '/usr/local/pgsql/share/extension/'
    /usr/bin/install -c -m 644 plpgsql_check--0.9.sql '/usr/local/pgsql/share/extension/'
    [root@localhost plpgsql_check]# exit
    [pavel@localhost plpgsql_check]$ make USE_PGXS=1 installcheck
    /usr/local/pgsql/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --psqldir='/usr/local/pgsql/bin'    --dbname=pl_regression --load-language=plpgsql --dbname=contrib_regression plpgsql_check_passive plpgsql_check_active plpgsql_check_active-9.5
    (using postmaster on Unix socket, default port)
    ============== dropping database "contrib_regression" ==============
    DROP DATABASE
    ============== creating database "contrib_regression" ==============
    CREATE DATABASE
    ALTER DATABASE
    ============== installing plpgsql                     ==============
    CREATE LANGUAGE
    ============== running regression test queries        ==============
    test plpgsql_check_passive    ... ok
    test plpgsql_check_active     ... ok
    test plpgsql_check_active-9.5 ... ok
    
    =====================
     All 3 tests passed. 
    =====================

## Compilation on Ubuntu

Sometimes successful compilation can require libicu-dev package (PostgreSQL 10 and higher - when pg was compiled with
ICU support)

    sudo apt install libicu-dev

## Compilation plpgsql_check on OS X

use `-undefined dynamic_lookup` to the last line of the `Makefile ("override CFLAGS += ...")` allowed it to build
(It should not be necessary for current code).

## Compilation plpgsql_check on Windows

You can check precompiled dll libraries http://okbob.blogspot.cz/2015/02/plpgsqlcheck-is-available-for-microsoft.html

or compile by self:

1. Download and install PostgreSQL for Win32 from http://www.enterprisedb.com
2. Download and install Microsoft Visual C++ Express
3. Lern tutorial http://blog.2ndquadrant.com/compiling-postgresql-extensions-visual-studio-windows
4. The plpgsql_check depends on plpgsql and we need to add plpgsql.lib to the library list. Unfortunately PostgreSQL 9.4.3 does not contain this library.
5. Create a plpgsql.lib from plpgsql.dll as described in http://adrianhenke.wordpress.com/2008/12/05/create-lib-file-from-dll (this step is not necessary now)
6. Change `plpgsql_check.c` file, add `PGDLLEXPORT` line before evry extension function, as described in http://blog.2ndquadrant.com/compiling-postgresql-extensions-visual-studio-windows 
   (Skip this step if you have a version with "plpgsql_check_builtins.h" header file).
   <pre>
    ...PGDLLEXPORT
    Datum plpgsql_check_function_tb(PG_FUNCTION_ARGS);
    PGDLLEXPORT
    Datum plpgsql_check_function(PG_FUNCTION_ARGS);
    ...
    PGDLLEXPORT
    Datum
    plpgsql_check_function(PG_FUNCTION_ARGS)
    {
    Oid            funcoid = PG_GETARG_OID(0);
    ...
    PGDLLEXPORT
    Datum
    plpgsql_check_function_tb(PG_FUNCTION_ARGS)
    {
    Oid            funcoid = PG_GETARG_OID(0);
    ...
   </pre>
7. Build plpgsql_check.dll
8. Install plugin
  1. copy `plpgsql_check.dll` to `PostgreSQL\9.3\lib`
  2. copy `plpgsql_check.control` and `plpgsql_check--0.8.sql` to `PostgreSQL\9.3\share\extension`

## Checked on

* gcc on Linux (against all supported PostgreSQL)
* clang 3.4 on Linux (against PostgreSQL 9.5)
* for success regress tests the PostgreSQL 9.5 or higher is required

Compilation against PostgreSQL 10 requires libICU!

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
