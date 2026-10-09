[![Stand With Ukraine](https://raw.githubusercontent.com/vshymanskyy/StandWithUkraine/main/banner2-direct.svg)](https://stand-with-ukraine.pp.ua)

plpgsql_check
=============

This extension is a full linter for plpgsql for PostgreSQL.  It leverages only the internal 
PostgreSQL parser/evaluator so you see exactly the errors would occur at runtime.
Furthermore, it parses the SQL inside your routines and finds errors not usually found during
the "CREATE PROCEDURE/FUNCTION" command.  You can control the levels of many warnings and hints.
Finally, you can add PRAGMA type markers to turn off/on many aspects allowing you to hide
messages you already know about, or to remind you to come back for deeper cleaning later.

I founded this project, because I wanted to publish the code I wrote for the  two years,
when I tried to write enhanced checking for PostgreSQL upstream. It was not fully
successful - integration into upstream requires some larger plpgsql refactoring.
But the code is fully functional and can be used in production (and it is used in production).
So, I created this extension to be available for all plpgsql developers.

If you want to join our group to help the further development of this extension, register
yourself at that [postgresql extension hacking](https://groups.google.com/forum/#!forum/postgresql-extensions-hacking)
google group.

# Features

* checks fields of referenced database objects and types inside embedded SQL
* validates you are using the correct types for function parameters
* identifies unused variables and function arguments, unmodified OUT arguments
* partial detection of dead code (code after an RETURN command)
* detection of missing RETURN command in function (common after exception handlers, complex logic)
* tries to identify unwanted hidden casts, which can be a performance issue like unused indexes
* ability to collect relations and functions used by function
* ability to check EXECUTE statements against SQL injection vulnerability

I invite any ideas, patches, bugreports.

PostgreSQL 14 - 20 are supported.

The SQL statements inside PL/pgSQL functions are checked by the validator for semantic errors. These errors
can be found by calling the plpgsql_check_function:

# Active mode

    postgres=# CREATE EXTENSION plpgsql_check;
    CREATE EXTENSION
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


Function plpgsql_check_function() has three possible output formats: text, json or xml

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
            
### Mandatory argument

* `funcoid regprocedure` (or `name text` for the text overload) - function name or function signature.
  Any function in PostgreSQL can be specified by Oid or by name or by signature. When
  you know oid or complete function's signature, you can use a regprocedure type parameter
  like `'fx()'::regprocedure` or `16799::regprocedure`. Possible alternative is using
  a name only, when function's name is unique - like `'fx'`. When the name is not unique
  or the function doesn't exists it raises a error.

### Optional arguments

* `relid regclass DEFAULT 0` - relation on which a DML trigger operates. It is required
   for checking DML trigger functions, but must not be supplied for ordinary functions
   or procedures. It can also be supplied by an in-comment option.

* `fatal_errors boolean DEFAULT true` - stop on first error (prevents massive error reports)

* `other_warnings boolean DEFAULT true` - show warnings like different attributes number
  in assignment on left and right side, variable overlaps function's parameter, unused
  variables, unwanted casting, etc.

* `extra_warnings boolean DEFAULT true` - show warnings like missing `RETURN`,
  shadowed variables, dead code, never read (unused) function's parameter,
  unmodified variables, modified auto variables, etc.

* `performance_warnings boolean DEFAULT false` - performance related warnings like
  declared type with type modifier, casting, implicit casts in where clause (can be
  the reason why an index is not used), etc.

* `security_warnings boolean DEFAULT false` - security related checks like SQL injection
  vulnerability detection

* `compatibility_warnings boolean DEFAULT false` - compatibility related checks like obsolete explicit
  setting internal cursor names in refcursor's or cursor's variables.

* `anyelememttype regtype DEFAULT 'int'` - an actual type to be used when testing
  `anyelement`. The SQL argument retains this historical misspelling; the corresponding
  in-comment option is spelled `anyelementtype`.

* `anyenumtype regtype DEFAULT '-'` - an actual type to be used when testing the anyenum type

* `anyrangetype regtype DEFAULT 'int4range'` - an actual type to be used when testing
  `anyrange`; its associated multirange type is used for `anymultirange`

* `anycompatibletype DEFAULT 'int'` - an actual type to be used when testing the anycompatible type

* `anycompatiblerangetype DEFAULT 'int4range'` - an actual range type to be used when
  testing `anycompatiblerange`; its associated multirange type is used for
  `anycompatiblemultirange`

* `without_warnings DEFAULT false` - disable all warnings (Ignores all xxxx_warning parameters, a quick override)

* `all_warnings DEFAULT false` - enable all warnings (Ignores other xxx_warning parameters, a quick positive)

* `newtable DEFAULT NULL`, `oldtable DEFAULT NULL` - the names of NEW or OLD transition
   tables. These parameters are required when transition tables are used in trigger functions.

* `use_incomment_options DEFAULT true` - when it is true, then in-comment options are active

* `incomment_options_usage_warning DEFAULT false` - when it is true, then the warning is raised when
   in-comment option is used.

* `constant_tracing boolean DEFAULT true` - when it is true, then the variable that holds
   some constant content, can be used like constant (it is work only in some simple cases,
   and the content of variable should not be ambiguous).

* `pragmas text[] DEFAULT NULL` - list of pragmas that are applied before the check is
   started. Use scope-independent pragmas here, such as table and sequence definitions
   or warning switches. Pragmas that need a PL/pgSQL variable namespace (`type` and
   `assert-schema`, `assert-table`, `assert-column`) must instead be placed inside the
   function; they are ignored in this argument. The main usage is passing table pragmas
   generated by `plpgsql_make_pragma` (see the section "Table pragmas generator").

## Triggers

When checking a DML trigger function, supply the relation on which it operates,
either through `relid` or an in-comment option.

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

For triggers with transition tables you can set the `oldtable` and `newtable` parameters:

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


## In-comment options

plpgsql_check allows persistent setting written in comments. These options are taken from
function's source code before checking. The syntax is:

    @plpgsql_check_options: optname [=] value [, optname [=] value ...]

The settings from comment options has top high priority, but generally it can be disabled
by option `use_incomment_options` to `false`.

Comment overrides are applied before validating the final options. A trigger's
required relation can therefore be provided by `@plpgsql_check_options: relid = schema.table`.
An ordinary function cannot acquire trigger-only options through a comment.

Example:

    create or replace function fx(anyelement)
    returns text as $$
    begin
      /*
       * rewrite default polymorphic type to text
       * @plpgsql_check_options: anyelementtype = text
       */
      return $1;
    end;
    $$ language plpgsql;


## Checking all of your code

You can use the plpgsql_check_function for mass checking of functions/procedures and mass checking
of triggers. Please, test following queries:

    -- check all nontrigger plpgsql functions
    SELECT p.oid, p.proname, plpgsql_check_function(p.oid)
       FROM pg_catalog.pg_namespace n
       JOIN pg_catalog.pg_proc p ON pronamespace = n.oid
       JOIN pg_catalog.pg_language l ON p.prolang = l.oid
      WHERE l.lanname = 'plpgsql' AND p.prorettype <> 2279;

or

    -- check all trigger plpgsql functions
    SELECT p.proname, tgrelid::regclass, cf.*
       FROM pg_proc p
            JOIN pg_trigger t ON t.tgfoid = p.oid 
            JOIN pg_language l ON p.prolang = l.oid
            JOIN pg_namespace n ON p.pronamespace = n.oid,
            LATERAL plpgsql_check_function(p.oid, t.tgrelid, oldtable=>t.tgoldtable, newtable=>t.tgnewtable) cf
      WHERE n.nspname = 'public' and l.lanname = 'plpgsql';

or

    -- check all plpgsql functions (functions or trigger functions with defined triggers)
    SELECT
        (pcf).functionid::regprocedure, (pcf).lineno, (pcf).statement,
        (pcf).sqlstate, (pcf).message, (pcf).detail, (pcf).hint, (pcf).level,
        (pcf)."position", (pcf).query, (pcf).context
    FROM
    (
        SELECT
            plpgsql_check_function_tb(pg_proc.oid, COALESCE(pg_trigger.tgrelid, 0),
                                      oldtable=>pg_trigger.tgoldtable,
                                      newtable=>pg_trigger.tgnewtable) AS pcf
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
    ORDER BY (pcf).functionid::regprocedure::text, (pcf).lineno;

# Passive mode (only recommended for development or preproduction)

Functions can be checked upon execution - plpgsql_check module must be loaded (via postgresql.conf).

## Configuration Settings

    plpgsql_check.mode = [ disabled | by_function | fresh_start | every_start ]
    plpgsql_check.fatal_errors = [ yes | no ]

    plpgsql_check.show_nonperformance_warnings = false
    plpgsql_check.show_performance_warnings = false

Default mode is <i>by_function</i>, that means that the enhanced check is done only in
active mode - by calling the <i>plpgsql_check_function</i>. `fresh_start` means cold start (first the function is called).

You can enable passive mode by

    load 'plpgsql'; -- 1.1 and higher doesn't need it
    load 'plpgsql_check';
    set plpgsql_check.mode = 'every_start';  -- This scans all code before it is executed

    SELECT fx(10); -- run functions - function is checked before runtime starts it

# Compatibility warnings

## Assigning string to refcursor variable

PostgreSQL cursor's and refcursor's variables are enhanced string variables that holds
unique name of related portal (internal structure of Postgres that is used for cursor's
implementation). Until PostgreSQL 16, the portal had same name like name of cursor
variable. PostgreSQL 16 and higher change this mechanism and by default related portal
will be named by some unique name. It solves some issues with cursors in nested blocks
or when cursor is used in recursive called function.

With mentioned change, the refcursor's variable should to take value from another
refcursor variable or from some cursor variable (when cursor is opened).

    -- obsolete pattern
    DECLARE
      cur CURSOR FOR SELECT 1;
      rcur refcursor;
    BEGIN
      rcur := 'cur';
      OPEN cur;
      ...

    -- new pattern
    DECLARE
      cur CURSOR FOR SELECT 1;
      rcur refcursor;
    BEGIN
      OPEN cur;
      rcur := cur;
      ...

When `compatibility_warnings` flag is active, then `plpgsql_check` try to identify
some fishy assigning to refcursor's variable or returning of refcursor's values:

    CREATE OR REPLACE FUNCTION public.foo()
     RETURNS refcursor
    AS $$
    declare
       c cursor for select 1;
       r refcursor;
    begin
      open c;
      r := 'c';
      return r;
    end;
    $$ LANGUAGE plpgsql;

    select * from plpgsql_check_function('foo', extra_warnings =>false, compatibility_warnings => true);
    ┌───────────────────────────────────────────────────────────────────────────────────┐
    │                              plpgsql_check_function                               │
    ╞═══════════════════════════════════════════════════════════════════════════════════╡
    │ compatibility:00000:6:assignment:obsolete setting of refcursor or cursor variable │
    │ Detail: Internal name of cursor should not be specified by users.                 │
    │ Context: at assignment to variable "r" declared on line 3                         │
    └───────────────────────────────────────────────────────────────────────────────────┘
    (3 rows)

# Limits

<i>plpgsql_check</i> should find almost all errors on really static code. When developers use
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
    $$ LANGUAGE plpgsql SET plpgsql_check.mode TO 'disabled';

<i>A usage of plpgsql_check adds a small overhead (when passive mode is enabled) and you should use
that setting only in development or preproduction environments.</i>

## Dynamic SQL

The checker can analyze dynamically executed queries when constant tracing or
known `format()` arguments determine their text, including field widths and
padding. When the query text or a format width is unknown, it cannot reliably
infer the result's record type or check expressions depending on its fields.

When type of record's variable is not know, you can assign it explicitly with pragma `type`:

    DECLARE r record;
    BEGIN
      EXECUTE format('SELECT * FROM %I', _tablename) INTO r;
      PERFORM plpgsql_check_pragma('type: r (id int, processed bool)');
      IF NOT r.processed THEN
        ...

<b>
Attention: The SQL injection check can detect only some SQL injection vulnerabilities. This tool
cannot be used for security audit! Some issues will not be detected. This check can raise false
alarms too - probably when variable is sanitized by other command or when the value is of some composite
type.
</b>

## Refcursors

<i>plpgsql_check</i> cannot be used to detect structure of referenced cursors. A reference on cursor
in PLpgSQL is implemented as name of global cursor. In check time, the name is not known (not in
all possibilities), and global cursor doesn't exist. It is a significant issue for any static analysis.

If the row structure is known, declare the target using a named composite type or
`table_name%ROWTYPE`. Alternatively, describe a record target with a pragma:

    CREATE OR REPLACE FUNCTION foo(refcur_var refcursor)
    RETURNS void AS $$
    DECLARE
      rec_var record;
    BEGIN
      FETCH refcur_var INTO rec_var;
      PERFORM plpgsql_check_pragma('type: rec_var (id integer, value text)');
      RAISE NOTICE '%', rec_var.id;
    END;
    $$ LANGUAGE plpgsql;

The pragma describes the expected row structure for checking; it does not change
the cursor or its result type at runtime.

## Temporary tables

Queries over temporary tables created at runtime need a check-time table definition.
Use a table pragma (written manually or generated by `plpgsql_make_pragma`), create
the temporary table before checking, or provide a matching permanent template.
Disabling checking for the function is a fallback.

Temporary tables are stored in a per-session schema, normally searched before schemas
containing persistent tables. This permits the following template-table technique:

    CREATE OR REPLACE FUNCTION public.disable_dml()
    RETURNS trigger
    LANGUAGE plpgsql AS $function$
    BEGIN
      RAISE EXCEPTION SQLSTATE '42P01'
         USING message = format('this instance of %I table doesn''t allow any DML operation', TG_TABLE_NAME),
               hint = format('you should use "CREATE TEMP TABLE %1$I(LIKE %1$I INCLUDING ALL);" statement',
                             TG_TABLE_NAME);
      RETURN NULL;
    END;
    $function$;
    
    CREATE TABLE foo(a int, b int); -- doesn't hold data, ever
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
       PERFORM plpgsql_check_pragma('table: pg_temp.zzz(like schemaname.table1)');
       ...

For temporary tables created inside the function's body, the table pragmas can be
generated automatically by the function `plpgsql_make_pragma` (see the section
"Table pragmas generator").


# Dependency list

A function <i>plpgsql_show_dependency_tb</i> will show all functions, operators and relations used
inside processed function:

    postgres=# select * from plpgsql_show_dependency_tb('testfunc(int,float)');
    ┌──────────┬───────┬────────┬─────────┬────────────────────────────┐
    │   type   │  oid  │ schema │  name   │           params           │
    ╞══════════╪═══════╪════════╪═════════╪════════════════════════════╡
    │ FUNCTION │ 36008 │ public │ myfunc1 │ (integer,double precision) │
    │ FUNCTION │ 35999 │ public │ myfunc2 │ (integer,double precision) │
    │ OPERATOR │ 36007 │ public │ **      │ (integer,integer)          │
    │ RELATION │ 36005 │ public │ myview  │                            │
    │ RELATION │ 36002 │ public │ mytable │                            │
    └──────────┴───────┴────────┴─────────┴────────────────────────────┘
    (4 rows)

Optional arguments of <i>plpgsql_show_dependency_tb</i> are `relid`, `anyelememttype`, `anyenumtype`,
`anyrangetype`, `anycompatibletype` and `anycompatiblerangetype`.

# Profiler

plpgsql_check includes a profiler for PL/pgSQL functions and procedures. It can
store statistics either in session-local memory or in shared memory. Shared
storage requires loading the library at server startup, for example:

    shared_preload_libraries = 'plpgsql_check'

It is not necessary to list `plpgsql` before `plpgsql_check`. Without shared
preloading, profiling works in the current session only.

Load the extension before executing the PL/pgSQL routines you want to profile or
trace. Calling `SELECT plpgsql_check_profiler(true)` loads the installed library
and enables profiling without requiring a privileged `LOAD` command. Profiling is
active while `plpgsql_check.profiler` is `on`; calling
`plpgsql_check_profiler(false)` disables it.

When shared storage is available, `plpgsql_check.use_shared_stats_when_it_possible`
selects it by default (`on`). Set this option to `off` to use local statistics.
Exhausting shared storage does not automatically switch to local storage.

Shared per-statement statistics are buffered in a transaction-local cache by
default: `plpgsql_check.use_lxcache` is `on`, and updates are merged at transaction
end. This reduces lock contention when many sessions execute short functions.
Set it to `off` to merge these statistics immediately after each function execution.

`plpgsql_check.max_stats_size` limits storage for per-statement statistics, not
all profiler overhead. Its default is 20 MB, minimum 64 kB, and maximum 200 MB.
When capacity is exhausted, new statement profiles are skipped with a warning.
`plpgsql_profiler_reset_all()` makes the allocated capacity reusable. Shared
storage is allocated at server startup, so changing its size requires a restart.
The local-storage limit can be changed without restarting the server.

The profiler can also retrieve query identifiers from cached plans for expressions
and static SQL statements. PostgreSQL's core query-ID computation can provide them
when an administrator enables `compute_query_id = on`; `pg_stat_statements` is not
required. An extension that enables or supplies query IDs can also be used.
There are some limitations to the query identifier retrieval:

* if a plpgsql expression contains underlying statements, only the top level
  query identifier will be retrieved
* the profiler does not compute query identifiers itself. Availability depends
  on PostgreSQL or the query-ID provider; some statements, including DDL, may
  not have an identifier.
* a query identifier is retrieved only for instructions containing
  expressions.  This means that plpgsql_profiler_function_tb() function can
  report less query identifier than instructions on a single line.
* query_id of dynamically executed queries are reported only when
  `plpgsql_check.profiler_show_dynquery_query_id` is on (default is off).
  Attention: in this case, the expression that produce query string is
  executed second by profiler.

Attention: An update of shared profiles can decrease performance on servers under higher load.

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

The times in the result are in milliseconds.

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

    postgres=# select stmtid, parent_stmtid, block_num, lineno, exec_stmts, stmtname
                 from plpgsql_profiler_function_statements_tb('fx1');
     stmtid | parent_stmtid | block_num | lineno | exec_stmts |    stmtname
    --------+---------------+-----------+--------+------------+-----------------
          1 |               |         1 |      2 |            | statement block
          2 |             1 |         1 |      3 |            | IF
          3 |             2 |         1 |      4 |            | RAISE
          4 |             2 |         2 |      5 |            | RETURN
          5 |             2 |         3 |      7 |            | RAISE
          6 |             2 |         4 |      8 |            | RETURN
    (6 rows)

Statement IDs start at 1. `block_num` is the statement's ordinal within its parent,
not a branch label. This example has no collected profile yet, so execution counts
are NULL (displayed as blanks).

All stored profiles can be displayed by calling function `plpgsql_profiler_functions_all`:

    postgres=# select funcoid, exec_count, total_time, avg_time, stddev_time, min_time, max_time
                 from plpgsql_profiler_functions_all();
    ┌───────────────────────┬────────────┬────────────┬──────────┬─────────────┬──────────┬──────────┐
    │        funcoid        │ exec_count │ total_time │ avg_time │ stddev_time │ min_time │ max_time │
    ╞═══════════════════════╪════════════╪════════════╪══════════╪═════════════╪══════════╪══════════╡
    │ fxx(double precision) │          1 │       0.01 │     0.01 │        0.00 │     0.01 │     0.01 │
    └───────────────────────┴────────────┴────────────┴──────────┴─────────────┴──────────┴──────────┘
    (1 row)


`SELECT *` also includes the `exec_stmts_err` column.

There are two functions for cleaning stored profiles: `plpgsql_profiler_reset_all()` and
`plpgsql_profiler_reset(regprocedure)`.

## Coverage metrics

plpgsql_check provides two functions:

* `plpgsql_coverage_statements(name)`
* `plpgsql_coverage_branches(name)`

The coverage data are collected only when profiling is active.

An exception while evaluating an `IF` or `ELSIF` condition does not cover a
branch, including an implicit `ELSE`. An exception after entering a branch
does count as reaching that branch.

## Note

There is another very good PLpgSQL profiler - https://github.com/glynastill/plprofiler

My extension is designed to be simple for use and practical. Nothing more or less.

plprofiler is more complex. It builds call graphs and from this graph it can create
flame graph of execution times.

Both extensions can be used together with the builtin PostgreSQL's feature - tracking functions.

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

The number after `#` is a execution frame counter (this number is related to depth of error context stack).
It allows to pair start and end of function. Attention - the initial depth of error context stack can be different
in dependency on environment (and used protocol).

Tracing is enabled by setting `plpgsql_check.tracer` to `on`. Attention - enabling this behaviour
has significant negative impact on performance (unlike the profiler). You can set a level for output used by
tracer `plpgsql_check.tracer_errlevel` (default is `notice`). The output content is limited by length
specified by `plpgsql_check.tracer_variable_max_length` configuration variable. The tracer can be activated
by calling function `plpgsql_check_tracer(true)` and disabled by calling same function with `false` argument
(or with literals `on`, `off`).

First, the usage of tracer should be explicitly enabled by superuser by setting `set plpgsql_check.enable_tracer to on;`
or `plpgsql_check.enable_tracer to on` in `postgresql.conf`. This is a security safeguard. The tracer shows content of
plpgsql's variables, and then some security sensitive information can be displayed to an unprivileged user (when he runs
security definer function). Second, the extension `plpgsql_check` should be loaded. It can be done by execution of some
`plpgsql_check` function or explicitly by command `load 'plpgsql_check';`. You can use configuration's option
`shared_preload_libraries`, `local_preload_libraries` or `session_preload_libraries`.

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

A special feature of the tracer is tracing `ASSERT` statements when
`plpgsql_check.trace_assert` is `on`. A false assertion prints the current routine's
variables. `plpgsql_check.trace_assert_verbosity = DEFAULT` also prints outer
PL/pgSQL frame locations; `VERBOSE` additionally prints those frames' variables.
This works independently of `plpgsql.check_asserts`, including when runtime
assertions are disabled.

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

Attention: When plpgsql assertions is enabled, then assert expression is evaluated
2x times - first by plpgsql_check's tracer, second by plpgsql engine.

Tracer can show usage of subtransaction buffer id (`nxids`). The displayed `tnl` number
is transaction nesting level number (for plpgsql it depends on deep of blocks with
exception's handlers).

## Detection of unclosed cursors

PLpgSQL's cursors are just names of SQL cursors. The life cycle of SQL cursors is not
joined with scope of related plpgsql's cursor variable. SQL cursors are closed by self
at transaction end, but for long transaction and too much opened cursors it can be too late.
It is better to close cursor explicitly when cursor is not necessary (by CLOSE statement).
Without it the significant memory issues are possible.

When OPEN statement try to use cursor that is not closed yet, the warning is raised.
This feature can be disabled by setting `plpgsql_check.cursors_leaks to off`. This check
is not active, when routine is called recursively.

The unclosed cursors can be checked immediately when function is finished. This check is
disabled by default, and should be enabled by `plpgsql_check.strict_cursors_leaks to on`.

Any unclosed cursor is reported once.

## Using with plugin_debugger

If you use `plugin_debugger` (plpgsql debugger) together with `plpgsql_check`, then
`plpgsql_check` should be initialized after `plugin_debugger` (because `plugin_debugger`
doesn't support the sharing of PL/pgSQL's debug API). For example (`postgresql.conf`):

    shared_preload_libraries = 'plugin_debugger,plpgsql,plpgsql_check'


## Attention - SECURITY

Tracer prints content of variables or function arguments. For security definer function, this
content can hold security sensitive data. This is reason why tracer is disabled by default and should
be enabled only with super user rights `plpgsql_check.enable_tracer`.

# Pragma

You can configure plpgsql_check behaviour inside a checked function with "pragma" function. This
is a analogy of PL/SQL or ADA language of PRAGMA feature. PLpgSQL doesn't support PRAGMA, but
plpgsql_check detects function named `plpgsql_check_pragma` and takes options from the parameters of
this function. These plpgsql_check options are valid to the end of this group of statements.

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

The extension's `plpgsql_check_pragma` function is `VOLATILE` and returns integer 1.
Its tracer directives can change runtime tracing state; other directives are
interpreted by the checker.

In a database without the extension, an application can provide a no-op
compatibility stub:

    CREATE FUNCTION plpgsql_check_pragma(VARIADIC name text[])
    RETURNS int AS $$
    SELECT 1
    $$ LANGUAGE sql IMMUTABLE;

This stub ignores every directive, including tracer controls. Do not replace the
extension-owned implementation with it.

Using pragma function in declaration part of top block sets options on function level too.

    CREATE OR REPLACE FUNCTION test()
    RETURNS void AS $$
    DECLARE
      aux int := plpgsql_check_pragma('disable:extra_warnings');
      ...


Shorter syntax for pragma is supported too:

    CREATE OR REPLACE FUNCTION test()
    RETURNS void AS $$
    DECLARE r record;
    BEGIN
      PERFORM 'PRAGMA:TYPE:r (a int, b int)';
      PERFORM 'PRAGMA:TABLE: x (like pg_class)';
      ...

## Supported pragmas

* `echo:str` - print string (for testing). Inside string, there can be used "variables": @@id, @@name, @@signature

* `status:check`,`status:tracer`, `status:other_warnings`, `status:performance_warnings`, `status:extra_warnings`,`status:security_warnings`,
  `status:compatibility_warnings`, `status:constants_tracing`
  This outputs the current value (e.g. other_warnings enabled)

* `enable:check`,`enable:tracer`, `enable:other_warnings`, `enable:performance_warnings`, `enable:extra_warnings`,`enable:security_warnings`,
  `enable:compatibility_warnings`, `enable:constants_tracing`

* `disable:check`,`disable:tracer`, `disable:other_warnings`, `disable:performance_warnings`, `disable:extra_warnings`,`disable:security_warnings`,
  `disable:compatibility_warnings`, `disable:constants_tracing`
  This can be used to disable the Hint in returning from an anyelement function.  Just put the pragma before the RETURN statement.
  
* `type:varname typename` or `type:varname (fieldname type, ...)` - set type to variable of record type

* `table: name (column_name type, ...)` or `table: name (like tablename)` - create ephemeral temporary table (if you want to specify schema, then only `pg_temp` schema is allowed).
  The column list supports only a column name and a type (with optional type modifiers and array dimensions) - column constraints (`PRIMARY KEY`, `NOT NULL`, `DEFAULT`, `CHECK`) and `COLLATE` clauses are not supported here.

* `sequence: name` - create ephemeral temporary sequence

* `assert-schema: varname` - check-time assertion - ensure so schema specified by variable is valid

* `assert-table: [ varname_schema, ] , varname` - ensure so table name specified by variables (by constant tracing) is valid

* `assert-column: [varname_schema, ], varname_table , varname` - ensure so column specified by variables is valid

The historical spelling `status:constants_trancing` remains an alias for
`status:constants_tracing`; prefer the correctly spelled form.

# Table pragmas generator

<i>plpgsql_check</i> cannot verify queries over temporary tables that are created at runtime.
The pragma `table` solves this issue, but the column list must be written (and maintained)
manually there - it gets out of sync easily when the definition is changed. These pragmas
can be generated automatically by the function `plpgsql_make_pragma`. It scans the
function's body, and for every statement there that creates a temporary table it returns
one table pragma string.

For `CREATE TEMP TABLE ... AS SELECT|VALUES|TABLE` statements the names and types of
columns are derived from planning of the inner query (the query is never executed). For
`CREATE TEMP TABLE` statements (the column definition list, the `LIKE` clause, the
`OF type_name` clause, inheritance and partitioning) the statement is executed inside an
always rolled back subtransaction, and the pragma is derived from the structure of the
really created table - so serial or identity columns, inherited or LIKE-copied columns
are expanded by PostgreSQL itself. Column constraints (`PRIMARY KEY`, `NOT NULL`,
`DEFAULT`, `CHECK`) don't block the generation, but they are not carried into the
generated pragma - the pragma holds only column names and types, which is enough for
the static checks. In both cases no object survives the call, and repeated calls
return the same result.

Zero-column tables produce an empty column list, such as `table: target()`,
which is accepted by the table pragma parser.

    create table gtp_src(a int, b text);

    create or replace function gtp_f1()
    returns void as $$
    begin
      create temp table gtp_t1 as select a, b from gtp_src;
      insert into gtp_t1 values (10, 'hello');
    end;
    $$ language plpgsql;

Without the pragma the check fails on the missing temporary table:

    postgres=# select * from plpgsql_check_function('gtp_f1()');
                        plpgsql_check_function                    
    --------------------------------------------------------------
     error:42P01:4:SQL statement:relation "gtp_t1" does not exist
     Query: insert into gtp_t1 values (10, 'hello')
     --                 ^
    (3 rows)

    postgres=# select * from plpgsql_make_pragma('gtp_f1()');
     plpgsql_make_pragma 
    --------------------------------------
     table: gtp_t1(a integer, b text)
    (1 row)

Generated pragmas (the returned text can be manually edited when it is necessary) can be
passed to the option `pragmas` of the functions `plpgsql_check_function` and
`plpgsql_check_function_tb`. These pragmas are applied before the check is started:

    postgres=# select * from plpgsql_check_function('gtp_f1()',
                   pragmas => array(select plpgsql_make_pragma('gtp_f1()')));
     plpgsql_check_function 
    ------------------------
    (0 rows)

The inner query is not limited to a simple `FROM` clause - joins, subqueries, CTE and
recursive CTE queries are supported:

    create or replace function gtp_f7()
    returns void as $$
    begin
      create temp table gtp_rcte as
        with recursive r(n) as (values (1) union all select n + 1 from r where n < 10)
        select n from r;
    end;
    $$ language plpgsql;

    postgres=# select * from plpgsql_make_pragma('gtp_f7()');
     plpgsql_make_pragma 
    --------------------------------------
     table: gtp_rcte(n integer)
    (1 row)

More temporary tables can be processed. Every detected temporary table is registered
immediately, so a temporary table can reference other temporary tables created earlier
in the same function (in a simple `FROM` clause, in a join or in a subquery):

    create or replace function gtp_f15()
    returns void as $$
    begin
      create temp table gtp_c1 as select a, b from gtp_src;
      create temp table gtp_c2 as select * from gtp_c1 where a > 0;
      create temp table gtp_c3 as
        select gtp_c1.a, gtp_c2.b from gtp_c1 join gtp_c2 on gtp_c1.a = gtp_c2.a;
      create temp table gtp_c4 as
        select a from gtp_src where a in (select a from gtp_c1);
    end;
    $$ language plpgsql;

    postgres=# select * from plpgsql_make_pragma('gtp_f15()');
     plpgsql_make_pragma 
    --------------------------------------
     table: gtp_c1(a integer, b text)
     table: gtp_c2(a integer, b text)
     table: gtp_c3(a integer, b text)
     table: gtp_c4(a integer)
    (4 rows)

The statements are processed in the order of appearance in the function's body (nested
blocks, loops, IF branches and exception handlers are scanned too), and the pragmas are
returned in the same order without deduplication. Explicitly written table pragmas inside
the function's body are respected, so a temporary table created by dynamic SQL can be
declared manually, and the following statements can reference it.

The `CREATE TEMP TABLE` statement based forms are supported in all variants - with
storage parameters, access method, `ON COMMIT` or `IF NOT EXISTS` clauses, or with the
`pg_temp` schema qualification used without the `TEMP` keyword:

    create or replace function gtp_f20()
    returns void as $$
    begin
      create temp table gtp_x (id serial, v varchar(10));
      create temp table gtp_y (like gtp_x including all);
      insert into gtp_y(v) values ('hello');
    end;
    $$ language plpgsql;

    postgres=# select * from plpgsql_make_pragma('gtp_f20()');
                    plpgsql_make_pragma                
    ---------------------------------------------------
     table: gtp_x(id integer, v character varying(10))
     table: gtp_y(id integer, v character varying(10))
    (2 rows)

Only statically written commands are processed - dynamic SQL (`EXECUTE`), not temporary
tables and materialized views are ignored.

A name collision (usually the pattern `CREATE`, `DROP`, `CREATE` with the same name -
DROP statements are not executed by static scanning) is not an error. A warning is
raised, pragmas are returned for both definitions, and the following statements of the
function's body see the last definition like in runtime. `CREATE TEMP TABLE IF NOT EXISTS`
over an existing table returns the structure of the existing table (the existing table
wins like in runtime).

The function raises an error when:

* the query references a missing (and not temporary) relation - like
  `plpgsql_check_function`, the error (sqlstate `42P01`) is raised on the first failed
  statement,

* a temporary table should be created in a non-temporary schema (e.g.
  `create temporary table public.t1 as select 1`) - the error `cannot create temporary
  relation in non-temporary schema` (sqlstate `42P16`) is raised. This check is done for
  all forms of temporary tables (for the column definition list, `LIKE` and `OF type_name`
  forms too),

* more column names are explicitly specified than the query returns - the error
  `too many column names were specified`,

* the execution of a `CREATE TEMP TABLE` statement fails - e.g. a temporary partition
  of a persistent table, a foreign key from a temporary table to a persistent table, or
  the `LIKE` clause over a missing table. The error is the same like in runtime.

When the option `fatal_errors` is false, then failed statements are skipped (with a
warning), and the scanning continues.

Optional arguments of `plpgsql_make_pragma` are `relid` (like for
`plpgsql_check_function`, it is necessary for checking of trigger functions) and
`fatal_errors`.

# Update

This distribution ships the SQL installation script for extension version `2.10`,
but no `ALTER EXTENSION UPDATE` migration scripts. Replacing the library with a
compatible release using the same SQL extension version does not itself require
dropping the extension. If the SQL extension version changes, plan its recreation
and review dependent objects rather than blindly using `DROP ... CASCADE`.

After replacing a loaded library, reconnect sessions using it. If it is in
`shared_preload_libraries`, restart the postmaster before using the new library.

# Compilation

Use a C build toolchain and the server development headers/PGXS files for the
target PostgreSQL installation. Select that installation consistently with
`PG_CONFIG`:

    make PG_CONFIG=/path/to/postgresql/bin/pg_config
    make PG_CONFIG=/path/to/postgresql/bin/pg_config install

The install step needs filesystem permissions for PostgreSQL's extension
directories. Then run the regression suite against a running, disposable server:

    PGHOST=/path/to/socket PGPORT=5432 PGUSER=test_admin \
      make PG_CONFIG=/path/to/postgresql/bin/pg_config installcheck

The regression harness creates its test database and uses administrative SQL.
Do not point it at a production server. If the selected PostgreSQL installation
expects LLVM but its Clang toolchain is unavailable, add `with_llvm=no` to each
`make` invocation to omit LLVM bitcode.

Reconnect or restart as described under "Update" after installing a replacement
library. `installcheck` does not restart the target server; test separately with
and without `shared_preload_libraries = 'plpgsql_check'`.

## Compilation for PostgresPro

`plpgsql_check` requires some unpublished patches to be successfully compiled and used with PostgresPro. Use
`plpgsql_check` from PostgresPro repository.

## Compilation on Ubuntu

Depending on how PostgreSQL was built, compilation may also require `libicu-dev`
when that installation uses ICU:

    sudo apt install libicu-dev

## Compilation plpgsql_check on Windows

You can check precompiled dll libraries http://okbob.blogspot.cz/2015/02/plpgsqlcheck-is-available-for-microsoft.html,
http://okbob.blogspot.com/2023/10/compiled-dll-of-plpgsqlcheck-254-and.html

or compile by self:

1. Install a supported PostgreSQL version for Windows from https://www.enterprisedb.com
2. Install a compatible Microsoft Visual C++ build toolchain.
3. Read tutorial http://blog.2ndquadrant.com/compiling-postgresql-extensions-visual-studio-windows
4. Build plpgsql_check.dll
5. Copy `plpgsql_check.dll` to `PostgreSQL\<version>\lib`.
6. Copy `plpgsql_check.control` and `plpgsql_check--2.10.sql` to `PostgreSQL\<version>\share\extension`.

The DLL must match the PostgreSQL major version and architecture; use matching
extension library and SQL files.

## Meson build

1. `meson setup build`
2. `cd build`
3. `ninja`
4. `ninja install`
5. optionally `ninja bindist`

## Testing prerequisites

Use a supported PostgreSQL version (14 - 20). The `plpgsql_check_tablefunc`
regression test exercises XML output with `xpath`, so PostgreSQL must be built
with libxml support. The standalone ordinary-user review reproducers are
documented separately in [reproducers/README.md](reproducers/README.md).

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
