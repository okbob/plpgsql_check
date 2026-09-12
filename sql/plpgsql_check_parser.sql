set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- Tests of the parser used for the function names passed to the
-- plpgsql_check functions, and of the tokenizer and the parsers of
-- the arguments of the pragmas.
--

--
-- function name or signature
--
create function pr_fx(a int)
returns int as $$
begin
  return a;
end;
$$ language plpgsql;

create function "pr FX"(a int)
returns int as $$
begin
  return a;
end;
$$ language plpgsql;

create function "pr""q"(a int)
returns int as $$
begin
  return a;
end;
$$ language plpgsql;

-- an overloaded function cannot be addressed by a name
create function pr_over(a int) returns int as $$ begin return a; end; $$ language plpgsql;
create function pr_over(a text) returns text as $$ begin return a; end; $$ language plpgsql;

-- the errors are raised by the parser of the name, so they are catched
-- and printed as a value
create function pr_name(fnname text)
returns text as $$
begin
  perform plpgsql_check_function(fnname);
  return 'ok';
exception when others then
  return sqlstate || ' ' || sqlerrm;
end;
$$ language plpgsql;

-- plain name, with and without the leading and trailing spaces
select pr_name('pr_fx');
select pr_name('   pr_fx   ');

-- quoted name, a doubled quote inside a quoted name
select pr_name('"pr FX"');
select pr_name('"pr""q"');

-- qualified names
select pr_name('public.pr_fx');
select pr_name('public."pr FX"');
select pr_name('"public" . "pr FX"');

-- signatures; the parser stops on the left parenthesis and the rest
-- is processed by the regprocedure input function
select pr_name('pr_fx(int)');
select pr_name('public.pr_fx(integer)');
select pr_name('"pr FX"(int)');

-- the name is not an identifier
select pr_name('"pr FX');
select pr_name('""');
select pr_name('.pr_fx');
select pr_name('public.');
select pr_name('public..pr_fx');
select pr_name('1234');
select pr_name('pr_fx pr_fx');

-- the name is an identifier, but the function is not there or the
-- name is not unique
select pr_name('pr_missing');
select pr_name('pr_over');

drop function pr_name(text);
drop function pr_over(text);
drop function pr_over(int);
drop function "pr""q"(int);
drop function "pr FX"(int);
drop function pr_fx(int);

--
-- tokenizer
--
-- The tokenizer is shared by all the pragmas and by the in-comment
-- options. The echo option is used here, because it is the only place
-- where a token of any type is accepted and printed back, so the result
-- of the tokenization is visible.
create function pr_tokens()
returns void as $func$
-- @plpgsql_check_options: echo = 'a string with a '' quote'
-- @plpgsql_check_options: echo = "a quoted identifier with a "" quote"
-- @plpgsql_check_options: echo = AnIdentifier
-- @plpgsql_check_options: echo = 3.14
begin
end;
$func$ language plpgsql;

select * from plpgsql_check_function('pr_tokens');

drop function pr_tokens();

--
-- pragma "settype"
--
create type pr_ctype as (a int, b int);
create table pr_tab(a int, b int);

create function pr_settype()
returns void as $$
<<lbl>>
declare
  r record;
  n int;
begin
  -- a plain, a qualified and a quoted type name
  perform plpgsql_check_pragma('type: r pr_ctype');
  perform plpgsql_check_pragma('type: lbl.r public.pr_ctype');
  perform plpgsql_check_pragma('type: "lbl"."r" public."pr_ctype"');
  -- a table can be used as a type too
  perform plpgsql_check_pragma('type: r pr_tab');
  raise notice '%', r.a;
  raise notice '%', n;
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_settype');

-- errors of the "settype" pragma
create or replace function pr_settype()
returns void as $$
declare
  r record;
  n int;
begin
  perform plpgsql_check_pragma('type: missing_var pr_ctype');
  perform plpgsql_check_pragma('type: n pr_ctype');
  perform plpgsql_check_pragma('type: r');
  perform plpgsql_check_pragma('type: 1 pr_ctype');
  perform plpgsql_check_pragma('type: r pr_ctype x');
  raise notice '%', r.a;
  raise notice '%', n;
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_settype');

drop function pr_settype();

--
-- the type parser
--
-- The type used by the "settype" and "table" pragmas is parsed by
-- plpgsql_check itself, so all the accepted forms are checked here.
create function pr_type()
returns void as $$
declare r record;
begin
  -- a multiword type name, a type modifier, a list of type modifiers
  perform plpgsql_check_pragma('type: r (a double precision, b varchar(10), c numeric(10,2))');
  -- an array, with and without a dimension
  perform plpgsql_check_pragma('type: r (a int[], b int[3], c pg_catalog.varchar[])');
  -- a composite type copied from a table
  perform plpgsql_check_pragma('type: r (like pr_tab)');
  raise notice '%', r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_type');

-- errors of the type parser
create or replace function pr_type()
returns void as $$
declare r record;
begin
  perform plpgsql_check_pragma('type: r (like int)');
  perform plpgsql_check_pragma('type: r (like pr_tab');
  perform plpgsql_check_pragma('type: r (1 int)');
  perform plpgsql_check_pragma('type: r (a int b int)');
  perform plpgsql_check_pragma('type: r (a numeric(x))');
  perform plpgsql_check_pragma('type: r (a numeric(10');
  perform plpgsql_check_pragma('type: r (a numeric(10 2))');
  perform plpgsql_check_pragma('type: r (a int[)');
  perform plpgsql_check_pragma('type: r (a int[');
  perform plpgsql_check_pragma('type: r (a "unclosed)');
  raise notice '%', r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_type');

drop function pr_type();

--
-- pragma "table"
--
create function pr_table()
returns void as $$
declare r record;
begin
  perform plpgsql_check_pragma('table: pr_tt1(a int, b int)');
  perform plpgsql_check_pragma('table: pg_temp.pr_tt2(a int)');
  perform plpgsql_check_pragma('table: pg_temp."pr""tt3"(a int)');
  select * from pr_tt1 into r;
  select * from pr_tt2 into r;
  raise notice '%', r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_table');

-- errors of the "table" pragma
create or replace function pr_table()
returns void as $$
begin
  perform plpgsql_check_pragma('table: public.pr_tt4(a int)');
  perform plpgsql_check_pragma('table: 1(a int)');
  perform plpgsql_check_pragma('table: pg_temp.1(a int)');
  perform plpgsql_check_pragma('table: pr_tt5');
  perform plpgsql_check_pragma('table: pr_tt6(a int) x');
  perform plpgsql_check_pragma('table: "pr_tt7(a int)');
  -- a composite type is not allowed as a type of a column of a table
  -- created by this pragma
  perform plpgsql_check_pragma('table: pr_tt8(a (b int))');
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_table');

drop function pr_table();

--
-- pragma "sequence"
--
create function pr_sequence()
returns void as $$
begin
  perform plpgsql_check_pragma('sequence: pr_sq1');
  perform plpgsql_check_pragma('sequence: pg_temp.pr_sq2');
  perform plpgsql_check_pragma('sequence: pg_temp."pr""sq3"');
  perform nextval('pg_temp.pr_sq1');
  perform nextval('pg_temp.pr_sq2');
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_sequence');

-- errors of the "sequence" pragma
create or replace function pr_sequence()
returns void as $$
begin
  perform plpgsql_check_pragma('sequence: public.pr_sq4');
  perform plpgsql_check_pragma('sequence: 1');
  perform plpgsql_check_pragma('sequence: pg_temp.1');
  perform plpgsql_check_pragma('sequence: pr_sq5 x y');
  perform plpgsql_check_pragma('sequence: "pr_sq6');
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_sequence');

drop function pr_sequence();

--
-- pragmas "assert-schema", "assert-table" and "assert-column"
--
create function pr_assert()
returns void as $$
<<lbl>>
declare
  v_schema varchar default 'public';
  v_table varchar default 'pr_tab';
  v_column varchar default 'a';
begin
  raise notice '%', format('%I.%I.%I', v_schema, v_table, v_column);
  perform 'pragma:assert-schema: v_schema';
  perform 'pragma:assert-table: v_schema, v_table';
  perform 'pragma:assert-table: v_table';
  perform 'pragma:assert-column: v_schema, v_table, v_column';
  perform 'pragma:assert-column: v_table, v_column';
  -- a variable can be addressed by a qualified name too
  perform 'pragma:assert-schema: lbl.v_schema';
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_assert');

-- the asserted objects do not exist
create or replace function pr_assert()
returns void as $$
declare
  v_schema varchar default 'pr_missing_schema';
  v_table varchar default 'pr_missing_table';
  v_column varchar default 'pr_missing_column';
begin
  perform 'pragma:assert-table: v_table';
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_assert');

create or replace function pr_assert()
returns void as $$
declare
  v_schema varchar default 'public';
  v_table varchar default 'pr_tab';
  v_column varchar default 'pr_missing_column';
begin
  perform 'pragma:assert-column: v_schema, v_table, v_column';
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_assert');

-- errors of the "assert" pragmas; they are reported as warnings and the
-- check continues
create or replace function pr_assert()
returns void as $$
<<lbl>>
declare
  v_schema varchar default 'public';
  v_table varchar default 'pr_tab';
  v_column varchar default 'a';
  v_var varchar;
begin
  -- an unknown variable, a name with too many parts
  perform 'pragma:assert-schema: v_missing';
  perform 'pragma:assert-schema: a.b.c.d';
  -- a variable without an assigned constant
  perform 'pragma:assert-schema: v_var';
  -- a missing comma between the variables
  perform 'pragma:assert-table: v_schema v_table';
  -- too many and too few variables; the parser reads three names at
  -- most, so a fourth one is reported as an unexpected text
  perform 'pragma:assert-schema: v_schema, v_table';
  perform 'pragma:assert-table: v_schema, v_table, v_column';
  perform 'pragma:assert-column: v_schema, v_table, v_column, v_column';
  perform 'pragma:assert-column: v_table';
  -- a syntax error in the name of the variable
  perform 'pragma:assert-schema: 1';
end;
$$ language plpgsql;

select * from plpgsql_check_function('pr_assert');

drop function pr_assert();

drop table pr_tab;
drop type pr_ctype;
