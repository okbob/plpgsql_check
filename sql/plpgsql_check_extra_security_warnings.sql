load 'plpgsql_check';

set client_min_messages to warning;
create extension if not exists plpgsql_check;
set client_min_messages to notice;

create or replace function fx_extra_security()
returns int as $$
begin
  return (select count(relname) + 1 from pg_class);
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 1);

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

create or replace function fx_extra_security()
returns int as $$
begin
  return (select pg_catalog.count(relname) + 1 from pg_catalog.pg_class);
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 1);

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 3);

create or replace function fx_extra_security()
returns int as $$
begin
  return (select pg_catalog.count(c.relname) + 1 from pg_catalog.pg_class c);
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 3);

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 4);

create or replace function fx_extra_security()
returns int as $$
begin
  return (select pg_catalog.count(c.relname) OPERATOR(pg_catalog.+) 1 from pg_catalog.pg_class c);
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 4);

drop function fx_extra_security();

create or replace function fx_extra_security()
returns text as $$
begin
  return 'hello'::text;
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

create or replace function fx_extra_security()
returns text as $$
begin
  return 'hello'::pg_catalog.text;
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 5);

drop function fx_extra_security();


-- names of CTE, transition tables and relations in FOR UPDATE OF
-- clause cannot be qualified, so they should not be reported
create table fx_extra_security_t(a int);

create or replace function fx_extra_security()
returns int as $$
declare r int;
begin
  with x as (select 1 as a), y as (select x.a from x)
    select y.a into r from y where y.a in (select x.a from x);

  with recursive z(n) as (select 1 union all select z.n + 1 from z where z.n < 3)
    select z.n into r from z order by z.n desc limit 1;

  select s.a into r from (with w as (select 1 as a) select w.a from w) s;

  select t.a into r from public.fx_extra_security_t t for update of t;

  with u as (update public.fx_extra_security_t set a = 1 returning a)
    insert into public.fx_extra_security_t select u.a from u;

  return r;
end;
$$ language plpgsql
set search_path = pg_catalog, public;

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

-- but tables should be reported, although there is a CTE with same name
create or replace function fx_extra_security()
returns int as $$
declare r int;
begin
  -- CTE doesn't hide the table for target of DML statement
  with fx_extra_security_t as (select 1 as a)
    delete from fx_extra_security_t where a in (select c.a from fx_extra_security_t c);

  -- not recursive CTE cannot see following CTE
  with x as (select y.a from fx_extra_security_t y), fx_extra_security_t as (select 1 as a)
    select x.a into r from x;

  -- CTE is not visible outside of the statement
  select s.a into r
    from (with fx_extra_security_t as (select 1 as a) select c.a from fx_extra_security_t c) s,
         fx_extra_security_t;

  return r;
end;
$$ language plpgsql
set search_path = pg_catalog, public;

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

drop function fx_extra_security();

create or replace function fx_extra_security_trg()
returns trigger as $$
declare n int;
begin
  select pg_catalog.count(*) into n from newtab;
  raise notice 'rows: %', n;
  return null;
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security_trg()', 'fx_extra_security_t',
                                     newtable => 'newtab',
                                     extra_security_warnings_level => 2);

-- passive mode, transition table is registered by executor
create trigger fx_extra_security_tg after insert on fx_extra_security_t
  referencing new table as newtab
  for each statement execute function fx_extra_security_trg();

set plpgsql_check.mode = 'every_start';
set plpgsql_check.extra_security_warnings_level = 2;

insert into fx_extra_security_t values(1);

reset plpgsql_check.extra_security_warnings_level;
reset plpgsql_check.mode;

drop table fx_extra_security_t;
drop function fx_extra_security_trg();

-- PL/pgSQL variables and star are not columns
create table fx_extra_security_t(a int, b int);

create or replace function fx_extra_security(p int)
returns int as $$
declare
  r int;
  rec record;
begin
  select t.a into r from public.fx_extra_security_t t where t.b = p;
  if found and r > 10 then
    return 1;
  end if;

  for rec in select * from public.fx_extra_security_t t
  loop
    raise notice '%', rec.a;
  end loop;

  -- should be reported
  select b into r from public.fx_extra_security_t t;

  -- should be reported, variables are not visible in dynamic SQL
  execute 'select r from (select 1 as r) s';

  return r;
end;
$$ language plpgsql
set search_path = pg_catalog;

select * from plpgsql_check_function('fx_extra_security(int)', extra_security_warnings_level => 3);

-- variable can be a column, when variable_conflict is use_column
create or replace function fx_extra_security(p int)
returns int as $$
#variable_conflict use_column
declare r int;
begin
  select t.a into r from public.fx_extra_security_t t where t.b = p;
  return r;
end;
$$ language plpgsql
set search_path = pg_catalog;

select * from plpgsql_check_function('fx_extra_security(int)', extra_security_warnings_level => 3);

drop function fx_extra_security(int);
drop table fx_extra_security_t;

-- assignments and CALL statements should be checked too
create procedure fx_extra_security_proc(a int)
as $$
begin
  raise notice '%', a;
end;
$$ language plpgsql;

create or replace function fx_extra_security()
returns int as $$
declare i int;
begin
  i := length('abc');
  i := (select max(c.oid::int) from pg_class c);
  i := '10'::int8;
  call fx_extra_security_proc(abs(i));

  -- qualified identifiers should not be reported
  i := pg_catalog.length('abc');
  i := (select pg_catalog.max(c.oid::int) from pg_catalog.pg_class c);
  i := '10'::pg_catalog.int8;
  call public.fx_extra_security_proc(pg_catalog.abs(i));

  return i;
end;
$$ language plpgsql
set search_path = pg_catalog, public;

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

drop function fx_extra_security();
drop procedure fx_extra_security_proc(int);

-- operators of ANY/ALL expressions, sublinks and ORDER BY USING
-- should be qualified too
create or replace function fx_extra_security(arr int[])
returns boolean as $$
begin
  if 1 = any (fx_extra_security.arr) then return true; end if;
  if 2 <> all (fx_extra_security.arr) then return true; end if;
  if 0 < any (select u.u from pg_catalog.unnest(fx_extra_security.arr) u) then return true; end if;
  return (select u.u from pg_catalog.unnest(fx_extra_security.arr) u order by u.u using < limit 1) is not null;
end;
$$ language plpgsql
set search_path = pg_catalog;

select * from plpgsql_check_function('fx_extra_security(int[])', extra_security_warnings_level => 4);

-- qualified operators and operators implied by syntax should not be reported
create or replace function fx_extra_security(arr int[])
returns boolean as $$
begin
  if 1 operator(pg_catalog.=) any (fx_extra_security.arr) then return true; end if;
  if 2 operator(pg_catalog.<>) all (fx_extra_security.arr) then return true; end if;
  if 0 operator(pg_catalog.<) any (select u.u from pg_catalog.unnest(fx_extra_security.arr) u) then return true; end if;
  if 1 in (select u.u from pg_catalog.unnest(fx_extra_security.arr) u) then return true; end if;
  if 1 in (2, 3) or 1 between 2 and 3 or 1 is distinct from 2 then return true; end if;
  if 'a'::pg_catalog.text like 'b' then return true; end if;
  return (select u.u from pg_catalog.unnest(fx_extra_security.arr) u order by u.u using operator(pg_catalog.<) limit 1) is not null;
end;
$$ language plpgsql
set search_path = pg_catalog;

select * from plpgsql_check_function('fx_extra_security(int[])', extra_security_warnings_level => 4);

drop function fx_extra_security(int[]);
