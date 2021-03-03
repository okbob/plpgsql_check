LOAD 'plpgsql';
CREATE EXTENSION  IF NOT EXISTS plpgsql_check;
set client_min_messages to notice;

-- check event trigger function 
create or replace function f1() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger: % %', tg_event, tg_tag;
END
$$ language plpgsql;

select * from plpgsql_check_function_tb('f1()');

-- should fail
create or replace function f1() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger: % %', tg_event, tg_tagX;
END
$$ language plpgsql;

select * from plpgsql_check_function_tb('f1()');

drop function f1();


-- check event trigger function 
create or replace function f1() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger: % %', tg_event, tg_tag;
END
$$ language plpgsql;

select * from plpgsql_check_function('f1()');

-- should fail
create or replace function f1() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger: % %', tg_event, tg_tagX;
END
$$ language plpgsql;

select * from plpgsql_check_function('f1()');

drop function f1();

create table t1tab(a int, b int);

create or replace function f1()
returns setof t1tab as $$
begin
  return next (10,20);
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings => true);

create or replace function f1()
returns setof t1tab as $$
begin
  return next (10::numeric,20);
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings => true);

create or replace function f1()
returns setof t1tab as $$
declare a int; b int;
begin
  return next (a,b);
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings => true);

create or replace function f1()
returns setof t1tab as $$
declare a numeric; b int;
begin
  return next (a,b::numeric);
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings => true);

drop function f1();

create table t1(a int, b int);

create or replace function fx()
returns t2 as $$
begin
  return (10,20,30)::t1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings => true);

drop function fx();

drop table t1tab;
drop table t1;

create or replace function fx()
returns void as $$
begin
  assert exists(select * from foo);
  assert false, (select boo from boo limit 1);
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', fatal_errors => false);

create or replace function ml_trg()
returns trigger as $$
#option dump
declare
begin
  if TG_OP = 'INSERT' then
    if NEW.status_from IS NULL then
      begin
        -- performance issue only
        select status into NEW.status_from
           from pa
          where pa_id = NEW.pa_id;
        -- nonexist target value
        select status into NEW.status_from_xxx
           from pa
          where pa_id = NEW.pa_id;
      exception
        when DATA_EXCEPTION then
          new.status_from := 'DE';
      end;
    end if;
  end if;
  if TG_OP = 'DELETE' then return OLD; else return NEW; end if;
exception
  when OTHERS then
    NULL;
    if TG_OP = 'DELETE' then return OLD; else return NEW; end if;
end;
$$ language plpgsql;

select * from plpgsql_check_function('ml_trg()', 'ml', performance_warnings := true);

create or replace function fx2()
returns void as $$
declare _pa pa;
begin
  select pa.id into _pa.id from pa limit 1;
  select pa.pa_id into _pa.pa_id from pa limit 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx2()', performance_warnings := true);

drop function fx2();

create or replace function fx2()
returns void as $$
declare _pa pa;
begin
  _pa.id := (select pa.id from pa limit 1);
  _pa.pa_id := (select pa.pa_id from pa limit 1);
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx2()', performance_warnings := true);

drop function fx2();

create type _exception_type as (
  state text,
  message text,
  detail text);

create or replace function f1()
returns void as $$
declare
  _exception record;
begin
  _exception := NULL::_exception_type;
exception when others then
  get stacked diagnostics
        _exception.state = RETURNED_SQLSTATE,
        _exception.message = MESSAGE_TEXT,
        _exception.detail = PG_EXCEPTION_DETAIL,
        _exception.hint = PG_EXCEPTION_HINT;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function_tb('f1()');

create or replace function f1()
returns void as $$
declare
  _exception _exception_type;
begin
  _exception := NULL::_exception_type;
exception when others then
  get stacked diagnostics
        _exception.state = RETURNED_SQLSTATE,
        _exception.message = MESSAGE_TEXT,
        _exception.detail = PG_EXCEPTION_DETAIL;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function_tb('f1()');

drop function f1();

drop type _exception_type;

create type _exception_type as (
  state text,
  message text,
  detail text);

create or replace function f1()
returns void as $$
declare
  _exception record;
begin
  _exception := NULL::_exception_type;
exception when others then
  get stacked diagnostics
        _exception.state = RETURNED_SQLSTATE,
        _exception.message = MESSAGE_TEXT,
        _exception.detail = PG_EXCEPTION_DETAIL,
        _exception.hint = PG_EXCEPTION_HINT;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function('f1()');

drop function f1();
drop type _exception_type;

create or replace procedure proc(a int)
as $$
begin
end;
$$ language plpgsql;

call proc(10);

select * from plpgsql_check_function('proc(int)');

create or replace procedure testproc()
as $$
begin
  call proc(10);
end;
$$ language plpgsql;

call testproc();

select * from plpgsql_check_function('testproc()');

-- should to fail
create or replace procedure testproc()
as $$
begin
  call proc((select count(*) from pg_class));
end;
$$ language plpgsql;

call testproc();

select * from plpgsql_check_function('testproc()');

drop procedure proc(int);

create procedure proc(in a int, inout b int, in c int)
as $$
begin
end;
$$ language plpgsql;

select * from plpgsql_check_function('proc(int,int, int)');

create or replace procedure proc(in a int, inout b int, in c int)
as $$
begin
  b := a + c;
end;
$$ language plpgsql;

select * from plpgsql_check_function('proc(int,int, int)');

create or replace procedure testproc()
as $$
declare r int;
begin
  call proc(10, r, 20);
end;
$$ language plpgsql;

call testproc();

select * from plpgsql_check_function('testproc()');

-- should to fail
create or replace procedure testproc()
as $$
declare r int;
begin
  call proc(10, r + 10, 20);
end;
$$ language plpgsql;

call testproc();

select * from plpgsql_check_function('testproc()');

create or replace procedure testproc(inout r int)
as $$
begin
  call proc(10, r, 20);
end;
$$ language plpgsql;

call testproc(10);

select * from plpgsql_check_function('testproc(int)');

drop procedure testproc(int);

-- should to raise warnings
create or replace procedure testproc2(in p1 int, inout p2 int, in p3 int, inout p4 int)
as $$
begin
  raise notice '% %', p1, p3;
end;
$$ language plpgsql;

select * from plpgsql_check_function('testproc2');

drop procedure testproc2;

-- should be ok
create or replace procedure testproc3(in p1 int, inout p2 int, in p3 int, inout p4 int)
as $$
begin
  p2 := p1;
  p4 := p3;
end;
$$ language plpgsql;

select * from plpgsql_check_function('testproc3');

drop procedure testproc3;

-- should be moved to generic test
create or replace function dyntest()
returns void as $$
begin
  execute 'drop table if exists xxx; create table xxx(a int)';
end;
$$ language plpgsql;

-- should be ok
select * from plpgsql_check_function('dyntest');

create or replace function dyntest()
returns void as $$
declare x int;
begin
  execute 'drop table if exists xxx; create table xxx(a int)' into x;
end;
$$ language plpgsql;

-- should to report error
select * from plpgsql_check_function('dyntest');

drop function dyntest();

/*
 * These function's cannot be executed in Postgres 9.5, because
 * Postgres there doesn't support plpgsql functions with record
 * type arguments.
 */
create or replace function df1(anyelement)
returns anyelement as $$
begin
  return $1;
end;
$$ language plpgsql;

create or replace function df2(anyelement, jsonb)
returns anyelement as $$
begin
  return $1;
end;
$$ language plpgsql;

create or replace function t1()
returns void as $$
declare
  r record;
begin
  r := df1(r);
end;
$$ language plpgsql;

select * from plpgsql_check_function('t1()');

create or replace function t1()
returns void as $$
declare
  r record;
begin
  r := df2(r, '{}');
end;
$$ language plpgsql;

select * from plpgsql_check_function('t1()');

create or replace function t1()
returns void as $$
declare
  r1 record;
  r2 record;
begin
  select 10 as a, 20 as b into r1;
  r2 := df1(r1);
  raise notice '%', r2.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('t1()');

create or replace function t1()
returns void as $$
declare
  r1 record;
  r2 record;
begin
  select 10 as a, 20 as b into r1;
  r2 := df2(r1, '{}');
  raise notice '%', r2.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('t1()');


create or replace function df1(anyelement)
returns anyelement as $$ select $1 $$ language sql;

create or replace function df22(jsonb, anyelement)
returns anyelement as $$ select $2; $$ language sql;

create or replace function t1()
returns void as $$
declare
  r1 record;
  r2 record;
begin
  select 10 as a, 20 as b into r1;
  r2 := df1(r1);
  raise notice '%', r2.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('t1()');

create or replace function t1()
returns void as $$
declare
  r1 record;
  r2 record;
begin
  select 10 as a, 20 as b into r1;
  r2 := df22('{}', r1);
  raise notice '%', r2.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('t1()');

drop function df1(anyelement);
drop function df2(anyelement, jsonb);
drop function df22(jsonb, anyelement);
drop function t1();
