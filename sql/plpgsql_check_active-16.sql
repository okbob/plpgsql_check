LOAD 'plpgsql';
CREATE EXTENSION  IF NOT EXISTS plpgsql_check;
set client_min_messages to notice;

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

/*
 * Test pragma
 */
create or replace function test_pragma()
 returns void
 language plpgsql
as $$
declare r record;
begin
  perform plpgsql_check_pragma('disable:check');
  raise notice '%', r.y;
  perform plpgsql_check_pragma('enable:check');
  select 10 as a, 20 as b into r;
  raise notice '%', r.a;
  raise notice '%', r.x;
end;
$$;

select * from plpgsql_check_function('test_pragma');

create or replace function test_pragma()
 returns void
 language plpgsql
as $$
declare r record;
begin
  if false then
    -- check is disabled just for if body
    perform plpgsql_check_pragma('disable:check');
    raise notice '%', r.y;
  end if;
  select 10 as a, 20 as b into r;
  raise notice '%', r.a;
  raise notice '%', r.x;
end;
$$;

select * from plpgsql_check_function('test_pragma');

drop function test_pragma();

create or replace function nested_trace_test(a int)
returns int as $$
begin
  return a + 1;
end;
$$ language plpgsql;

create or replace function trace_test(b int)
returns int as $$
declare r int default 0;
begin
  for i in 1..b
  loop
    r := nested_trace_test(r);
  end loop;
  return r;
end;
$$ language plpgsql;

select trace_test(3);

set plpgsql_check.enable_tracer to on;
set plpgsql_check.tracer to on;
set plpgsql_check.tracer_test_mode = true;

select trace_test(3);

set plpgsql_check.tracer_verbosity TO verbose;

select trace_test(3);

create or replace function trace_test(b int)
returns int as $$
declare r int default 0;
begin
  for i in 1..b
  loop
    perform plpgsql_check_pragma('disable:tracer');
    r := nested_trace_test(r);
  end loop;
  return r;
end;
$$ language plpgsql;

select trace_test(3);

create or replace function nested_trace_test(a int)
returns int as $$
begin
  perform plpgsql_check_pragma('enable:tracer');
  return a + 1;
end;
$$ language plpgsql;

select trace_test(3);

drop function trace_test(int);
drop function nested_trace_test(int);

create or replace function trace_test(int)
returns int as $$
declare r int default 0;
begin
  for i in 1..$1 loop
    r := r + 1;
  end loop;
  r := r + 10;
  return r;
end;
$$ language plpgsql;

select trace_test(4);

create or replace function trace_test(int)
returns int as $$
declare r int default 0;
begin
  for i in 1..$1 loop
    perform plpgsql_check_pragma('disable:tracer');
    r := r + 1;
  end loop;
  r := r + 10;
  return r;
end;
$$ language plpgsql;

select trace_test(4);

create or replace function trace_test(int)
returns int as $$
declare r int default 0;
begin
  perform plpgsql_check_pragma('disable:tracer');

  for i in 1..$1 loop
    r := r + 1;
  end loop;

  perform plpgsql_check_pragma('enable:tracer');

  r := r + 10;
  return r;
end;
$$ language plpgsql;

select trace_test(4);

drop function trace_test(int);

create or replace function fxtest()
returns void as $$
declare
  v_sqlstate text;
  v_message text;
  v_context text;
begin
  get stacked diagnostics
    v_sqlstate = returned_sqlstate,
    v_message = message_text,
    v_context = pg_exception_context;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fxtest');

drop function fxtest();

create or replace procedure prtest()
as $$
begin
  commit;
end;
$$ language plpgsql;

select * from plpgsql_check_function('prtest'); --ok

create or replace procedure prtest()
as $$
begin
  begin
    begin
      commit;
    end;
  end;
exception when others then
  raise;
end;
$$ language plpgsql;

select * from plpgsql_check_function('prtest'); --error

create or replace procedure prtest()
as $$
begin
  raise exception 'error';
exception when others then
  begin
    begin
      commit;
    end;
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('prtest'); --ok

drop procedure prtest();

create function return_constant_refcursor() returns refcursor as $$
declare
    rc constant refcursor;
begin
    open rc for select a from rc_test;
    return rc;
end
$$ language plpgsql;

create table rc_test(a int);

select * from plpgsql_check_function('return_constant_refcursor');

drop table rc_test;
drop function return_constant_refcursor();

create procedure p1(a int, out b int)
as $$
begin
  b := a + 10;
end;
$$ language plpgsql;

create function f1()
returns void as $$
declare b constant int;
begin
  call p1(10, b);
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1');

drop function f1();
drop procedure p1(int, int);

create or replace function f1()
returns int as $$
declare c constant int default 100;
begin
  return c;
end;
$$ language plpgsql;

-- should be ok
select * from plpgsql_check_function('f1');

drop function f1();

-- do not raise false warning
create or replace function test_function()
returns text as $$
declare s text;
begin
  get diagnostics s = PG_CONTEXT;
  return s;
end;
$$ language plpgsql;

create or replace procedure test_procedure()
as $$
begin
  null;
end;
$$ language plpgsql;

-- should be without any warnings
select * from plpgsql_check_function('test_function', performance_warnings=>true);
select * from plpgsql_check_function('test_procedure', performance_warnings=>true);

drop function test_function();
drop procedure test_procedure();

-- detect dependecy in CALL statement
create or replace function fx1_dep(int)
returns int as $$
begin
  return $1;
end;
$$ language plpgsql;

create or replace procedure px1_dep(int)
as $$
begin
end;
$$ language plpgsql;

create or replace function test_function()
returns void as $$
begin
  call px1_dep(fx1_dep(10));
end;
$$ language plpgsql;

select type, schema, name, params from plpgsql_show_dependency_tb('test_function');

drop function test_function();
drop procedure px1_dep();
drop function fx1_dep(int);
