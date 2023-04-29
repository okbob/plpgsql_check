LOAD 'plpgsql';
CREATE EXTENSION  IF NOT EXISTS plpgsql_check;
set client_min_messages to notice;

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
drop procedure px1_dep(int);
drop function fx1_dep(int);