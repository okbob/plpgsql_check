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
