LOAD 'plpgsql';
CREATE EXTENSION IF NOT EXISTS plpgsql_check;

create table t1(a int, b int);

create function f1()
returns void as $$
begin
  if false then
    update t1 set c = 30;
  end if;
  if false then
    raise notice '% %', r.c;
  end if;
end;
$$ language plpgsql;

select f1();
select * from plpgsql_check_function_tb('f1()', fatal_errors := true);
select * from plpgsql_check_function_tb('f1()', fatal_errors := false);

select * from plpgsql_check_function_tb('f1()');

select f1();

drop function f1();

create or replace function f1()
returns void as $$
begin
  if false then
    raise notice '%', 1, 2;
  end if;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function_tb('f1()');

select f1();

drop function f1();

create or replace function f1()
returns void as $$
begin
  if false then
    raise notice '% %';
  end if;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function_tb('f1()');

select f1();

drop function f1();

