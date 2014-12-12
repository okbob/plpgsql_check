LOAD 'plpgsql';
CREATE EXTENSION  IF NOT EXISTS plpgsql_check;

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

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns setof t1tab as $$
begin
  return next (10::numeric,20);
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns setof t1tab as $$
declare a int; b int;
begin
  return next (a,b);
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns setof t1tab as $$
declare a numeric; b int;
begin
  return next (a,b::numeric);
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

drop function f1();

create table t1(a int, b int);

create or replace function fx()
returns t2 as $$
begin
  return (10,20,30)::t1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

drop table t1tab;
drop table t1;
