LOAD 'plpgsql';
CREATE EXTENSION  IF NOT EXISTS plpgsql_check;

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
drop table t1tab;

create or replace function fx()
returns t2 as $$
begin
  return (10,20,30)::t1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

drop table t1;

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

create or replace function test_lab()
returns void as $$
begin
    <<outer>>
    for a in 1..3 loop
    <<sub>>
    BEGIN
        <<inner>>
        for b in 8..9 loop
            if a=2 then
                continue sub;
            end if;
            raise notice '% %', a, b;
        end loop inner;
    END sub;
    end loop outer;
end;
$$ language plpgsql;

select test_lab();
select * from plpgsql_check_function('test_lab()', performance_warnings := true);

create or replace function test_lab()
returns void as $$
begin
  continue;
end;
$$ language plpgsql;

select test_lab();
select * from plpgsql_check_function('test_lab()', performance_warnings := true);

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
