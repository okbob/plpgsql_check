set client_min_messages to warning;
create extension if not exists plpgsql_check;
set client_min_messages to notice;

--
-- checks done before the function is checked
--

-- the checked routine must be written in plpgsql
select * from plpgsql_check_function('upper(text)');
select * from plpgsql_check_function('sum(int)');

-- a routine with a pseudotype result is rejected sooner, the result type
-- is read before the language of the routine is validated
select * from plpgsql_check_function('textout(text)');

-- record, void and the polymorphic types are pseudotypes too, but they
-- are allowed
create function cat_f1() returns void as $$
begin
end;
$$ language plpgsql;

create function cat_f2(a anyelement) returns anyelement as $$
begin
  return a;
end;
$$ language plpgsql;

create function cat_f3() returns record as $$
declare r record;
begin
  select 1 as a, 2 as b into r;
  return r;
end;
$$ language plpgsql;

select * from plpgsql_check_function('cat_f1');
select * from plpgsql_check_function('cat_f2(anyelement)');
select * from plpgsql_check_function('cat_f3');

--
-- the relation of a trigger
--

create table cat_tab1(a int, b int);

create function cat_trg1() returns trigger as $$
begin
  return new;
end;
$$ language plpgsql;

create function cat_evtrg1() returns event_trigger as $$
begin
end;
$$ language plpgsql;

-- a dml trigger cannot be checked without the triggering relation
select * from plpgsql_check_function('cat_trg1');
select * from plpgsql_check_function('cat_trg1', relid => 'cat_tab1'::regclass);

-- an event trigger has no triggering relation
select * from plpgsql_check_function('cat_evtrg1', relid => 'cat_tab1'::regclass);
select * from plpgsql_check_function('cat_evtrg1');

-- a plain function has no triggering relation either
select * from plpgsql_check_function('cat_f1', relid => 'cat_tab1'::regclass);

-- the profiler does not check the triggering relation, it only reads the
-- collected statistics
select * from plpgsql_profiler_function_tb('cat_trg1');

--
-- the pragma function is searched by name, and only the function from the
-- schema of the extension is used
--

create schema cat_ns;

create function cat_ns.plpgsql_check_pragma(int) returns int as $$
  select 1;
$$ language sql;

create function cat_f4() returns int as $$
declare r record;
begin
  perform plpgsql_check_pragma('type: r (a int)');
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('cat_f4');

drop function cat_ns.plpgsql_check_pragma(int);
drop schema cat_ns;

drop function cat_f1();
drop function cat_f2(anyelement);
drop function cat_f3();
drop function cat_f4();
drop function cat_evtrg1();
drop trigger if exists cat_trg1 on cat_tab1;
drop function cat_trg1();
drop table cat_tab1;
