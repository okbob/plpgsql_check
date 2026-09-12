set client_min_messages to warning;
create extension if not exists plpgsql_check;
set client_min_messages to notice;

--
-- the polymorphic arguments are replaced by the types passed as the
-- arguments of the check function
--

create function cf_f1(a anynonarray) returns text as $$
begin
  return a::text;
end;
$$ language plpgsql;

-- anynonarray uses the anyelememttype option (the name of the argument
-- has a typo), which must not be an array
select * from plpgsql_check_function('cf_f1(anynonarray)');
select * from plpgsql_check_function('cf_f1(anynonarray)'::regprocedure, anyelememttype => 'date');
select * from plpgsql_check_function('cf_f1(anynonarray)'::regprocedure, anyelememttype => 'int[]');

create type cf_enum as enum ('a', 'b');

create function cf_f2(a anyenum) returns text as $$
begin
  return a::text;
end;
$$ language plpgsql;

-- anyenum requires the anyenumtype option, and the passed type must be
-- an enum
select * from plpgsql_check_function('cf_f2(anyenum)');
select * from plpgsql_check_function('cf_f2(anyenum)'::regprocedure, anyenumtype => 'int');
select * from plpgsql_check_function('cf_f2(anyenum)'::regprocedure, anyenumtype => 'cf_enum');

create function cf_f3(a anycompatiblenonarray, b anycompatible) returns text as $$
begin
  return a::text || b::text;
end;
$$ language plpgsql;

-- anycompatiblenonarray uses the anycompatibletype option, which must
-- not be an array
select * from plpgsql_check_function('cf_f3(anycompatiblenonarray,anycompatible)');
select * from plpgsql_check_function('cf_f3(anycompatiblenonarray,anycompatible)'::regprocedure, anycompatibletype => 'int[]');

create function cf_f4(a anymultirange) returns int as $$
begin
  return 1;
end;
$$ language plpgsql;

-- anymultirange has no option, the fallback type is used for it
select * from plpgsql_check_function('cf_f4(anymultirange)');

-- when some argument is not an input argument, then the modes of all the
-- arguments are read from the catalog
create function cf_f5(a anyelement, out b anyelement, out c int) as $$
begin
  b := a;
  c := 1;
end;
$$ language plpgsql;

-- the result descriptor is built from the output arguments, and it cannot
-- be used when some of them is polymorphic
select * from plpgsql_check_function('cf_f5(anyelement)');

create function cf_f6(variadic a anyarray) returns int as $$
begin
  return array_length(a, 1);
end;
$$ language plpgsql;

-- a variadic polymorphic argument is replaced by an array of the passed
-- type
select * from plpgsql_check_function('cf_f6(anyarray)');

--
-- the dependencies on the rowtypes of the relations
--

create table cf_tab1(a int, b int);
create view cf_view1 as select * from cf_tab1;
create materialized view cf_matview1 as select * from cf_tab1;
create type cf_type1 as (a int, b int);

create function cf_f7() returns int as $$
declare
  r1 cf_tab1%rowtype;
  r2 cf_view1%rowtype;
  r3 cf_matview1%rowtype;
  r4 cf_type1;
begin
  r1.a := 1; r2.a := 1; r3.a := 1; r4.a := 1;
  return r1.a + r2.a + r3.a + r4.a;
end;
$$ language plpgsql;

select type, schema, name from plpgsql_show_dependency_tb('cf_f7') order by 1, 2, 3;

--
-- the name of an argument should not be a reserved keyword
--

create function cf_f8("table" int, "left" int) returns int as $$
begin
  return 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('cf_f8(int,int)');

--
-- the configuration of a routine is set before the check and it is
-- restored when the check is done
--

set datestyle to 'ISO, DMY';

create function cf_f9() returns text as $$
begin
  return current_setting('datestyle');
end;
$$ language plpgsql set datestyle to 'ISO, MDY';

select * from plpgsql_check_function('cf_f9');
show datestyle;

reset datestyle;

--
-- the check can be disabled
--

set plpgsql_check.mode = 'disabled';
select * from plpgsql_check_function('cf_f7');
set plpgsql_check.mode = 'by_function';

--
-- the passive check is started when the checked routine is executed
--

set plpgsql_check.mode = 'every_start';

-- a procedure and a routine which returns void are not required to be
-- closed by a RETURN statement
create procedure cf_p1() as $$
begin
  raise notice 'cf_p1';
end;
$$ language plpgsql;

create function cf_f10() returns void as $$
begin
  raise notice 'cf_f10';
end;
$$ language plpgsql;

call cf_p1();
select cf_f10();

-- but a routine which returns a value is, although the missing RETURN
-- can be reported as an extra warning only
create function cf_f11() returns int as $$
begin
  if random() < -1 then
    return 1;
  end if;
end;
$$ language plpgsql;

create function cf_f12() returns int as $$
begin
  raise notice 'cf_f12';
end;
$$ language plpgsql;

select cf_f11();
select cf_f12();

-- in the fresh_start mode every routine is checked only once
set plpgsql_check.mode = 'fresh_start';

select cf_f10();
select cf_f10();

set plpgsql_check.mode = 'by_function';

drop function cf_f1(anynonarray);
drop function cf_f2(anyenum);
drop function cf_f3(anycompatiblenonarray,anycompatible);
drop function cf_f4(anymultirange);
drop function cf_f5(anyelement);
drop function cf_f6(anyarray);
drop function cf_f7();
drop function cf_f8(int,int);
drop function cf_f9();
drop function cf_f10();
drop function cf_f11();
drop function cf_f12();
drop procedure cf_p1();
drop type cf_enum;
drop type cf_type1;
drop materialized view cf_matview1;
drop view cf_view1;
drop table cf_tab1;
