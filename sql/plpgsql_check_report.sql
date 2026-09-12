set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- reports about unused, never read and unmodified variables and parameters
--

-- a declared variable that is not used at all, a variable that is only
-- written and a variable that is only read
create function rp_f1()
returns int as $$
declare
  unused_var int;
  write_only int;
  read_only int := 10;
begin
  write_only := 1;
  return read_only;
end;
$$ language plpgsql;

-- the "never read" report belongs to the extra warnings, the "unused"
-- report is printed always
select * from plpgsql_check_function('rp_f1', extra_warnings => false);
select * from plpgsql_check_function('rp_f1', extra_warnings => true);

-- the same for the parameters of a function - "a" is not used at all,
-- "b" is only written and "c" is read
create function rp_f2(a int, b int, c int)
returns int as $$
begin
  b := 1;
  return c;
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_f2', extra_warnings => true);

-- INOUT parameter of a procedure that is only written is expected, because
-- a procedure cannot have a plain OUT parameter in older releases
create procedure rp_p1(inout a int, inout b int)
as $$
begin
  a := 1;
  b := b + 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_p1', extra_warnings => true);

-- single OUT variable that is not modified
create function rp_f4(out a int)
as $$
begin
  raise notice 'nothing';
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_f4', extra_warnings => true);

-- more OUT variables, one of them is composite, so the result of the
-- function cannot be assigned by a simple expression
create type rp_t1 as (x int, y int);

create function rp_f5(out a int, out b rp_t1, out c rp_t1)
as $$
begin
  b := (1,2)::rp_t1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_f5', extra_warnings => true);

-- when the result is returned by a dynamic query whose text is not known,
-- the OUT variables are reported as "maybe unmodified" only
create function rp_f6(p text, out a int, out b int)
returns setof record as $$
begin
  return query execute p;
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_f6', extra_warnings => true);

-- the same with a single OUT variable
create function rp_f6b(p text, out a int)
returns setof int as $$
begin
  return query execute p;
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_f6b', extra_warnings => true);

-- a dynamic query with a known text is checked like a static query
create function rp_f6c(out a int, out b int)
returns setof record as $$
begin
  return query execute 'select 1, 2';
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_f6c', extra_warnings => true);

-- RETURN QUERY (without EXECUTE) sets the OUT variables, so nothing is
-- reported
create function rp_f7(out a int, out b int)
returns setof record as $$
begin
  return query select 1, 2;
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_f7', extra_warnings => true);

-- a variable used only as a field of a record or of a row is used
create function rp_f8()
returns int as $$
declare
  r record;
  x int;
  y int;
begin
  select 1 as a, 2 as b into r;
  select 1, 2 into x, y;
  return r.a + x + y;
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_f8', extra_warnings => true);

--
-- reports about too high volatility
--

-- the body is immutable, but the function is declared as volatile
create function rp_f9(a int)
returns int as $$
begin
  return a + 1;
end;
$$ language plpgsql volatile;

select * from plpgsql_check_function('rp_f9', performance_warnings => true);

-- the same function declared as stable
create function rp_f10(a int)
returns int as $$
begin
  return a + 1;
end;
$$ language plpgsql stable;

select * from plpgsql_check_function('rp_f10', performance_warnings => true);

-- reading a table makes the function stable
create table rp_tab1(a int);

create function rp_f11()
returns int as $$
begin
  return (select count(*) from rp_tab1);
end;
$$ language plpgsql volatile;

select * from plpgsql_check_function('rp_f11', performance_warnings => true);

-- a procedure returns void, so the stable volatility is not suggested
create procedure rp_p2()
as $$
begin
  perform count(*) from rp_tab1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('rp_p2', performance_warnings => true);

-- when the function uses a dynamic SQL, the volatility cannot be safely
-- determined, so the report has an additional detail
create function rp_f12(a int)
returns int as $$
declare r int;
begin
  execute 'select 1' into r;
  return a + r;
end;
$$ language plpgsql volatile;

select * from plpgsql_check_function('rp_f12', performance_warnings => true);

-- an immutable function is never reported
create function rp_f13(a int)
returns int as $$
begin
  return a + 1;
end;
$$ language plpgsql immutable;

select * from plpgsql_check_function('rp_f13', performance_warnings => true);

drop function rp_f1();
drop function rp_f2(int, int, int);
drop procedure rp_p1(int, int);
drop function rp_f4();
drop function rp_f5();
drop function rp_f6(text);
drop function rp_f6b(text);
drop function rp_f6c();
drop function rp_f7();
drop function rp_f8();
drop function rp_f9(int);
drop function rp_f10(int);
drop function rp_f11();
drop procedure rp_p2();
drop function rp_f12(int);
drop function rp_f13(int);
drop table rp_tab1;
drop type rp_t1;
