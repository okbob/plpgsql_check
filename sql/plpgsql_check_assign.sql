set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- assignment to the automatic variables
--

-- the variable of a numeric FOR loop is maintained by the runtime, so it
-- should not be modified by the body of the loop
create function as_f1(n int)
returns int as $$
declare s int := 0;
begin
  for i in 1..n loop
    s := s + i;
    i := i + 1;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f1', extra_warnings => false);
select * from plpgsql_check_function('as_f1', extra_warnings => true);

-- the same for the FOUND variable
create function as_f2()
returns int as $$
begin
  found := true;
  return 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f2', extra_warnings => true);

--
-- assignment to a constant
--

-- the default value of a constant is assigned at the start of the block,
-- and it is not reported
create function as_f4()
returns int as $$
declare c constant int := 1;
begin
  return c;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f4');

--
-- assignment to a field of a record
--

-- the structure of a record is known after the first assignment only
create function as_f5()
returns int as $$
declare r record;
begin
  r.a := 1;
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f5');

-- when the record has a structure, the name of the field is validated
create function as_f6()
returns int as $$
declare r record;
begin
  select 1 as a, 2 as b into r;
  r.c := 1;
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f6');

-- and the type of the field is used for the check of the assigned value
create function as_f7()
returns int as $$
declare r record;
begin
  select 1 as a, 2 as b into r;
  r.a := 'x';
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f7');

--
-- checks of the assigned type
--

create table as_tab1(a int, b int, c int);

-- a composite value cannot be assigned to a scalar variable
create function as_f8()
returns int as $$
declare x int;
begin
  select t into x from as_tab1 t;
  return x;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f8');

-- a cast which is only explicit, a cast which is allowed on assignment
-- and a cast which is done implicitly
create function as_f9()
returns void as $$
declare
  i int;
  t text;
  n numeric;
begin
  select 'x'::text into i;
  select 1 into t;
  select 1.5::numeric into i;
  select 1 into n;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f9', fatal_errors => false);
select * from plpgsql_check_function('as_f9', fatal_errors => false, performance_warnings => true);

--
-- assignment of a query result to a composite variable
--

-- a variable of a table rowtype where the table has a dropped column -
-- the descriptors are not equal, but the dropped column is skipped on
-- both sides
alter table as_tab1 drop column b;

create function as_f10()
returns int as $$
declare r as_tab1;
begin
  select * into r from as_tab1;
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f10');

-- the query returns less columns than the variable has
create function as_f11()
returns int as $$
declare r as_tab1;
begin
  select a into r from as_tab1;
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f11');

-- and more columns than the variable has
create function as_f12()
returns int as $$
declare r as_tab1;
begin
  select a, c, a from as_tab1 into r;
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f12');

-- the types of the columns are compared one by one
create function as_f13()
returns int as $$
declare r as_tab1;
begin
  select 'x'::text, 1 into r;
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f13', fatal_errors => false);

-- a whole row value of a type with a dropped column is assigned to a
-- record variable
create function as_f14()
returns int as $$
declare r record;
begin
  select t.* into r from as_tab1 t;
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f14');

-- the NEW and the OLD records of a trigger get the descriptor of the
-- triggering relation, which contains the dropped column too
create function as_trg1()
returns trigger as $$
begin
  new.a := old.a;
  new.c := 1;
  return new;
end;
$$ language plpgsql;

create trigger as_trg1 before update on as_tab1
  for each row execute procedure as_trg1();

select * from plpgsql_check_function('as_trg1', relid => 'as_tab1'::regclass);

drop function as_f1(int);
drop function as_f2();
drop function as_f4();
drop function as_f5();
drop function as_f6();
drop function as_f7();
drop function as_f8();
drop function as_f9();
drop function as_f10();
drop function as_f11();
drop function as_f12();
drop function as_f13();
drop function as_f14();
drop trigger as_trg1 on as_tab1;
drop function as_trg1();
drop table as_tab1;
