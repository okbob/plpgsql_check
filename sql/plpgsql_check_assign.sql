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

-- FOREACH applies assignment casts to elements and slices, not array input.
create function as_f15()
returns int as $$
declare
  item int;
  total int := 0;
begin
  foreach item in array array[1.6::numeric, 2.4::numeric] loop
    total := total + item;
  end loop;
  return total;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f15');
select as_f15();

create function as_f16()
returns int[] as $$
declare item int[];
begin
  foreach item slice 1 in array array[[1.6::numeric, 2.4::numeric]] loop
    return item;
  end loop;
  return null;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f16');
select as_f16();

-- Without an assignment cast, invalid textual input is still rejected.
create function as_f17()
returns void as $$
declare item int;
begin
  foreach item in array array['not an integer'] loop
    raise notice '%', item;
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f17');

drop function as_f15();
drop function as_f16();
drop function as_f17();
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

-- A composite source may have a valid scalar assignment cast, or be NULL.
create type as_pair as (a int, b int);

create function as_f18()
returns text as $$
declare value text;
begin
  select row(1, 2) into value;
  return value;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f18');
select as_f18();

create function as_f19()
returns int as $$
declare value int;
begin
  select null::as_tab1 into value;
  return value;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f19');
select as_f19() is null;

create function as_pair_to_int(p as_pair)
returns int as $$ select p.a + p.b $$ language sql immutable;
create cast (as_pair as int) with function as_pair_to_int(as_pair) as assignment;

create function as_f20()
returns int as $$
declare value int;
begin
  select row(1, 2)::as_pair into value;
  return value;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f20');
select as_f20();

drop function as_f18();
drop function as_f19();
drop function as_f20();
drop cast (as_pair as int);
drop function as_pair_to_int(as_pair);
drop type as_pair;

-- RETURN QUERY retains composite-valued columns; RETURN expands a row value.
create type as_leaf as (n int);
create type as_wrapper as (payload as_leaf);

create function as_f21()
returns setof as_wrapper as $$
begin
  return query select row(7)::as_leaf;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f21');
select (payload).n from as_f21();

create function as_f22()
returns setof as_wrapper as $$
begin
  return query execute 'select row(7)::as_leaf';
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f22');
select (payload).n from as_f22();

create function as_f23()
returns setof as_leaf as $$
begin
  return query select row(7)::as_leaf;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f23');

select * from as_f23();

create function as_f24()
returns as_leaf as $$
begin
  return row(7)::as_leaf;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f24');
select (as_f24()).n;

create function as_f25()
returns setof as_leaf as $$
begin
  return next row(7)::as_leaf;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f25');
select * from as_f25();

-- Bounds and the RHS read their variables; the implicit old array does not.
create function as_f26()
returns void as $$
declare
  element_array int[] := array[1, 2];
  lower_array int[] := array[1, 2];
  upper_array int[] := array[1, 2];
  rhs_array int[] := array[1, 2];
  fetch_array int[] := array[1, 2];
  write_only int[] := array[1, 2];
begin
  element_array[element_array[1]] := 3;
  lower_array[lower_array[1]:2] := array[3, 4];
  upper_array[1:upper_array[2]] := array[3, 4];
  rhs_array[1] := rhs_array[2];
  fetch_array := fetch_array[1:2];
  write_only[1] := 3;
end;
$$ language plpgsql;

select * from plpgsql_check_function('as_f26');
select as_f26();

drop function as_f26();
drop function as_f21();
drop function as_f22();
drop function as_f23();
drop function as_f24();
drop function as_f25();
drop type as_wrapper;
drop type as_leaf;

drop table as_tab1;
