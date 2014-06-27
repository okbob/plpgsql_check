LOAD 'plpgsql';
CREATE EXTENSION plpgsql_check;

--
-- check function statement tests
--

--should fail - is not plpgsql
select * from plpgsql_check_function_tb('session_user()');

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

create function g1(out a int, out b int)
as $$
  select 10,20;
$$ language sql;

create function f1()
returns void as $$
declare r record;
begin
  r := g1();
  if false then
    raise notice '%', r.c;
  end if;
end;
$$ language plpgsql;

select f1();
select * from plpgsql_check_function_tb('f1()');

select f1();

drop function f1();
drop function g1();

create function g1(out a int, out b int)
returns setof record as $$
select * from t1;
$$ language sql;

create function f1()
returns void as $$
declare r record;
begin
  for r in select * from g1()
  loop
    raise notice '%', r.c;
  end loop;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function_tb('f1()');

select f1();

create or replace function f1()
returns void as $$
declare r record;
begin
  for r in select * from g1()
  loop
    r.c := 20;
  end loop;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function_tb('f1()');

select f1();

drop function f1();
drop function g1();

create function f1()
returns int as $$
declare r int;
begin
  if false then
    r := a + b;
  end if;
  return r;
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

create or replace function f1()
returns void as $$
declare r int[];
begin
  if false then
    r[c+10] := 20;
  end if;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function_tb('f1()');

select f1();

drop function f1();

create or replace function f1()
returns void as $$
declare r int;
begin
  if false then
    r[10] := 20;
  end if;
end;
$$ language plpgsql set search_path = public;

select f1();

select * from plpgsql_check_function_tb('f1()');

select f1();

drop function f1();

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

select * from plpgsql_check_function_tb('f1()');

drop function f1();

create or replace function f1_trg()
returns trigger as $$
begin
  if new.a > 10 then
    raise notice '%', new.b;
    raise notice '%', new.c;
  end if;
  return new;
end;
$$ language plpgsql;

create trigger t1_f1 before insert on t1
  for each row
  execute procedure f1_trg();

insert into t1 values(6,30);

select * from plpgsql_check_function_tb('f1_trg()','t1');

insert into t1 values(6,30);

create or replace function f1_trg()
returns trigger as $$
begin
  new.a := new.a + 10;
  new.b := new.b + 10;
  new.c := 30;
  return new;
end;
$$ language plpgsql;

-- should to fail

select * from plpgsql_check_function_tb('f1_trg()','t1');

-- should to fail but not crash
insert into t1 values(6,30);

create or replace function f1_trg()
returns trigger as $$
begin
  new.a := new.a + 10;
  new.b := new.b + 10;
  return new;
end;
$$ language plpgsql;

-- ok
select * from plpgsql_check_function_tb('f1_trg()', 't1');

-- ok
insert into t1 values(6,30);

select * from t1;

drop trigger t1_f1 on t1;

drop function f1_trg();

-- test of showing caret on correct place for multiline queries
create or replace function f1()
returns void as $$
begin
  select
  var
  from
  foo;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('f1()');

drop function f1();

create or replace function f1()
returns int as $$
begin
  return (select a
             from t1
            where hh = 20);
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('f1()');

create or replace function f1()
returns int as $$
begin
  return (select a
             from txxxxxxx
            where hh = 20);
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('f1()');

drop function f1();

drop table t1;
drop type _exception_type;

-- raise warnings when target row has different number of attributies in
-- SELECT INTO statement

create or replace function f1()
returns void as $$
declare a1 int; a2 int;
begin
  select 10,20 into a1,a2;
end;
$$ language plpgsql;

-- should be ok
select * from plpgsql_check_function_tb('f1()');

create or replace function f1()
returns void as $$
declare a1 int;
begin
  select 10,20 into a1;
end;
$$ language plpgsql;

-- raise warning
select * from plpgsql_check_function_tb('f1()');

create or replace function f1()
returns void as $$
declare a1 int; a2 int;
begin
  select 10 into a1,a2;
end;
$$ language plpgsql;

-- raise warning
select * from plpgsql_check_function_tb('f1()');

-- bogus code
set check_function_bodies to off;

create or replace function f1()
returns void as $$
adasdfsadf
$$ language plpgsql;

select * from plpgsql_check_function_tb('f1()');

drop function f1();

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
select * from plpgsql_check_function('f1()', fatal_errors := true);
select * from plpgsql_check_function('f1()', fatal_errors := false);

select * from plpgsql_check_function('f1()');

select f1();

drop function f1();

create function g1(out a int, out b int)
as $$
  select 10,20;
$$ language sql;

create function f1()
returns void as $$
declare r record;
begin
  r := g1();
  if false then
    raise notice '%', r.c;
  end if;
end;
$$ language plpgsql;

select f1();
select * from plpgsql_check_function('f1()');

select f1();

drop function f1();
drop function g1();

create function g1(out a int, out b int)
returns setof record as $$
select * from t1;
$$ language sql;

create function f1()
returns void as $$
declare r record;
begin
  for r in select * from g1()
  loop
    raise notice '%', r.c;
  end loop;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function('f1()');

select f1();

create or replace function f1()
returns void as $$
declare r record;
begin
  for r in select * from g1()
  loop
    r.c := 20;
  end loop;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function('f1()');

select f1();

drop function f1();
drop function g1();

create function f1()
returns int as $$
declare r int;
begin
  if false then
    r := a + b;
  end if;
  return r;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function('f1()');

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

select * from plpgsql_check_function('f1()');

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

select * from plpgsql_check_function('f1()');

select f1();

drop function f1();

create or replace function f1()
returns void as $$
declare r int[];
begin
  if false then
    r[c+10] := 20;
  end if;
end;
$$ language plpgsql;

select f1();

select * from plpgsql_check_function('f1()');

select f1();

drop function f1();

create or replace function f1()
returns void as $$
declare r int;
begin
  if false then
    r[10] := 20;
  end if;
end;
$$ language plpgsql set search_path = public;

select f1();

select * from plpgsql_check_function('f1()');

select f1();

drop function f1();

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

create or replace function f1_trg()
returns trigger as $$
begin
  if new.a > 10 then
    raise notice '%', new.b;
    raise notice '%', new.c;
  end if;
  return new;
end;
$$ language plpgsql;

create trigger t1_f1 before insert on t1
  for each row
  execute procedure f1_trg();

insert into t1 values(6,30);

select * from plpgsql_check_function('f1_trg()','t1');

insert into t1 values(6,30);

create or replace function f1_trg()
returns trigger as $$
begin
  new.a := new.a + 10;
  new.b := new.b + 10;
  new.c := 30;
  return new;
end;
$$ language plpgsql;

-- should to fail

select * from plpgsql_check_function('f1_trg()','t1');

-- should to fail but not crash
insert into t1 values(6,30);

create or replace function f1_trg()
returns trigger as $$
begin
  new.a := new.a + 10;
  new.b := new.b + 10;
  return new;
end;
$$ language plpgsql;

-- ok
select * from plpgsql_check_function('f1_trg()', 't1');

-- ok
insert into t1 values(6,30);

select * from t1;

drop trigger t1_f1 on t1;

drop function f1_trg();

-- test of showing caret on correct place for multiline queries
create or replace function f1()
returns void as $$
begin
  select
  var
  from
  foo;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()');

drop function f1();

create or replace function f1()
returns int as $$
begin
  return (select a
             from t1
            where hh = 20);
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()');

create or replace function f1()
returns int as $$
begin
  return (select a
             from txxxxxxx
            where hh = 20);
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()');

drop function f1();

drop table t1;
drop type _exception_type;

-- raise warnings when target row has different number of attributies in
-- SELECT INTO statement

create or replace function f1()
returns void as $$
declare a1 int; a2 int;
begin
  select 10,20 into a1,a2;
end;
$$ language plpgsql;

-- should be ok
select * from plpgsql_check_function('f1()');

create or replace function f1()
returns void as $$
declare a1 int;
begin
  select 10,20 into a1;
end;
$$ language plpgsql;

-- raise warning
select * from plpgsql_check_function('f1()');

create or replace function f1()
returns void as $$
declare a1 int; a2 int;
begin
  select 10 into a1,a2;
end;
$$ language plpgsql;

-- raise warning
select * from plpgsql_check_function('f1()');

-- bogus code
set check_function_bodies to off;

create or replace function f1()
returns void as $$
adasdfsadf
$$ language plpgsql;

select * from plpgsql_check_function('f1()');

drop function f1();

create table f1tbl(a int, b int);

-- unused variables
create or replace function f1(_input1 int)
returns table(_output1 int, _output2 int)
as $$
declare
_f1 int;
_f2 int;
_f3 int;
_f4 int;
_f5 int;
_r record;
_tbl f1tbl;
begin
if true then
	_f1 := 1;
end if;
select 1, 2 into _f3, _f4;
perform 1 where _f5 is null;
select 1 into _r;
select 1, 2 into _tbl;

-- check that SQLSTATE and SQLERRM don't raise false positives
begin
exception when raise_exception then
end;

end
$$ language plpgsql;

select * from plpgsql_check_function('f1(int)');

drop function f1(int);
drop table f1tbl;

-- check that NEW and OLD are not reported unused
create table f1tbl();
create or replace function f1()
returns trigger
as $$
begin
return null;
end
$$ language plpgsql;

select * from plpgsql_check_function('f1()', 'f1tbl');

drop function f1();
drop table f1tbl;

