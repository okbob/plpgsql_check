load 'plpgsql';
create extension if not exists plpgsql_check;

--
-- check function statement tests
--

--should fail - is not plpgsql
select * from plpgsql_check_function_tb('session_user()');

create table t1(a int, b int);

create table pa (id int, pa_id character varying(32), status character varying(60));
create table  ml(ml_id character varying(32), status_from character varying(60), pa_id character varying(32), xyz int);

create function f1()
returns void as $$
begin
  if false then
    update t1 set c = 30;
  end if;
  if false then
     raise notice '%', r.c;
  end if;
end;
$$ language plpgsql;

select f1();
select * from plpgsql_check_function_tb('f1()', fatal_errors := true);
select * from plpgsql_check_function_tb('f1()', fatal_errors := false);
select * from plpgsql_check_function_tb('f1()');

drop function f1();

create function f1()
returns void as $$
begin
  if false then
    insert into t1 values(10,20);
    update t1 set a = 10;
    delete from t1;
  end if;
end;
$$ language plpgsql stable;

select f1();

select * from plpgsql_check_function_tb('f1()', fatal_errors := false);

drop function f1();

-- profiler check
set plpgsql_check.profiler to on;

create function f1()
returns void as $$
begin
  if false then
    insert into t1 values(10,20);
    update t1 set a = 10;
    delete from t1;
  end if;
end;
$$ language plpgsql;

select lineno, stmt_lineno, exec_stmts, source from plpgsql_profiler_function_tb('f1()');

select f1();

select lineno, stmt_lineno, exec_stmts, source from plpgsql_profiler_function_tb('f1()');

select plpgsql_profiler_reset('f1()');

select lineno, stmt_lineno, exec_stmts, source from plpgsql_profiler_function_tb('f1()');

select f1();

select lineno, stmt_lineno, exec_stmts, source from plpgsql_profiler_function_tb('f1()');

select plpgsql_profiler_reset_all();

select lineno, stmt_lineno, exec_stmts, source from plpgsql_profiler_function_tb('f1()');

drop function f1();

set plpgsql_check.profiler to off;

create function f1()
returns void as $$
declare r record;
begin
  if false then
    for r in update t1 set a = a + 1 returning *
    loop
      raise notice '%', r.a;
    end loop;
  end if;
end;
$$ language plpgsql;

select f1();
select * from plpgsql_check_function_tb('f1()', fatal_errors := false);

drop function f1();

create function f1()
returns void as $$
declare r record;
begin
  if false then
    for r in update t1 set a = a + 1 returning *
    loop
      raise notice '%', r.a;
    end loop;
  end if;
end;
$$ language plpgsql stable;

select f1();
select * from plpgsql_check_function_tb('f1()', fatal_errors := false);

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

create or replace function f1_trg()
returns trigger as $$
begin
  new.a := new.a + 10;
  new.b := new.b + 10;
  return null;
end;
$$ language plpgsql;

-- ok
select * from plpgsql_check_function_tb('f1_trg()', 't1');

insert into t1 values(60,300);

select * from t1;

insert into t1 values(600,30);

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

create table tabret(a int, b int);

insert into tabret values(10,10);

create or replace function f1()
returns int as $$
begin
  return (select a from tabret);
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns int as $$
begin
  return (select a::numeric from tabret);
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns int as $$
begin
  return (select a, b from tabret);
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

drop function f1();

create or replace function f1()
returns table(ax int, bx int) as $$
begin
  return query select * from tabret;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

drop function f1();

create or replace function f1()
returns table(ax numeric, bx numeric) as $$
begin
  return query select * from tabret;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

drop function f1();

create or replace function f1()
returns setof tabret as $$
begin
  return query select * from tabret;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns setof tabret as $$
begin
  return query select a from tabret;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns setof tabret as $$
begin
  return query select a::numeric,b::numeric from tabret;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

drop function f1();

create or replace function f1(a int)
returns setof numeric as $$
begin return query select a;
end $$ language plpgsql;

select * from plpgsql_check_function('f1(int)', performance_warnings := true);

drop function f1(int);
drop table tabret;

create or replace function f1() returns void as $$
declare
intval integer;
begin
  intval := null; -- ok
  intval := 1; -- OK
  intval := '1'; -- OK
  intval := text '1'; -- not OK
  intval := current_date; -- not OK

  select 1 into intval; -- OK
  select '1' into intval; -- OK
  select text '1' into intval; -- not OK
end
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

drop function f1();

create or replace function f1()
returns int as $$
begin
  return 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns int as $$
begin
  return 1::numeric;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns int as $$
begin
  return null;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns int as $$
begin
  return current_date;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns int as $$
declare a int;
begin
  return a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns int as $$
declare a numeric;
begin
  return a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

drop function f1();

create or replace function f1()
returns setof int as $$
begin
  return next 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

create or replace function f1()
returns setof int as $$
begin
  return next 1::numeric; -- tolerant, doesn't use tupmap
end;
$$ language plpgsql;

select * from plpgsql_check_function('f1()', performance_warnings := true);

drop function f1();

create type t1 as (a int, b int, c int);
create type t2 as (a int, b numeric);

create or replace function fx()
returns t2 as $$
declare x t1;
begin
  return x;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

create or replace function fx()
returns t2 as $$
declare x t2;
begin
  return x;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

drop function fx();

create or replace function fx()
returns setof t2 as $$
declare x t1;
begin
  return next x;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

create or replace function fx()
returns setof t2 as $$
declare x t2;
begin
  return next x;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

drop function fx();

create or replace function fx2(_id int, _pa_id varchar(32), _status varchar(60))
returns void as $$
declare
begin
  insert into pa values(_id, _pa_id, _status);
exception
  when OTHERS then
    raise notice '%', 'some message';
    raise exception '%', sqlerrm;
end
$$ language plpgsql;

select * from plpgsql_check_function('fx2(int, varchar, varchar)', performance_warnings := true);

create or replace function fx2(_id int, _pa_id varchar(32), _status varchar(60))
returns void as $$
declare
begin
  insert into pa values(_id, _pa_id, _status) returning *;
exception
  when OTHERS then
    raise notice '%', 'some message';
    raise exception '%', sqlerrm;
end
$$ language plpgsql;

select * from plpgsql_check_function('fx2(int, varchar, varchar)', performance_warnings := true);

create or replace function fx2(_id int, _pa_id varchar(32), _status varchar(60))
returns void as $$
declare
begin
  SELECT * FROM pa LIMIT 1;
exception
  when OTHERS then
    raise notice '%', 'some message';
    raise exception '%', sqlerrm;
end
$$ language plpgsql;

select * from plpgsql_check_function('fx2(int, varchar, varchar)', performance_warnings := true);

drop function fx2(int, varchar, varchar);

create or replace function foreach_array_loop()
returns void as
$body$
declare
  arr text[];
  el text;
begin
  arr := array['1111','2222','3333'];
  foreach el in array arr loop
    raise notice '%', el;
  end loop;
end;
$body$
language 'plpgsql' stable;

select * from plpgsql_check_function_tb('foreach_array_loop()', performance_warnings := true);

create or replace function foreach_array_loop()
returns void as
$body$
declare
  arr text[];
  el int;
begin
  arr := array['1111','2222','3333'];
  foreach el in array arr loop
    raise notice '%', el;
  end loop;
end;
$body$
language 'plpgsql' stable;

select * from plpgsql_check_function_tb('foreach_array_loop()', performance_warnings := true);

create or replace function foreach_array_loop()
returns void as
$body$
declare
  arr date[];
  el int;
begin
  arr := array['2014-01-01','2015-01-01','2016-01-01']::date[];
  foreach el in array arr loop
    raise notice '%', el;
  end loop;
end;
$body$
language 'plpgsql' stable;

select * from plpgsql_check_function_tb('foreach_array_loop()', performance_warnings := true);

create or replace function foreach_array_loop()
returns void as
$body$
declare
  el text;
begin
  foreach el in array array['1111','2222','3333'] loop
    raise notice '%', el;
  end loop;
end;
$body$
language 'plpgsql' stable;

select * from plpgsql_check_function_tb('foreach_array_loop()', performance_warnings := true);

create or replace function foreach_array_loop()
returns void as
$body$
declare
  el int;
begin
  foreach el in array array['1111','2222','3333'] loop
    raise notice '%', el;
  end loop;
end;
$body$
language 'plpgsql' stable;

select * from plpgsql_check_function_tb('foreach_array_loop()', performance_warnings := true);

create or replace function foreach_array_loop()
returns void as
$body$
declare
  el int;
begin
  foreach el in array array['2014-01-01','2015-01-01','2016-01-01']::date[] loop
    raise notice '%', el;
  end loop;
end;
$body$
language 'plpgsql' stable;

select * from plpgsql_check_function_tb('foreach_array_loop()', performance_warnings := true);

drop function foreach_array_loop();

create or replace function scan_rows(int[]) returns void AS $$
declare
  x int[];
begin
  foreach x slice 1 in array $1
  loop
    raise notice 'row = %', x;
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('scan_rows(int[])', performance_warnings := true);

create or replace function scan_rows(int[]) returns void AS $$
declare
  x int[];
begin
  foreach x in array $1
  loop
    raise notice 'row = %', x;
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('scan_rows(int[])', performance_warnings := true);

drop function scan_rows(int[]);

drop function fx();
drop type t1;
drop type t2;

create table t1(a int, b int);
create table t2(a int, b int, c int);
create table t3(a numeric, b int);

insert into t1 values(10,20),(30,40);

create or replace function fx()
returns int as $$
declare
  s int default 0;
  r t1;
begin
  foreach r in array (select array_agg(t1) from t1)
  loop
    s := r.a + r.b;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true);

create or replace function fx()
returns int as $$
declare
  s int default 0;
  r t1;
  c t1[];
begin
  c := (select array_agg(t1) from t1);
  foreach r in array c
  loop
    s := r.a + r.b;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true);

create or replace function fx()
returns int as $$
declare
  s int default 0;
  r t1;
  c t1[];
begin
  select array_agg(t1) into c from t1;
  foreach r in array c
  loop
    s := r.a + r.b;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true);

create or replace function fx()
returns int as $$
declare
  s int default 0;
  r t1;
  c t1[];
begin
  select array_agg(t1) into c from t1;
  for i in array_lower(c, 1) .. array_upper(c, 1)
  loop
    r := c[i];
    s := r.a + r.b;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true);

create or replace function fx()
returns int as $$
declare
  s int default 0;
  c t1[];
begin
  select array_agg(t1) into c from t1;
  for i in array_lower(c, 1) .. array_upper(c, 1)
  loop
    s := (c[i]).a + (c[i]).b;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true);

create or replace function fx()
returns int as $$
declare
  s int default 0;
  r record;
  c t1[];
begin
  select array_agg(t1) into c from t1;
  for i in array_lower(c, 1) .. array_upper(c, 1)
  loop
    r := c[i];
    s := r.a + r.b;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true);

create or replace function fx()
returns int as $$
declare
  s int default 0;
  r record;
  c t1[];
begin
  select array_agg(t1) into c from t1;
  for i in array_lower(c, 1) .. array_upper(c, 1)
  loop
    r := c[i];
    s := r.a + r.b + r.c;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true);

create or replace function fx()
returns int as $$
declare
  s int default 0;
  r t2;
begin
  foreach r in array (select array_agg(t1) from t1)
  loop
    s := r.a + r.b;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true);

create or replace function fx()
returns int as $$
declare
  s int default 0;
  r t3;
begin
  foreach r in array (select array_agg(t1) from t1)
  loop
    s := r.a + r.b;
  end loop;
  return s;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true);

drop function fx();
drop table t1;

-- mscottie issue #13
create table test (
  a text,
  b integer,
  c uuid
);

create function before_insert_test()
returns trigger language plpgsql as $$
begin
  select a into NEW.a from test where b = 1;
  select b into NEW.b from test where b = 1;
  select null::uuid into NEW.c from test where b = 1;
  return new;
end;
$$;

select * from plpgsql_check_function_tb('before_insert_test()','test');

create or replace function before_insert_test()
returns trigger language plpgsql as $$
begin
  NEW.a := (select a from test where b = 1);
  NEW.b := (select b from test where b = 1);
  NEW.c := (select c from test where b = 1);
  return new;
end;
$$;

select * from plpgsql_check_function_tb('before_insert_test()','test', fatal_errors := false);

create or replace function before_insert_test()
returns trigger language plpgsql as $$
begin
  NEW.a := 'Hello'::text;
  NEW.b := 10;
  NEW.c := null::uuid;
  return new;
end;
$$;

select * from plpgsql_check_function_tb('before_insert_test()','test', fatal_errors := false);

drop function before_insert_test();

create or replace function fx()
returns void as $$
declare NEW test; OLD test;
begin
  select null::uuid into NEW.c from test where b = 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true, fatal_errors := false);

drop function fx();

create or replace function fx()
returns void as $$
declare NEW test;
begin
  NEW.a := 'Hello'::text;
  NEW.b := 10;
  NEW.c := null::uuid;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true, fatal_errors := false);

drop function fx();

drop table test;

create or replace function fx()
returns void as $$
declare
  s int;
  sa int[];
  sd date;
  bs int[];
begin
  sa[10] := s;
  sa[10] := sd;
  s := bs[10];
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx()', performance_warnings := true, fatal_errors := false);

drop function fx();

create type t as (t text);

create or replace function fx()
returns void as $$
declare _t t; _tt t[];
  _txt text;
begin
  _t.t := 'ABC'; -- correct warning "unknown"
  _tt[1] := _t;
  _txt := _t;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

drop function fx();

create or replace function fx()
returns void as $$
declare _t1 t; _t2 t;
begin
  _t1.t := 'ABC'::text;
  _t2 := _t1;
  raise notice '% %', _t2, _t2.t;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

drop function fx();

create or replace function fx(out _tt t[]) as $$
declare _t t;
begin
  _t.t := 'ABC'::text;
  _tt[1] := _t;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

drop function fx();
drop type t;

create or replace function fx()
returns int as $$
declare x int;
begin
  perform 1;
  return 10;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx()', performance_warnings := true);

drop function fx();

create table t(i int);

create function test_t(OUT t) returns t AS $$
begin
    $1 := null;
end;
$$ language plpgsql;

select test_t();
select * from test_t();

select * from plpgsql_check_function('test_t()', performance_warnings := true);

create or replace function fx()
returns void as $$
declare
  c cursor for select * from t;
  x varchar;
begin
  open c;
  fetch c into x;
  close c;
end;
$$ language plpgsql;

select test_t();
select * from test_t();

select * from plpgsql_check_function('fx()', performance_warnings := true, fatal_errors := false);

drop function fx();

create or replace function fx()
returns void as $$
declare
  c cursor for select * from t;
  x int;
begin
  open c;
  fetch c into x;
  close c;
end;
$$ language plpgsql;

select test_t();
select * from test_t();

select * from plpgsql_check_function('fx()', performance_warnings := true, fatal_errors := false);

drop function fx();

create or replace function fx()
returns void as $$
declare
  c cursor for select * from t;
begin
  for r in c loop
    raise notice '%', r.a;
  end loop;
end;
$$ language plpgsql;

select test_t();
select * from test_t();

select * from plpgsql_check_function('fx()', performance_warnings := true, fatal_errors := false);

drop function fx();

create or replace function fx()
returns void as $$
declare
  c cursor for select * from t;
begin
  for r in c loop
    raise notice '%', r.i;
  end loop;
end;
$$ language plpgsql;

select test_t();
select * from test_t();

select * from plpgsql_check_function('fx()', performance_warnings := true, fatal_errors := false);

drop function fx();

create table foo(a int, b int);

create or replace function fx()
returns void as $$
declare f1 int; f2 int;
begin
  select 1, 2 into f1;
  select 1 into f1, f2;
  select a b into f1, f2 from foo;
end;
$$ language plpgsql;

select fx();

select * from plpgsql_check_function('fx()', performance_warnings := true, fatal_errors := false);

drop function fx();
drop table foo;

create or replace function fx()
returns void as $$
declare d date;
begin
  d := (select 1 from pg_class limit 1);
  raise notice '%', d;
end;
$$ language plpgsql;

select fx();

select * from plpgsql_check_function('fx()', performance_warnings := true, fatal_errors := false);

drop function fx();

create table tab_1(i int);

create or replace function fx(a int)
returns setof int as $$
declare
  c refcursor;
  r record;
begin
  open c for select i from tab_1 where i = a;
  loop
    fetch c into r;
    if not found then
      exit;
    end if;
    return next r.i;
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx(int)', performance_warnings := true, fatal_errors := false);

create or replace function fx(a int)
returns setof int as $$
declare
  c refcursor;
  r record;
begin
  open c for select i from tab_1 where i = a;
  loop
    fetch c into r;
    if not found then
      exit;
    end if;
    return next r.x;
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx(int)', performance_warnings := true, fatal_errors := false);

drop function fx(int);
drop table tab_1;

create or replace function fxx()
returns void as $$
begin
  rollback;
end;
$$ language plpgsql;

select fxx();

select * from plpgsql_check_function('fxx()');

drop function fxx();

create or replace function fxx()
returns void as $$
declare x int;
begin
  declare x int;
  begin
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fxx()');
select * from plpgsql_check_function('fxx()', extra_warnings := false);

drop function fxx();

create or replace function fxx(in a int, in b int, out c int, out d int)
as $$
begin
  c := a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fxx(int, int)');

create or replace function fxx(in a int, in b int, out c int, out d int)
as $$
begin
  c := d;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fxx(int, int)');


create type ct as (a int, b int);

create or replace function fxx(a ct, b ct, OUT c ct, OUT d ct)
as $$
begin
  c.a := a.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fxx(ct, ct)');

create or replace function fxx(a ct, b ct, OUT c ct, OUT d ct)
as $$
begin
  c.a := d.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fxx(ct, ct)');

create or replace function tx(a int)
returns int as $$
declare a int; ax int;
begin
  declare ax int;
  begin
    ax := 10;
  end;
  a := 10;
  return 20;
end;
$$ language plpgsql;

select * from plpgsql_check_function('tx(int)');

create type xt as (a int, b int, c int);
create or replace function fx_xt(out x xt)
as $$
declare l xt;
a int;
begin
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_xt()');

drop function fx_xt();

create or replace function fx_xt(out x xt)
as $$
declare l xt;
a int;
begin
  x.c := 1000;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_xt()');

drop function fx_xt();

create or replace function fx_xt(out x xt, out y xt)
as $$
declare c1 xt; c2 xt;
begin
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_xt()');

drop function fx_xt();

create or replace function fx_xt(out x xt, out y xt)
as $$
declare c1 xt; c2 xt;
begin
  x.a := 100;
  y := row(10,20,30);
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_xt()');

drop function fx_xt();

create or replace function fx_xt(out x xt, out z int)
as $$
begin
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_xt()');

drop function fx_xt();

drop type xt;

-- missing RETURN
create or replace function fx_flow()
returns int as $$
begin
  raise notice 'kuku';
end;
$$ language plpgsql;

select fx_flow();
select * from plpgsql_check_function('fx_flow()');

-- ok
create or replace function fx_flow()
returns int as $$
declare a int;
begin
  if a > 10 then
    return a;
  end if;
  return 10;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_flow()');

-- dead code
create or replace function fx_flow()
returns int as $$
declare a int;
begin
  if a > 10 then
    return a;
  else
    return a + 1;
  end if;
  return 10;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_flow()');

-- missing return
create or replace function fx_flow()
returns int as $$
declare a int;
begin
  if a > 10 then
    return a;
  end if;
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_flow()');

drop function fx_flow();

create or replace function fx_flow(in p_param1 integer)
returns text as
$$
declare
  z1 text;
begin
  if p_param1 is not null then
    z1 := '1111';
    return z1;
  else
    z1 := '222222';
  end if;
  return z1;
end;
$$
language plpgsql stable;

select * from plpgsql_check_function_tb('fx_flow(integer)');

create or replace function fx_flow(in p_param1 integer)
returns text as
$$
declare
  z1 text;
begin
  if p_param1 is not null then
    z1 := '1111';
    return z1;
  else
    z1 := '222222';
    raise exception 'stop';
  end if;
  return z1;
end;
$$
language plpgsql stable;

select * from plpgsql_check_function_tb('fx_flow(integer)');

drop function fx_flow();

drop function fx(int);

create or replace function fx(x int)
returns table(y int)
as $$
begin
  return query select x union select x;
end
$$ language plpgsql;

select * from fx(10);

select * from plpgsql_check_function_tb('fx(int)');

drop function fx(int);

create or replace function fx(x int)
returns table(y int, z int)
as $$
begin
  return query select x,x+1 union select x, x+1;
end
$$ language plpgsql;

select * from fx(10);

select * from plpgsql_check_function_tb('fx(int)');

drop function fx(int);

create table xx(a int);

create or replace function fx(x int)
returns int as $$
declare _a int;
begin
  begin
    select a from xx into strict _a where a = x;
    return _a;
  exception when others then
    null;
  end;
  return -1;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx(int)');

drop table xx;

create or replace function fx(x int)
returns int as $$
begin
  begin
    if (x > 0) then
      raise exception 'xxx' using errcode = 'XX888';
    else
      raise exception 'yyy' using errcode = 'YY888';
    end if;
    return -1; -- dead code;
  end;
  return -1;
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx(int)');

create or replace function fx(x int)
returns int as $$
begin
  begin
    if (x > 0) then
      raise exception 'xxx' using errcode = 'XX888';
    else
      raise exception 'yyy' using errcode = 'YY888';
    end if;
  exception
    when sqlstate 'XX888' then
      null;
    when sqlstate 'YY888' then
      null;
  end;
end; -- missing return;
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx(int)');

create or replace function fx(x int)
returns int as $$
begin
  begin
    if (x > 0) then
      raise exception 'xxx' using errcode = 'XX888';
    else
      raise exception 'yyy' using errcode = 'YY888';
    end if;
  exception
    when others then
      return 10;
  end;
end; -- ok now
$$ language plpgsql;

select * from plpgsql_check_function_tb('fx(int)');

--false alarm reported by Filip Zach
create type testtype as (id integer);

create or replace function fx()
returns testtype as $$
begin
  return row(1);
end;
$$ language plpgsql;

select * from fx();
select fx();

select * from plpgsql_check_function('fx()');

drop function fx();

create function out1(OUT f1 int, OUT f2 int)
returns setof record as
$$
begin
  for f1, f2 in
     execute $q$ select 1, 2 $q$
  loop
    return next;
  end loop;
end $$ language plpgsql;

select * from plpgsql_check_function('out1()');

drop function out1();

create function out1(OUT f1 int, OUT f2 int)
returns setof record as
$$
begin
  for f1, f2 in
     select 1, 2
  loop
    return next;
  end loop;
end $$ language plpgsql;

select * from plpgsql_check_function('out1()');

drop function out1();

-- never read variable detection
create function a()
returns int as $$
declare foo int;
begin
  foo := 2;
  return 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('a()');

drop function a();

-- issue #29 false unused variable
create or replace function f1(in p_cursor refcursor) returns void as
$body$
declare
  z_offset integer;
begin
  z_offset := 10;
  move absolute z_offset from p_cursor;
end;
$body$ language 'plpgsql' stable;

select * from plpgsql_check_function_tb('f1(refcursor)');

drop function f1(refcursor);

-- issue #30 segfault due NULL refname
create or replace function test(a varchar)
returns void as $$
  declare x cursor (_a varchar) for select _a;
begin
  open x(a);
end;
$$ language plpgsql;

select * from plpgsql_check_function_tb('test(varchar)');

drop function test(varchar);

create or replace function test()
returns void as $$
declare x numeric;
begin
  x := NULL;
end;
$$ language plpgsql;

select * from plpgsql_check_function('test()');

drop function test();

create table testtable(a int);

create or replace function test()
returns int as $$
declare r testtable;
begin
  select * into r from testtable;
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('test()');

set check_function_bodies to on;

drop table testtable;

create table testtable(a int, b int);

create or replace function test()
returns int as $$
declare r testtable;
begin
  select * into r from testtable;
  return r.a;
end;
$$ language plpgsql;

alter table testtable drop column b;

-- expected false alarm on PostgreSQL 10 and older
-- there is not possibility to enforce recompilation
-- before checking. 
select * from plpgsql_check_function('test()');

drop function test();

-- issue #32
create table bigtable(id bigint, v varchar);

create or replace function test()
returns void as $$
declare
  r record;
  _id numeric;
begin
  select * into r from bigtable where id = _id;
  for r in select * from bigtable where _id = id
  loop
  end loop;
  if (exists(select * from bigtable where id = _id)) then
  end if;
end;
$$ language plpgsql;

select test();

-- should to show performance warnings
select * from plpgsql_check_function('test()', performance_warnings := true);

create or replace function test()
returns void as $$
declare
  r record;
  _id bigint;
begin
  select * into r from bigtable where id = _id;
  for r in select * from bigtable where _id = id
  loop
  end loop;
  if (exists(select * from bigtable where id = _id)) then
  end if;
end;
$$ language plpgsql;

-- there are not any performance issue now
select * from plpgsql_check_function('test()', performance_warnings := true);

-- nextval, currval and setval test
create table test_table();

create or replace function testseq()
returns void as $$
begin
  perform nextval('test_table');
  perform currval('test_table');
  perform setval('test_table', 10);
  perform setval('test_table', 10, true);
end;
$$ language plpgsql;

-- should to fail
select testseq();

select * from plpgsql_check_function('testseq()');

drop function testseq();
drop table test_table;

-- tests designed for PostgreSQL 9.2

set check_function_bodies to off;
create table t1(a int, b int);

create function f1()
returns void as $$
begin
  if false then
    update t1 set c = 30;
  end if;
  if false then
    raise notice '%', r.c;
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

drop table t1;

create function myfunc1(a int, b float) returns integer as $$ begin end $$ language plpgsql;
create function myfunc2(a int, b float) returns integer as $$ begin end $$ language plpgsql;
create function myfunc3(a int, b float) returns integer as $$ begin end $$ language plpgsql;
create function myfunc4(a int, b float) returns integer as $$ begin end $$ language plpgsql;

create table mytable(a int);
create table myview as select * from mytable;

create function testfunc(a int, b float)
returns void as $$
declare x integer;
begin
  raise notice '%', myfunc1(a, b);
  x := myfunc2(a, b);
  perform myfunc3(m.a, b) from myview m;
  insert into mytable select myfunc4(a, b);
end;
$$ language plpgsql;

select * from plpgsql_check_function('testfunc(int,float)');
select type, schema, name, params from plpgsql_show_dependency_tb('testfunc(int,float)');

drop function testfunc(int, float);
drop function myfunc1(int, float);
drop function myfunc2(int, float);
drop function myfunc3(int, float);
drop function myfunc4(int, float);

drop table mytable;
drop view myview;

-- issue #34
create or replace function testcase()
returns bool as $$
declare x int;
begin
  set local search_path to public, test;
  case x when 1 then return true; else return false; end case;
end;
$$ language plpgsql;

-- should not to raise warning
select * from plpgsql_check_function('testcase()');

drop function testcase();

-- Adam's Bartoszewicz example
create or replace function public.test12()
returns refcursor
language plpgsql
as $body$
declare
  rc refcursor;
begin
  open rc scroll for select pc.* from pg_cast pc;
  return rc;
end;
$body$;

-- should not returns false alarm
select * from plpgsql_check_function('test12()');

drop function public.test12();

-- should to show performance warning on bad flag
create or replace function flag_test1(int)
returns int as $$
begin
  return $1 + 10;
end;
$$ language plpgsql stable;

create table fufu(a int);

create or replace function flag_test2(int)
returns int as $$
begin
  return (select * from fufu limit 1);
end;
$$ language plpgsql volatile;

select * from plpgsql_check_function('flag_test1(int)', performance_warnings := true);
select * from plpgsql_check_function('flag_test2(int)', performance_warnings := true);

drop table fufu;
drop function flag_test1(int);
drop function flag_test2(int);

create or replace function rrecord01()
returns setof record as $$
begin
  return query select 1,2;
end;
$$ language plpgsql;

create or replace function rrecord02()
returns record as $$
begin
  return row(10,20,30);
end;
$$ language plpgsql;

-- should not to raise false alarms
select * from plpgsql_check_function('rrecord01');
select * from plpgsql_check_function('rrecord02');

drop function rrecord01();
drop function rrecord02();

create or replace function bugfunc01()
returns void as $$
declare
  cvar cursor(a int, b int) for select a + b from generate_series(1,b);
begin
  for t in cvar(1,3)
  loop
    raise notice '%', t;
  end loop;
end;
$$ language plpgsql;

select bugfunc01();

select * from plpgsql_check_function('bugfunc01');

create or replace function bugfunc02()
returns void as $$
declare
  cvar cursor(a int, b int) for select a + b from generate_series(1,b);
begin
  open cvar(10,20);
  close cvar;
end;
$$ language plpgsql;

select bugfunc02();

select * from plpgsql_check_function('bugfunc02');

create or replace function bugfunc03()
returns void as $$
declare
  cvar cursor(a int, b int) for select a + b from not_exists_table;
begin
  open cvar(10,20);
  close cvar;
end;
$$ language plpgsql;

select bugfunc03();

select * from plpgsql_check_function('bugfunc03');

create or replace function f1(out cr refcursor)
as $$
begin
end;
$$ language plpgsql;

-- should to raise warning
select * from plpgsql_check_function('f1()');

create or replace function f1(out cr refcursor)
as $$
begin
  open cr for select 1;
end;
$$ language plpgsql;

-- should not to raise warning, see issue #43
select * from plpgsql_check_function('f1()');

drop function f1();

create table testt(a int);

create or replace function testt_trg_func()
returns trigger as $$
begin
  return new;
end;
$$ language plpgsql;

create trigger testt_trg
  before insert or update
  on testt
  for each row execute procedure testt_trg_func();

create or replace function maintaince_function()
returns void as $$
begin

  alter table testt disable trigger testt_trg;
  alter table testt enable trigger testt_trg;

end;
$$ language plpgsql;

-- should not to crash
select * from plpgsql_check_function_tb('maintaince_function()', 0, true, true, true);

drop function maintaince_function();
drop trigger testt_trg on testt;
drop function testt_trg_func();
drop table testt;

create or replace function test_crash()
returns void as $$
declare
  ec int default buggyfunc(10);
begin
  select * into ec from buggytab;
end;
$$ language plpgsql;

-- should not to crash
select * from plpgsql_check_function('test_crash', fatal_errors := false);
select * from plpgsql_check_function('test_crash', fatal_errors := true);

drop function test_crash();

-- fix false alarm reported by Piotr Stepniewski
create or replace function public.fx()
returns void
language plpgsql
as $function$
begin
  raise exception 'xxx';
end;
$function$;

-- show raise nothing
select * from plpgsql_check_function('fx()');

create table errtab(
  message text,
  code character(5)
);

create or replace function public.fx()
returns void
language plpgsql
as $function$
declare
  var errtab%rowtype;
begin
  raise exception using message = var.message, errcode = var.code;
end;
$function$;

-- should not to crash
select * from plpgsql_check_function('fx()');

create or replace function public.fx()
returns void
language plpgsql
as $function$
declare
  var errtab%rowtype;
begin
  raise exception using message = var.message, errcode = var.code, hint = var.hint;
end;
$function$;

-- should not to crash
select * from plpgsql_check_function('fx()');

drop function fx();


