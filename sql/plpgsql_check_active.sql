load 'plpgsql';
create extension if not exists plpgsql_check;

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
     raise notice '%', r.c;
  end if;
end;
$$ language plpgsql;

select f1();
select * from plpgsql_check_function_tb('f1()', fatal_errors := true);
select * from plpgsql_check_function_tb('f1()', fatal_errors := false);
select * from plpgsql_check_function_tb('f1()');

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

create or replace function f1()
returns void as $$
declare
  _exception _exception_type;
begin
  _exception := NULL::_exception_type;
exception when others then
  get stacked diagnostics
        _exception.state = RETURNED_SQLSTATE,
        _exception.message = MESSAGE_TEXT,
        _exception.detail = PG_EXCEPTION_DETAIL;
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

create table pa (id int, pa_id character varying(32), status character varying(60));
create table  ml(ml_id character varying(32), status_from character varying(60), pa_id character varying(32), xyz int);

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
