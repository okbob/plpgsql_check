load 'plpgsql';
load 'plpgsql_check';

set plpgsql_check.mode = 'every_start';

create table t1(a int, b int);

create function f1()
returns void as $$
begin
  if false then
    update t1 set c = 30;
  end if;
end;
$$ language plpgsql;

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

drop function f1();


create or replace function f1()
returns void as $$
declare r int;
begin
  if false then
    r[10] := 20;
  end if;
end;
$$ language plpgsql;

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


create or replace function f1()
returns void as $$
begin
  if false then
    insert into badbadtable values(10,20);
  end if;
  return;
end;
$$ language plpgsql;

set plpgsql_check.mode = 'fresh_start';

select f1();
-- should not raise exception there
select f1();

create or replace function f1()
returns void as $$
begin
  if false then
    insert into badbadtable values(10,20);
  end if;
  return;
end;
$$ language plpgsql;

-- after refreshing it should to raise exception again
select f1();

set plpgsql_check.mode = 'every_start';

-- should to raise warning only
set plpgsql_check.fatal_errors = false;
select f1();

drop function f1();

create function f1()
returns setof t1 as $$
begin
  if false then
    return query select a,a,a from t1;
    return;
  end if;
end;
$$ language plpgsql;

select * from f1();

drop function f1();

create function f1()
returns setof t1 as $$
begin
  if false then
    return query select a, b::numeric from t1;
    return;
  end if;
end;
$$ language plpgsql;

select * from f1();

drop function f1();

drop table t1;
drop type _exception_type;

do $$
declare
begin
  if false then
    for i in 1,3..(2) loop
      raise notice 'foo %', i;
    end loop;
  end if;
end;
$$;
