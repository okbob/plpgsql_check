load 'plpgsql';
load 'plpgsql_check';

set plpgsql_check.mode = 'every_start';

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

drop function f1();
drop type _exception_type;

create or replace procedure proc_test()
as $$
begin
  commit;
end;
$$ language plpgsql;

call proc_test();

drop procedure proc_test();

create table footab(a int, b int, c int);

create or replace function footab_trig_func()
returns trigger as $$
declare x int;
begin
  if false then
    -- should be ok;
    select count(*) from newtab into x;

    -- should fail;
    select count(*) from newtab where d = 10 into x;
  end if;
  return null;
end;
$$ language plpgsql;

/*
create trigger footab_trigger
  after insert on footab
  referencing new table as newtab
  for each statement execute procedure footab_trig_func();

insert into footab values(1,2,3);
*/
drop table footab;
drop function footab_trig_func();
