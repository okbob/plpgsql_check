set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- Tests of the tracer.
--
-- The tracer prints the entered and left functions and statements, the
-- arguments of the expressions and the content of the variables. The
-- test mode replaces the measured time and the oid of the function by
-- constants, so the output is stable.
--

-- the tracer can be activated by anybody, but it does nothing until a
-- superuser enables it
select plpgsql_check_tracer(true);

set plpgsql_check.enable_tracer to on;
set plpgsql_check.tracer_test_mode to true;

-- the control function sets the state and the verbosity and reports both
select plpgsql_check_tracer(false);
select plpgsql_check_tracer();
select plpgsql_check_tracer(null, 'terse');

create table tr_t1(a int, b int);

create function tr_f1(a int, t text)
returns int as $$
declare b int;
begin
  b := a * 2;
  if b > 100 then
    b := 100;
  end if;
  raise notice 'tr_f1: % %', b, t;
  return b;
end;
$$ language plpgsql;

-- the terse verbosity prints one short line per function and statement
select plpgsql_check_tracer(true, 'terse');

select tr_f1(10, 'terse');

-- the default verbosity prints the frame numbers and the context
select plpgsql_check_tracer(true, 'default');

select tr_f1(10, 'default');

-- the verbose verbosity prints the arguments of the function and the
-- arguments of every traced expression
select plpgsql_check_tracer(true, 'verbose');

select tr_f1(10, 'verbose');

-- the number of the subtransactions can be printed too
set plpgsql_check.tracer_show_nsubxids to on;

select tr_f1(1, 'nsubxids');

set plpgsql_check.tracer_show_nsubxids to off;

-- an IF statement with the ELSIF parts prints the condition of every part
create function tr_f2(a int)
returns text as $$
begin
  if a < 0 then
    return 'negative';
  elsif a = 0 then
    return 'zero';
  elsif a < 10 then
    return 'small';
  else
    return 'big';
  end if;
end;
$$ language plpgsql;

select tr_f2(5);

-- When the assert fails, the tracer prints the content of all the
-- variables of the function. The variables of the types which cannot be
-- converted to a string are skipped, a value containing a newline is
-- printed on a separate row, and a long value is trimmed.
create function tr_f3()
returns void as $$
declare
  r record;
  v tr_t1;
  a int[];
  n int;
  t text;
  m text;
  c refcursor := 'tr_cursor';
begin
  a := array[1, 2, 3];
  t := 'a longer text used for the trimming';
  m := 'first line' || chr(10) || 'second line';
  select 1 as x, 2 as y into r;
  v := (1, 2);
  open c for select * from tr_t1;
  assert n is not null, 'n should not be null';
  close c;
end;
$$ language plpgsql;

-- The ASSERT statement is traced separately and has its own verbosity.
-- With the verbose verbosity it prints all the variables of the
-- function. The tracer itself is switched to the terse verbosity here,
-- so only the output of the traced assert is interesting.
set plpgsql_check.trace_assert to on;
set plpgsql_check.trace_assert_verbosity to verbose;

select plpgsql_check_tracer(true, 'terse');

-- the exception raised by the failed assert is caught, so the output of
-- the tracer and the message of the exception are not mixed
do $$
begin
  perform tr_f3();
exception when others then
  raise notice 'catched: %', sqlerrm;
end;
$$;

-- the content of a long variable is trimmed
set plpgsql_check.tracer_variable_max_length to 10;

do $$
begin
  perform tr_f3();
exception when others then
  raise notice 'catched: %', sqlerrm;
end;
$$;

set plpgsql_check.tracer_variable_max_length to default;

-- a failed assert is traced too
create function tr_f4(a int)
returns void as $$
begin
  assert a > 0, 'a should be positive';
end;
$$ language plpgsql;

select tr_f4(1);

do $$
begin
  perform tr_f4(-1);
exception when others then
  raise notice 'catched: %', sqlerrm;
end;
$$;

set plpgsql_check.trace_assert_verbosity to terse;

do $$
begin
  perform tr_f4(-1);
exception when others then
  raise notice 'catched: %', sqlerrm;
end;
$$;

set plpgsql_check.trace_assert to off;
set plpgsql_check.trace_assert_verbosity to default;

-- the tracer prints the type of the fired trigger and the transition
-- records
create function tr_trg_row()
returns trigger as $$
begin
  if TG_OP = 'DELETE' then
    return old;
  end if;
  return new;
end;
$$ language plpgsql;

create function tr_trg_stmt()
returns trigger as $$
begin
  return null;
end;
$$ language plpgsql;

create trigger tr_t1_row before insert or update or delete on tr_t1
  for each row execute procedure tr_trg_row();

create trigger tr_t1_trunc before truncate on tr_t1
  for each statement execute procedure tr_trg_stmt();

select plpgsql_check_tracer(true, 'default');

insert into tr_t1 values(1, 2);
update tr_t1 set b = 3;
delete from tr_t1;

insert into tr_t1 values(1, 2);
truncate tr_t1;

select plpgsql_check_tracer(false);

set plpgsql_check.enable_tracer to off;
set plpgsql_check.tracer_test_mode to false;
set plpgsql_check.tracer_verbosity to default;

drop trigger tr_t1_trunc on tr_t1;
drop trigger tr_t1_row on tr_t1;
drop function tr_trg_stmt();
drop function tr_trg_row();
drop function tr_f4(int);
drop function tr_f3();
drop function tr_f2(int);
drop function tr_f1(int, text);
drop table tr_t1;
