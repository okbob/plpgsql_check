set client_min_messages to warning;
create extension if not exists plpgsql_check;
set client_min_messages to notice;

--
-- Tests of the plugin multiplexer.
--
-- The plugin of plpgsql allows one client only, so plpgsql_check
-- registers a multiplexer, which dispatches the events to the tracer,
-- to the profiler and to the passive check.  The multiplexer keeps a
-- stack of the entered statements per an executed routine, and it has
-- to unwind the stack when an exception aborts the execution.
--

-- the multiplexer is installed when the library is loaded, so the
-- routines which are already running have no state allocated for them,
-- and the events of their statements have to be ignored (this is a no
-- operation when the library is preloaded)
do $$
begin
  execute 'load ''plpgsql_check''';
  perform 1;
end;
$$;

select plpgsql_check_tracer(true, 'terse');

set plpgsql_check.enable_tracer to on;
set plpgsql_check.tracer_test_mode to true;
set plpgsql_check.profiler to on;

create function pd_f1(a int) returns int as $$
begin
  if a = 0 then
    raise exception 'pd_f1 failed';
  end if;
  return a;
end;
$$ language plpgsql;

create function pd_f2(a int) returns int as $$
declare res int;
begin
  begin
    res := pd_f1(a);
  exception when others then
    res := -1;
  end;
  return res;
end;
$$ language plpgsql;

-- no exception, the statements are closed in the expected order
select pd_f2(1);

-- the called routine is aborted, so its part of the stack is unwound
-- when the outer routine continues
select pd_f2(0);

-- the same, but the exception is raised in a deeper nested block, so
-- more statements have to be closed at once
create function pd_f3(a int) returns int as $$
declare res int;
begin
  begin
    for i in 1..3 loop
      if i = 2 then
        res := pd_f1(a);
      end if;
    end loop;
  exception when others then
    res := -1;
  end;
  return res;
end;
$$ language plpgsql;

select pd_f3(0);

-- an exception which is not handled by the executed routine leaves the
-- whole stack open
do $$
begin
  perform pd_f3(1);
  perform pd_f1(0);
exception when others then
  raise notice 'handled by the block';
end;
$$;

-- an exception raised by an inline block, which is not handled at all
do $$
begin
  perform pd_f1(0);
end;
$$;

-- the memory of an aborted inline block is released later than the
-- memory of an aborted routine, so its entry has to be removed from the
-- stack of the multiplexer when the outer routine continues
create function pd_f4() returns int as $$
declare res int;
begin
  begin
    execute 'do $x$ begin perform pd_f1(0); end $x$';
  exception when others then
    res := -1;
  end;
  res := coalesce(res, 0);
  return res;
end;
$$ language plpgsql;

select pd_f4();

-- when a plugin raises an exception, the multiplexer has to restore the
-- plugin info of the executed routine before the exception is reraised
set plpgsql_check.cursors_leaks to on;
set plpgsql_check.cursors_leaks_errlevel to 'error';

create function pd_f5() returns int as $$
declare c refcursor;
begin
  open c for select 1;
  return 1;
end;
$$ language plpgsql;

-- the leaked cursor is detected when the routine is left
set plpgsql_check.strict_cursors_leaks to on;

do $$
begin
  perform pd_f5();
exception when others then
  raise notice 'leak reported: %', sqlerrm;
end;
$$;

-- and when the statement which opened it is executed again
set plpgsql_check.strict_cursors_leaks to off;

create function pd_f6() returns int as $$
declare c refcursor;
begin
  for i in 1..2 loop
    c := 'pd_cursor_' || i;
    open c for select 1;
  end loop;
  return 1;
end;
$$ language plpgsql;

do $$
begin
  perform pd_f6();
exception when others then
  raise notice 'leak reported: %', sqlerrm;
end;
$$;

set plpgsql_check.strict_cursors_leaks to default;
set plpgsql_check.cursors_leaks_errlevel to default;
set plpgsql_check.cursors_leaks to default;

-- the multiplexer is not used when no plugin is active
set plpgsql_check.enable_tracer to off;
set plpgsql_check.profiler to off;

select pd_f2(0);

-- the profiler alone
set plpgsql_check.profiler to on;
select pd_f2(0);
set plpgsql_check.profiler to off;

set plpgsql_check.enable_tracer to off;
select plpgsql_check_tracer(false, 'default');

drop function pd_f6();
drop function pd_f5();
drop function pd_f4();
drop function pd_f3(int);
drop function pd_f2(int);
drop function pd_f1(int);
