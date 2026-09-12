set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- Tests of the control functions of the profiler and of the checks of
-- the arguments of the profiler functions.
--

-- the profiler is enabled and disabled by a function, which reports the
-- new state and returns it
select plpgsql_check_profiler(true);
select plpgsql_check_profiler(false);

-- when the argument is omitted or null, the state is only reported. The
-- reported notices describe the configuration of the shared memory,
-- which depends on the preloading of the library and on the size of the
-- allocated segment, so they are suppressed here and only the returned
-- value is checked.
set client_min_messages to warning;

select plpgsql_check_profiler();

select plpgsql_check_profiler(true);
select plpgsql_check_profiler();

set plpgsql_check.use_shared_stats_when_it_possible to off;
select plpgsql_check_profiler(null);

set plpgsql_check.use_shared_stats_when_it_possible to default;
select plpgsql_check_profiler(null);

select plpgsql_check_profiler(false);

set client_min_messages to notice;

-- the fake queryid hook is installed and removed only once, the
-- repeated calls do nothing
select plpgsql_profiler_install_fake_queryid_hook();
select plpgsql_profiler_install_fake_queryid_hook();
select plpgsql_profiler_remove_fake_queryid_hook();
select plpgsql_profiler_remove_fake_queryid_hook();

-- the coverage functions are not strict, so they check the argument
-- themselves
select plpgsql_coverage_statements(null::text);
select plpgsql_coverage_branches(null::text);
select plpgsql_coverage_statements(null::regprocedure);
select plpgsql_coverage_branches(null::regprocedure);

-- the profile of a function which was never executed while the profiler
-- was active is empty, and the function is not listed
create function pc_f1()
returns void as $$
begin
  raise exception 'pc_f1 failed';
end;
$$ language plpgsql;

do $$
begin
  perform pc_f1();
exception when others then
  raise notice 'catched: %', sqlerrm;
end;
$$;

select stmtid, exec_stmts, stmtname
  from plpgsql_profiler_function_statements_tb('pc_f1');

-- a function whose whole body is written on a single line has all the
-- statements on one row of the profile
set plpgsql_check.profiler to on;

create function pc_f2(a int) returns int as $$ declare b int; begin b := a; if b > 0 then b := -b; end if; return b; end $$ language plpgsql;

select pc_f2(1);

select lineno, stmt_lineno, exec_stmts, source
  from plpgsql_profiler_function_tb('pc_f2');

select plpgsql_coverage_statements('pc_f2(int)');
select plpgsql_coverage_branches('pc_f2(int)');

set plpgsql_check.profiler to off;

select plpgsql_profiler_reset_all();

drop function pc_f2(int);
drop function pc_f1();
