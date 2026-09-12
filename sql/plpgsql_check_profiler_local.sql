set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- Tests of the profiler with the statistics stored in the local memory
-- of the session.
--
-- When the shared memory was preallocated, the profiler stores the
-- statistics there, so the local variant of the storage is used only
-- when the shared memory is not available or when it is disabled by
-- the following option. The option is used here, so the same code is
-- tested in both configurations.
--
set plpgsql_check.use_shared_stats_when_it_possible to off;

-- the profiler can be enabled by a function too
select plpgsql_check_profiler(true);

create table pl_t1(a int, b int);

create function pl_f1(a int)
returns int as $$
declare b int = 0;
begin
  if a > 10 then
    b := a;
  else
    b := -a;
  end if;
  while b > 0 loop
    b := b - 1;
  end loop;
  return b;
end;
$$ language plpgsql;

-- there are no statistics before the first execution of the function,
-- but the statements are listed anyway
select stmtid, parent_stmtid, exec_stmts, stmtname
  from plpgsql_profiler_function_statements_tb('pl_f1');

select lineno, stmt_lineno, exec_stmts, source
  from plpgsql_profiler_function_tb('pl_f1');

select plpgsql_coverage_statements('pl_f1');
select plpgsql_coverage_branches('pl_f1');

-- only a part of the function is executed, so the coverage is partial
select pl_f1(20);

select stmtid, parent_stmtid, exec_stmts, stmtname
  from plpgsql_profiler_function_statements_tb('pl_f1');

select lineno, stmt_lineno, exec_stmts, source
  from plpgsql_profiler_function_tb('pl_f1');

select plpgsql_coverage_statements('pl_f1');
select plpgsql_coverage_branches('pl_f1');

-- the coverage functions accept the oid of the function too
select plpgsql_coverage_statements('pl_f1(int)'::regprocedure);
select plpgsql_coverage_branches('pl_f1(int)'::regprocedure);

-- the statistics of the second execution are merged with the first ones
-- and both branches of the IF statement are executed now
select pl_f1(5);

select stmtid, parent_stmtid, exec_stmts, stmtname
  from plpgsql_profiler_function_statements_tb('pl_f1');

select plpgsql_coverage_statements('pl_f1');
select plpgsql_coverage_branches('pl_f1');

-- a function without any conditional statement has no branches, and the
-- branch coverage of such function is 1
create function pl_f2()
returns void as $$
begin
  insert into pl_t1 values(1, 2);
end;
$$ language plpgsql;

select pl_f2();

select plpgsql_coverage_statements('pl_f2');
select plpgsql_coverage_branches('pl_f2');

-- the statistics of an aborted execution are collected too; the
-- anonymous block is profiled as well, but it is not listed by
-- plpgsql_profiler_functions_all(), because it has no entry in pg_proc
create function pl_f3()
returns void as $$
begin
  raise exception 'pl_f3 failed';
end;
$$ language plpgsql;

do $$
begin
  perform pl_f3();
exception when others then
  raise notice 'catched: %', sqlerrm;
end;
$$;

select funcoid, exec_count, exec_stmts_err
  from plpgsql_profiler_functions_all()
 order by funcoid::text;

-- the statistics of one function can be removed
select plpgsql_profiler_reset('pl_f1(int)');

select stmtid, parent_stmtid, exec_stmts, stmtname
  from plpgsql_profiler_function_statements_tb('pl_f1');

select funcoid, exec_count, exec_stmts_err
  from plpgsql_profiler_functions_all()
 order by funcoid::text;

-- ... and the statistics of all functions too
select plpgsql_profiler_reset_all();

select funcoid, exec_count, exec_stmts_err
  from plpgsql_profiler_functions_all()
 order by funcoid::text;

-- the size of the statistics is limited by a configuration option; when
-- the limit is reached, the statistics of the function are not stored
-- and a warning is raised
set plpgsql_check.max_stats_size to '64kB';

do $$
begin
  execute 'create function pl_big() returns void as $x$ begin ' ||
          repeat('perform 1;', 2000) ||
          'end; $x$ language plpgsql';
end;
$$;

select pl_big();

-- the function level statistics are collected even in this case
select funcoid, exec_count from plpgsql_profiler_functions_all()
 where funcoid = 'pl_big()'::regprocedure;

select stmtid, exec_stmts from plpgsql_profiler_function_statements_tb('pl_big')
 where stmtid = 1;

set plpgsql_check.max_stats_size to default;

select plpgsql_profiler_reset_all();

select plpgsql_check_profiler(false);

drop function pl_big();
drop function pl_f3();
drop function pl_f2();
drop function pl_f1(int);
drop table pl_t1;

set plpgsql_check.use_shared_stats_when_it_possible to default;
