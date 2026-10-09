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

drop function pl_big();
drop function pl_f3();
drop function pl_f2();
drop function pl_f1(int);
drop table pl_t1;

-- CASE does not have the hypothetical ELSE branch used for IF.
create function pl_case(n int) returns int as $$
begin
  case n
    when 1 then return 10;
    when 2 then return 20;
  end case;
end;
$$ language plpgsql;
create function pl_case_else(n int) returns int as $$
begin
  case n
    when 1 then return 10;
    when 2 then return 20;
    else return 0;
  end case;
end;
$$ language plpgsql;
select pl_case(1), pl_case(2);
select pl_case_else(1), pl_case_else(2), pl_case_else(0);
select plpgsql_coverage_branches('pl_case'),
       plpgsql_coverage_branches('pl_case_else');
drop function pl_case(int);
drop function pl_case_else(int);

-- Row counts belong to the SQL/fetch/return statements, not to subsequent
-- assignments or enclosing control statements.
create table pl_row_data(a int);
create function pl_rows() returns setof int as $$
declare n int; c refcursor;
begin
  insert into pl_row_data values (1), (2), (3);
  n := 99;
  if n > 0 then
    perform * from pl_row_data;
  end if;
  update pl_row_data set a = a + 10 where a < 3;
  delete from pl_row_data where a = 3;
  execute 'select * from pl_row_data';
  select count(*) into n from pl_row_data;
  open c for select a from pl_row_data order by a;
  fetch c into n;
  move forward 1 from c;
  fetch c into n;
  close c;
  return next n;
  return query select a from pl_row_data order by a;
  return query execute 'select 7';
  return;
end;
$$ language plpgsql;

select count(*) from pl_rows();
select stmtid, stmtname, processed_rows
  from plpgsql_profiler_function_statements_tb('pl_rows');
select sum(processed_rows[1]) as processed_rows
  from plpgsql_profiler_function_tb('pl_rows');

drop function pl_rows();
drop table pl_row_data;

-- Error counts are a subset of execution counts, not additional executions.
create function pl_timing_error() returns int as $$
begin
  perform pg_sleep(0.01);
  raise exception 'controlled timing error';
end;
$$ language plpgsql;
create function pl_timing_catch() returns void as $$
begin
  perform pl_timing_error();
exception when raise_exception then
  null;
end;
$$ language plpgsql;

select pl_timing_catch();
select count(*) = 1 and bool_and(exec_stmts = 1 and exec_stmts_err = 1
                                and total_time > 0 and avg_time = total_time) as correct_average
  from plpgsql_profiler_function_statements_tb('pl_timing_catch')
 where stmtname = 'PERFORM';
select count(*) = 1 and bool_and(exec_stmts[1] = 1 and exec_stmts_err[1] = 1
                                and total_time[1] > 0 and avg_time[1] = total_time[1]) as correct_average
  from plpgsql_profiler_function_tb('pl_timing_catch')
 where source like '%perform pl_timing_error%';

drop function pl_timing_catch();
drop function pl_timing_error();

-- The types of the parameters of EXECUTE ... USING, which are cached for
-- the computation of the query identifier, have to stay valid when all
-- statistics are reset while the function is running. Else the types
-- cached for the first EXECUTE would be overwritten by the types of the
-- second one, and the first query would be analyzed with wrong types.
-- The cached types are used again only while the query identifier is
-- unknown, so this needs a server that does not compute query identifiers
-- (the default). The next test does not depend on it.
create function pl_reset_using() returns text as $$
declare
  r int;
  t text;
begin
  perform plpgsql_profiler_reset_all();
  for i in 1..2 loop
    execute 'select $1 + 1' into r using i;
    perform plpgsql_profiler_reset_all();
    execute 'select $1 || $2' into t using 'a'::text, 'b'::text;
  end loop;
  return t || r;
end;
$$ language plpgsql;

select pl_reset_using();

drop function pl_reset_using();

-- The statistics can be reset even while the profiler computes the query
-- identifier, because it evaluates the expression of the query string,
-- after the types of the parameters were cached.
create function pl_reset_in_expr() returns text as $$
begin
  perform plpgsql_profiler_reset_all();
  return '1';
end;
$$ language plpgsql stable;

create function pl_reset_using_expr() returns int as $$
declare
  r int;
begin
  execute 'select $1 + ' || pl_reset_in_expr() into r using 41;
  return r;
end;
$$ language plpgsql;

select pl_reset_using_expr();

drop function pl_reset_using_expr();
drop function pl_reset_in_expr();

select plpgsql_check_profiler(false);

select plpgsql_profiler_reset_all();

set plpgsql_check.max_stats_size to default;

-- Condition errors enter neither branch; errors in a body do enter it.
set plpgsql_check.profiler = on;
create function pl_condition(n int) returns int as $$
begin
  if 1 / n > 0 then
    return 1;
  end if;
  return 0;
end;
$$ language plpgsql;
create function pl_body_error(n int) returns int as $$
begin
  if n > 0 then
    perform 1 / (n - 1);
    return 1;
  end if;
  return 0;
end;
$$ language plpgsql;
create function pl_elsif_error(n int) returns int as $$
begin
  if n = 2 then
    return 2;
  elsif 1 / n > 0 then
    return 1;
  end if;
  return 0;
end;
$$ language plpgsql;
create function pl_caught_condition(n int) returns int as $$
begin
  if 1 / n > 0 then
    return 1;
  end if;
  return 0;
exception when division_by_zero then
  return -1;
end;
$$ language plpgsql;
create function pl_coverage_calls() returns void as $$
begin
  begin
    perform pl_condition(0);
  exception when division_by_zero then null;
  end;
  begin
    perform pl_body_error(1);
  exception when division_by_zero then null;
  end;
  perform pl_body_error(0);
  begin
    perform pl_elsif_error(0);
  exception when division_by_zero then null;
  end;
  perform pl_caught_condition(0);
end;
$$ language plpgsql;

select pl_coverage_calls();
select plpgsql_coverage_branches('pl_condition') = 0 as condition_ok,
       plpgsql_coverage_branches('pl_body_error') = 1 as body_ok,
       plpgsql_coverage_branches('pl_elsif_error') = 0 as elsif_ok,
       plpgsql_coverage_branches('pl_caught_condition') = 0.5 as caught_ok;
select pl_condition(1), pl_condition(-1),
       pl_elsif_error(2), pl_elsif_error(1), pl_elsif_error(-1);
select plpgsql_coverage_branches('pl_condition') = 1 as complete_if,
       plpgsql_coverage_branches('pl_elsif_error') = 1 as complete_elsif;
select plpgsql_profiler_reset_all();

-- Exercise both shared merge paths when the library is preloaded.
set plpgsql_check.use_shared_stats_when_it_possible = on;
set plpgsql_check.use_lxcache = off;
select pl_coverage_calls();
select plpgsql_coverage_branches('pl_condition') = 0 as condition_ok,
       plpgsql_coverage_branches('pl_body_error') = 1 as body_ok,
       plpgsql_coverage_branches('pl_elsif_error') = 0 as elsif_ok,
       plpgsql_coverage_branches('pl_caught_condition') = 0.5 as caught_ok;
select plpgsql_profiler_reset_all();

set plpgsql_check.use_lxcache = on;
select pl_coverage_calls();
select plpgsql_coverage_branches('pl_condition') = 0 as condition_ok,
       plpgsql_coverage_branches('pl_body_error') = 1 as body_ok,
       plpgsql_coverage_branches('pl_elsif_error') = 0 as elsif_ok,
       plpgsql_coverage_branches('pl_caught_condition') = 0.5 as caught_ok;
select pl_condition(1), pl_condition(-1),
       pl_elsif_error(2), pl_elsif_error(1), pl_elsif_error(-1);
select plpgsql_coverage_branches('pl_condition') = 1 as complete_if,
       plpgsql_coverage_branches('pl_elsif_error') = 1 as complete_elsif;
set plpgsql_check.profiler = off;
select plpgsql_profiler_reset_all();
drop function pl_condition(int);
drop function pl_body_error(int);
drop function pl_elsif_error(int);
drop function pl_caught_condition(int);
drop function pl_coverage_calls();
set plpgsql_check.use_lxcache = default;

set plpgsql_check.use_shared_stats_when_it_possible to default;
