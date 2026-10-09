set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- Tests of the collection of the query identifiers by the profiler.
--
-- The profiler remembers the identifier of the query executed by a
-- statement. A fake hook assigns the command type as its identifier so
-- the expected output does not depend on PostgreSQL's query jumbling.
--
set plpgsql_check.use_shared_stats_when_it_possible to off;
set plpgsql_check.profiler to on;

select plpgsql_profiler_install_fake_queryid_hook();

create table pq_t1(a int, b int);

-- the queryid of a static query is taken from the cached plan of the
-- expression. Dynamic SQL has no retained plan, so its identifier is
-- unavailable rather than reconstructed by evaluating application code.
create function pq_f1()
returns void as $$
declare
  n int = 1;
  c int;
begin
  insert into pq_t1 values(1, 2);
  update pq_t1 set a = 10;
  select count(*) into c from pq_t1;
  delete from pq_t1;
  execute 'insert into pq_t1 values($1, $2)' using n, n + 1;
  execute 'update pq_t1 set a = $1' using n;
  execute 'delete from pq_t1';
  -- an empty dynamic query and a dynamic query with more than one
  -- statement have no identifier
  execute '';
  execute 'select 1; select 2';
end;
$$ language plpgsql;

select pq_f1();

select queryids, lineno, stmt_lineno, exec_stmts, source
  from plpgsql_profiler_function_tb('pq_f1');

select plpgsql_profiler_reset_all();

set plpgsql_check.profiler_show_dynquery_query_id to on;

select pq_f1();

select queryids, lineno, stmt_lineno, exec_stmts, source
  from plpgsql_profiler_function_tb('pq_f1');

select plpgsql_profiler_reset_all();

set plpgsql_check.profiler_show_dynquery_query_id to off;

-- Profiling must not re-evaluate SQL text or USING expressions.
create function pq_f2()
returns void as $$
declare r pq_t1;
begin
  r := (1, 2);
  execute 'select $1, $2' using r.*;
end;
$$ language plpgsql;

do $$
begin
  perform pq_f2();
exception when others then
  raise notice 'catched: %', sqlerrm;
end;
$$;

select queryids, lineno, stmt_lineno, exec_stmts, source
  from plpgsql_profiler_function_tb('pq_f2');

create function pq_into() returns text as $$
declare sql_text text := 'SELECT 42';
begin
  execute sql_text into sql_text;
  return sql_text;
end;
$$ language plpgsql;
create function pq_loop() returns int as $$
declare sql_text text := 'SELECT 42'; n int;
begin
  for n in execute sql_text loop
    sql_text := 'not sql';
  end loop;
  return n;
end;
$$ language plpgsql;
create function pq_open() returns int as $$
declare c refcursor; n int;
begin
  open c for execute coalesce(c::text, 'SELECT 42');
  fetch c into n;
  close c;
  return n;
end;
$$ language plpgsql;
create function pq_return() returns setof int as $$
begin
  return query execute case when found then 'not sql' else 'SELECT 42' end;
end;
$$ language plpgsql;
select pq_into(), pq_loop(), pq_open();
select * from pq_return();
select stmtname, queryid, exec_stmts
  from plpgsql_profiler_function_statements_tb('pq_into')
 where stmtname = 'EXECUTE';
drop function pq_into();
drop function pq_loop();
drop function pq_open();
drop function pq_return();

select plpgsql_profiler_remove_fake_queryid_hook();

-- without a provider of the identifiers no statement has one
create function pq_f3()
returns void as $$
begin
  insert into pq_t1 values(1, 2);
  execute 'delete from pq_t1';
end;
$$ language plpgsql;

select pq_f3();

select queryids, lineno, stmt_lineno, exec_stmts, source
  from plpgsql_profiler_function_tb('pq_f3');

select plpgsql_profiler_install_fake_queryid_hook();

-- Skipping a statement must retain its last observed query identifier.
create function pq_branch(take_branch boolean) returns void as $$
begin
  if take_branch then
    perform 42;
  end if;
end;
$$ language plpgsql;
select pq_branch(true);
select queryid, exec_stmts
  from plpgsql_profiler_function_statements_tb('pq_branch')
 where stmtname = 'PERFORM';
select pq_branch(false);
select queryid, exec_stmts
  from plpgsql_profiler_function_statements_tb('pq_branch')
 where stmtname = 'PERFORM';
select queryids, exec_stmts
  from plpgsql_profiler_function_tb('pq_branch')
 where source like '%perform 42%';

select plpgsql_profiler_reset_all();

set plpgsql_check.profiler to off;

select plpgsql_profiler_remove_fake_queryid_hook();

drop function pq_f3();
drop function pq_f2();
drop function pq_f1();
drop table pq_t1;
drop function pq_branch(boolean);

set plpgsql_check.use_shared_stats_when_it_possible to default;
