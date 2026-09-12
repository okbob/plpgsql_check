set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- Tests of the collection of the query identifiers by the profiler.
--
-- The profiler remembers the identifier of the query executed by a
-- statement. The identifier is assigned by an extension like
-- pg_stat_statements, which is not available here, so a fake hook, which
-- assigns the type of the command as the identifier, is installed
-- instead.
--
set plpgsql_check.use_shared_stats_when_it_possible to off;
set plpgsql_check.profiler to on;

select plpgsql_profiler_install_fake_queryid_hook();

create table pq_t1(a int, b int);

-- the queryid of a static query is taken from the cached plan of the
-- expression; the queryid of a dynamic query is calculated by parsing of
-- the query string, and the types of the parameters passed by USING are
-- deduced from the expressions
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

-- when the type of a parameter of a dynamic query cannot be deduced,
-- the query is not parsed and the statement has no identifier
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

select plpgsql_profiler_reset_all();

set plpgsql_check.profiler to off;

drop function pq_f3();
drop function pq_f2();
drop function pq_f1();
drop table pq_t1;

set plpgsql_check.use_shared_stats_when_it_possible to default;
