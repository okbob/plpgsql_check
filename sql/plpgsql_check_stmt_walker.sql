--
-- regress tests of generic PL/pgSQL statement walker (src/generic_stmt_walker.c)
--
-- The walker itself has no SQL interface. It is used by the profiler
-- (statement and branch coverage, per statement statistics, queryid
-- retrieval), so the profiler API is used here as a probe.
--
load 'plpgsql_check';

set client_min_messages to warning;
create extension if not exists plpgsql_check;
set client_min_messages to notice;

create table swt_tab(a int, b int);
insert into swt_tab values(1,1),(2,2),(3,3);

create procedure swt_proc(a int)
as $$
begin
end;
$$ language plpgsql;

--
-- A function using every statement type known to the walker. The
-- statements are executed, so the profiler collects statistics for all
-- of them.
--
create function swt_all_stmts(par int)
returns int as $$
declare
  i int;
  j int;
  r record;
  arr int[] = array[1, 2, 3];
  c refcursor;
  cc cursor(p int) for select a from swt_tab where a = p;
  res int = 0;
begin
  -- IF with two ELSIF branches and an ELSE branch
  if par = 1 then
    res := res + 1;
  elsif par = 2 then
    res := res + 2;
  elsif par = 3 then
    res := res + 3;
  else
    res := res + 4;
  end if;

  -- IF without ELSE branch (hypothetical else branch of coverage)
  if par > 100 then
    res := res + 1;
  end if;

  -- simple CASE - the tested expression is the statement's expression
  case par
    when 1 then res := res + 1;
    when 2 then res := res + 2;
    else res := res + 3;
  end case;

  -- searched CASE - has no tested expression
  case
    when par > 0 then res := res + 1;
    else res := res + 2;
  end case;

  -- unconditional LOOP terminated by conditional EXIT
  i := 0;
  loop
    i := i + 1;
    exit when i >= 2;
  end loop;

  -- WHILE loop
  while i < 4 loop
    i := i + 1;
  end loop;

  -- FOR over integer range, with an explicit BY step
  for j in reverse 10 .. 1 by 3 loop
    res := res + 1;
  end loop;

  -- FOR over a static query
  for r in select a, b from swt_tab order by a loop
    res := res + r.a;
  end loop;

  -- FOR over a bound cursor (the argument list is the statement's query)
  for r in cc(1) loop
    res := res + r.a;
  end loop;

  -- FOR over a dynamic query with USING parameters
  for r in execute 'select a from swt_tab where a > $1' using 0 loop
    res := res + r.a;
  end loop;

  -- FOREACH over an array
  foreach i in array arr loop
    res := res + i;
  end loop;

  -- OPEN of a bound cursor, FETCH, CLOSE
  open cc(2);
  fetch cc into r;
  close cc;

  -- OPEN of an unbound cursor for a static query
  open c for select a from swt_tab;
  fetch c into i;
  close c;

  -- OPEN of an unbound cursor for a dynamic query with USING parameters
  open c for execute 'select a from swt_tab where a > $1' using 0;
  fetch c into i;
  close c;

  -- PERFORM
  perform count(*) from swt_tab;

  -- CALL
  call swt_proc(1);

  -- plain SQL statements
  insert into swt_tab values(10, 10);
  update swt_tab set b = 11 where a = 10;
  delete from swt_tab where a = 10;

  -- dynamic SQL with USING parameters
  execute 'delete from swt_tab where a = $1' using -1;

  -- GET DIAGNOSTICS has no interesting substructure
  get diagnostics i = row_count;

  -- ASSERT with a message expression
  assert res is not null, 'res is null for ' || par;

  -- RAISE with format parameters and with options, handled by a nested
  -- block with an exception handler
  begin
    raise exception 'raised for %', par
      using errcode = 'division_by_zero', hint = 'no hint ' || par;
  exception when division_by_zero then
    res := res + 1;
  end;

  return res;
end;
$$ language plpgsql;

--
-- RETURN NEXT and both flavours of RETURN QUERY live in a set returning
-- function.
--
create function swt_setof()
returns setof int as $$
declare
  i int;
begin
  for i in 1 .. 2 loop
    return next i;
  end loop;

  return query select a from swt_tab order by a limit 1;
  return query execute 'select b from swt_tab where b = $1' using 2;

  return;
end;
$$ language plpgsql;

--
-- COMMIT and ROLLBACK are allowed in procedures only.
--
create procedure swt_txn()
as $$
begin
  commit;
  rollback;
end;
$$ language plpgsql;

set plpgsql_check.profiler to on;

select swt_all_stmts(1);
select swt_all_stmts(2);
select swt_all_stmts(10);

select * from swt_setof();

call swt_txn();

set plpgsql_check.profiler to off;

-- the walker is used to assign the statement statistics to the AST
select stmtid, parent_stmtid, block_num, lineno, exec_stmts, stmtname
  from plpgsql_profiler_function_statements_tb('swt_all_stmts');

select stmtid, parent_stmtid, block_num, lineno, exec_stmts, stmtname
  from plpgsql_profiler_function_statements_tb('swt_setof');

select stmtid, parent_stmtid, block_num, lineno, exec_stmts, stmtname
  from plpgsql_profiler_function_statements_tb('swt_txn');

-- every loop kind has to be recognized as a branch, and the walker has
-- to be able to return the body of every loop kind
select plpgsql_coverage_statements('swt_all_stmts');
select plpgsql_coverage_branches('swt_all_stmts');

select plpgsql_coverage_statements('swt_setof');
select plpgsql_coverage_branches('swt_setof');

select plpgsql_coverage_statements('swt_txn');
select plpgsql_coverage_branches('swt_txn');

--
-- The expression returned by the walker for a statement is used for the
-- queryid retrieval. The fake queryid hook stores the command type of the
-- query, so the result shows which statements carry a query.
--
create function swt_queryid()
returns void as $$
declare
  r record;
  c refcursor;
  cc cursor(p int) for select a from swt_tab where a = p;
  tabname text = 'swt_tab';
  i int;
begin
  insert into swt_tab values(20, 20);
  update swt_tab set b = 21 where a = 20;
  delete from swt_tab where a = 20;

  select count(*) into i from swt_tab;
  perform count(*) from swt_tab;

  for r in select a from swt_tab order by a loop
    null;
  end loop;

  for r in cc(1) loop
    null;
  end loop;

  for r in execute 'select a from ' || tabname loop
    null;
  end loop;

  open c for select a from swt_tab;
  fetch c into i;
  close c;

  open c for execute 'select a from ' || tabname;
  fetch c into i;
  close c;

  execute 'delete from ' || tabname || ' where a = $1' using -1;
end;
$$ language plpgsql;

create function swt_queryid_setof()
returns setof int as $$
declare
  tabname text = 'swt_tab';
begin
  return query select a from swt_tab order by a;
  return query execute 'select b from ' || tabname || ' order by b';
end;
$$ language plpgsql;

set plpgsql_check.profiler to on;

select plpgsql_profiler_reset_all();
select plpgsql_profiler_install_fake_queryid_hook();

select swt_queryid();
select * from swt_queryid_setof();

select plpgsql_profiler_remove_fake_queryid_hook();

select lineno, stmt_lineno, queryids, exec_stmts, source
  from plpgsql_profiler_function_tb('swt_queryid');

select lineno, stmt_lineno, queryids, exec_stmts, source
  from plpgsql_profiler_function_tb('swt_queryid_setof');

set plpgsql_check.profiler to off;

drop function swt_queryid_setof();
drop function swt_queryid();
drop procedure swt_txn();
drop function swt_setof();
drop function swt_all_stmts(int);
drop procedure swt_proc(int);
drop table swt_tab;
