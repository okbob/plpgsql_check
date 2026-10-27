set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- WHILE statement
--

-- the condition is checked like a scalar boolean expression
create function sw_f1(n int)
returns int as $$
declare i int := 0;
begin
  while i < n loop
    i := i + 1;
  end loop;
  return i;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f1');

-- a non boolean condition is reported
create function sw_f2(n int)
returns int as $$
declare i int := 0;
begin
  while i loop
    i := i + 1;
  end loop;
  return i;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f2');

-- the body of a loop is not necessarily executed, so a RETURN inside
-- the loop does not close the execution path
create function sw_f3(n int)
returns int as $$
begin
  while n > 0 loop
    return n;
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f3');

-- an unconditional loop is closed by the RETURN inside its body
create function sw_f4(n int)
returns int as $$
begin
  loop
    return n;
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f4');

-- the STEP expression of a numeric FOR loop is checked too
create function sw_f4b(n int)
returns int as $$
declare i int := 0;
begin
  for j in 1..n by 2 loop
    i := i + j;
  end loop;
  return i;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f4b');

--
-- names of the local variables
--

-- a reserved keyword used as a variable name, a local variable shadowing
-- a parameter and a local variable shadowing an outer local variable
create function sw_f4c(shadowed_par int)
returns int as $$
declare
  "table" int := 1;
  shadowed_var int := 2;
begin
  declare
    shadowed_par int := 3;
    shadowed_var int := 4;
  begin
    return "table" + shadowed_par + shadowed_var;
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f4c');

--
-- CASE statement
--

-- when the CASE has no ELSE part, and no WHEN part matches, then the
-- CASE_NOT_FOUND exception is raised, so the execution path is closed
-- when every WHEN part returns
create function sw_f5(n int)
returns int as $$
begin
  case n
    when 1 then return 10;
    when 2 then return 20;
  end case;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f5');

-- the CASE_NOT_FOUND exception can be handled
create function sw_f5b(n int)
returns int as $$
begin
  begin
    case n
      when 1 then return 10;
    end case;
  exception when case_not_found then
    return -1;
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f5b');

-- with the ELSE part the path is closed
create function sw_f6(n int)
returns int as $$
begin
  case n
    when 1 then return 10;
    else return 20;
  end case;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f6');

--
-- IF statement with ELSIF parts
--

-- every ELSIF condition is checked separately
create function sw_f7(n int)
returns int as $$
begin
  if n = 1 then
    return 10;
  elsif n then
    return 20;
  elsif n = 3 then
    return 30;
  else
    return 40;
  end if;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f7');

--
-- EXIT and CONTINUE statements
--

-- EXIT can leave a labeled statement block, CONTINUE needs a labeled loop
create function sw_f8(n int)
returns int as $$
declare i int := 0;
begin
  <<outer_block>>
  begin
    <<outer_loop>>
    for j in 1..n loop
      if j = 2 then
        continue outer_loop;
      end if;
      if j = 3 then
        exit outer_block;
      end if;
      i := i + j;
    end loop;
  end;
  return i;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f8');

--
-- RAISE statement
--

-- a condition name closes the execution path by the related exception
create function sw_f9()
returns int as $$
begin
  raise division_by_zero;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f9');

-- the errcode option is preferred against the condition name
create function sw_f10()
returns int as $$
begin
  raise division_by_zero using errcode = 'unique_violation';
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f10');

-- an errcode that cannot be evaluated at the check time
create function sw_f11(c text)
returns int as $$
begin
  raise exception 'broken' using errcode = c;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f11');

-- the doubled percent is not a placeholder, so the message needs no
-- parameter for it
create function sw_f12()
returns int as $$
begin
  raise notice '100%% done, value %', 1;
  return 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f12');

--
-- exception handlers
--

-- the raised exception is matched by the name of its category
create function sw_f13()
returns int as $$
begin
  begin
    raise division_by_zero;
  exception when data_exception then
    return -1;
  end;
  return 0;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f13', extra_warnings => true);

-- an exception that cannot be raised by the protected statements is
-- reported as an unused handler
create function sw_f14()
returns int as $$
begin
  begin
    raise division_by_zero;
  exception when unique_violation then
    return -1;
  end;
  return 0;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f14', extra_warnings => true);

-- a block whose body is closed by an exception and whose handler raises
-- another exception is closed by the exception of the handler
create function sw_f14b()
returns int as $$
begin
  begin
    raise division_by_zero;
  exception when division_by_zero then
    raise unique_violation;
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f14b', extra_warnings => true);

-- when the handlers only re-raise the caught exception, the block is
-- closed by the exceptions raised by the protected body
create function sw_f14c(n int)
returns int as $$
begin
  begin
    if n = 1 then
      raise division_by_zero;
    else
      raise unique_violation;
    end if;
  exception
    when division_by_zero then
      raise;
    when unique_violation then
      raise;
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f14c', extra_warnings => true);

--
-- RETURN statement
--

-- a function returning refcursor should return a refcursor variable
create function sw_f14d()
returns refcursor as $$
declare c text := 'some_cursor';
begin
  return c;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f14d', compatibility_warnings => true);

create function sw_f14e()
returns refcursor as $$
declare c refcursor;
begin
  open c for select 1;
  return c;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f14e', compatibility_warnings => true);

--
-- RETURN NEXT statement
--

-- the returned value is a single OUT variable
create function sw_f15(out a int)
returns setof int as $$
begin
  a := 1;
  return next;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f15');

-- more OUT variables are collected into a row
create function sw_f16(out a int, out b int)
returns setof record as $$
begin
  a := 1; b := 2;
  return next;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f16');

-- the types of the row fields have to match the result type
create table sw_tab1(a int, b int);

create function sw_f17(out a int, out b text)
returns setof record as $$
begin
  a := 1; b := 'x';
  return next;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f17');

-- a record variable is returned
create function sw_f18()
returns setof sw_tab1 as $$
declare r sw_tab1;
begin
  r := (1,2);
  return next r;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f18');

--
-- transaction control statements
--

-- COMMIT and ROLLBACK are allowed in a procedure only
create function sw_f19()
returns void as $$
begin
  commit;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f19');

create procedure sw_p1()
as $$
begin
  commit;
  rollback;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_p1');

-- inside a block with an exception handler a subtransaction is active,
-- so neither COMMIT nor ROLLBACK can be used
create procedure sw_p2()
as $$
begin
  begin
    commit;
  exception when others then
    null;
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_p2');

create procedure sw_p3()
as $$
begin
  begin
    rollback;
  exception when others then
    null;
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_p3');

--
-- dynamic SQL
--

-- OPEN FOR EXECUTE is checked like the other dynamic statements
create function sw_f20(p text)
returns int as $$
declare
  c refcursor;
  r int;
begin
  open c for execute p using 1;
  fetch c into r;
  close c;
  return r;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f20');

-- when the query text is not known, the result of the dynamic SQL cannot
-- be determined, and the record variable stays without a descriptor
create function sw_f21(p text)
returns int as $$
declare r record;
begin
  execute p into r;
  return r.x;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f21');

-- the expression of EXECUTE is built by a function which does not
-- sanitize its argument, and the position of the unsafe value is unknown
create function sw_f21b(p text)
returns void as $$
begin
  execute upper(p);
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f21b', security_warnings => true);

-- a record variable with a declared type is known, so no warning is raised
create function sw_f22(p text)
returns int as $$
declare r sw_tab1;
begin
  execute p into r;
  return r.a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_f22');

drop function sw_f1(int);
drop function sw_f2(int);
drop function sw_f3(int);
drop function sw_f4(int);
drop function sw_f4b(int);
drop function sw_f4c(int);
drop function sw_f5(int);
drop function sw_f5b(int);
drop function sw_f6(int);
drop function sw_f7(int);
drop function sw_f8(int);
drop function sw_f9();
drop function sw_f10();
drop function sw_f11(text);
drop function sw_f12();
drop function sw_f13();
drop function sw_f14();
drop function sw_f14b();
drop function sw_f14c(int);
drop function sw_f14d();
drop function sw_f14e();
drop function sw_f15();
drop function sw_f16();
drop function sw_f17();
drop function sw_f18();
drop function sw_f19();
drop procedure sw_p1();
drop procedure sw_p2();
drop procedure sw_p3();
drop function sw_f20(text);
drop function sw_f21(text);
drop function sw_f21b(text);
drop function sw_f22(text);
drop table sw_tab1;

-- Bare RAISE needs an active handler, including through nested blocks
create function sw_bare_raise() returns int as $$
begin
  raise;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_bare_raise');

create function sw_handler_raise() returns void as $$
begin
  raise division_by_zero;
exception when division_by_zero then
  begin
    raise;
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_handler_raise');

drop function sw_bare_raise();
drop function sw_handler_raise();

-- MOVE has no result target, even when the cursor query has several columns
create function sw_move(distance int) returns void as $$
declare c cursor for select 1 as a, 2 as b;
begin
  open c;
  move forward distance from c;
  close c;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_move(int)');

drop function sw_move(int);

-- Check before execution initializes the hidden CASE variable's type
create type sw_single_field as (a int);
create function sw_case_single(v sw_single_field) returns int as $$
begin
  case v
    when row(1)::sw_single_field then return 1;
    else return 2;
  end case;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_case_single(sw_single_field)');
select sw_case_single(row(1)::sw_single_field) = 1
   and sw_case_single(row(2)::sw_single_field) = 2 as valid;

drop function sw_case_single(sw_single_field);
drop type sw_single_field;

-- Earlier exception paths must remain visible when later paths are merged
create function sw_exception_union(p boolean) returns int as $$
begin
  begin
    if p then
      raise division_by_zero;
    else
      raise unique_violation;
    end if;
  exception
    when division_by_zero then null;
    when unique_violation then return 2;
  end;
  return 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_exception_union(boolean)');

drop function sw_exception_union(boolean);

-- A first-handler rethrow keeps the caught SQLSTATE for outer handlers
create function sw_nested_rethrow() returns int as $$
begin
  begin
    begin
      raise division_by_zero;
    exception when division_by_zero then
      raise;
    end;
  exception when division_by_zero then
    null;
  end;
  return 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_nested_rethrow()');

drop function sw_nested_rethrow();

-- Dead exits must not change which exception handler is reachable
create function sw_dead_flow() returns int as $$
begin
  begin
    raise division_by_zero;
    raise unique_violation;
  exception
    when division_by_zero then return 1;
    when unique_violation then null;
  end;
end;
$$ language plpgsql;

select sqlstate, message from plpgsql_check_function_tb('sw_dead_flow()');

-- Expressions in dead statements still need to be checked
create function sw_dead_expression() returns int as $$
begin
  return 1;
  perform sw_missing_dead_column;
end;
$$ language plpgsql;

select exists (
  select from plpgsql_check_function_tb('sw_dead_expression()')
  where sqlstate = '42703') as checked;

drop function sw_dead_flow();
drop function sw_dead_expression();


-- Pragmas in one branch or handler must not affect its siblings
create function sw_pragma_siblings(p boolean) returns void as $$
begin
  if p then
    perform plpgsql_check_pragma('disable:check');
  else
    perform sw_missing_else_column;
  end if;
  case when p then
    perform plpgsql_check_pragma('disable:check');
  else
    perform sw_missing_case_column;
  end case;
  begin
    raise division_by_zero;
  exception
    when unique_violation then
      perform plpgsql_check_pragma('disable:check');
    when division_by_zero then
      perform sw_missing_handler_column;
  end;
end;
$$ language plpgsql;

select count(*) = 3 as sibling_checks
from plpgsql_check_function_tb('sw_pragma_siblings(boolean)', fatal_errors => false)
where sqlstate = '42703';

drop function sw_pragma_siblings(boolean);

-- Check dynamic cursors before execution can prime record-field metadata
create function sw_dynamic_cursor() returns int as $$
declare c refcursor; r record;
begin
  open c for execute 'select 1 as a';
  fetch c into r;
  close c;
  return r.a;
end;
$$ language plpgsql;

create function sw_parameter_cursor(v int) returns int as $$
declare c refcursor; r record;
begin
  open c for execute 'select $1::int as a' using v;
  fetch c into r;
  close c;
  return r.a;
end;
$$ language plpgsql;

-- A later static OPEN supersedes the earlier dynamic query
create function sw_cursor_reopen(v int) returns int as $$
declare c refcursor; r record;
begin
  open c for execute 'select $1::int as a' using v;
  fetch c into r;
  close c;
  open c for select v as b;
  fetch c into r;
  close c;
  return r.b;
end;
$$ language plpgsql;

-- FETCH into scalar targets also invalidates their previous traced constants
create function sw_dynamic_scalar() returns void as $$
declare c refcursor; s text := 'select sw_missing_dynamic_column';
begin
  open c for execute $q$select 'select 1'::text$q$;
  fetch c into s;
  close c;
  execute s;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_dynamic_cursor()');
select * from plpgsql_check_function('sw_parameter_cursor(int)');
select * from plpgsql_check_function('sw_cursor_reopen(int)');
select * from plpgsql_check_function('sw_dynamic_scalar()');
select sw_dynamic_cursor() = 1 and sw_parameter_cursor(3) = 3
   and sw_cursor_reopen(4) = 4 as valid;
do $$ begin perform sw_dynamic_scalar(); end; $$;

drop function sw_dynamic_cursor();
drop function sw_parameter_cursor(int);
drop function sw_cursor_reopen(int);
drop function sw_dynamic_scalar();

-- FETCH from a cursor that is not opened in the function (so its query
-- is unknown) has to be checked without crash, also when there is no
-- OPEN FOR EXECUTE in the function. The passive mode does the same check.
create function sw_fetch_unbound(c refcursor) returns int as $$
declare v int;
begin
  fetch c into v;
  return v;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_fetch_unbound(refcursor)');

set plpgsql_check.mode = 'every_start';
begin;
declare sw_fetch_cur cursor for select 1;
select sw_fetch_unbound('sw_fetch_cur');
commit;
reset plpgsql_check.mode;

drop function sw_fetch_unbound(refcursor);

create function sw_exit_loop(p boolean)
returns int as $$
begin
  loop
    if p then
      exit;
    end if;
    return 1;
  end loop;
  return 2;
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_exit_loop(boolean)');

drop function sw_exit_loop(boolean);

create function sw_exit_loop(p boolean)
returns int as $$
begin
  loop
    exit when p;
    return 1;
  end loop;
  return 2;
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_exit_loop');

drop function sw_exit_loop(boolean);

create function sw_exit_nested(p boolean)
returns int as $$
begin
  <<outer_loop>>
  loop
    loop
      exit outer_loop when p;
      return 1;
    end loop;
  end loop;
  return 2;
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_exit_nested');

drop function sw_exit_nested(boolean);

create function sw_exit_block(p boolean)
returns int as $$
begin
  <<target>>
  begin
    exit target when p;
    return 1;
  end;
  return 2;
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_exit_block');

drop function sw_exit_block(boolean);

create function sw_exit_missing(p boolean)
returns int as $$
begin
  loop
    exit when p;
    return 1;
  end loop;
  return 1;
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_exit_missing');

drop function sw_exit_missing(boolean);

create function sw_exit_inner()
returns int as $$
begin
  loop
    loop
      exit;
    end loop;
    return 1;
  end loop;
  return 2;
end;
$$ language plpgsql;

-- should be warning
select * from plpgsql_check_function('sw_exit_inner');

drop function sw_exit_inner();

create function sw_continue_path()
returns int as $$
declare i int := 0;
begin
  loop
    i := i + 1;
    continue when i = 1;
    return i;
  end loop;
  return 3;
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_continue_path');

drop function sw_continue_path();

create function sw_continue_outer()
returns int as $$
begin
  <<outer_loop>>
  for i in 1..2 loop
    loop
      continue outer_loop;
    end loop;
  end loop;
  return 1;
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_continue_outer');

drop function sw_continue_outer();

create function sw_dead_exit()
returns int as $$
begin
  loop
    return 1;
    exit;
  end loop;
  return 2;
end;
$$ language plpgsql;

-- should be warning
select * from plpgsql_check_function('sw_dead_exit');

drop function sw_dead_exit();

create function sw_handler_exit()
returns int as $$
begin
  loop
    begin
      raise division_by_zero;
    exception
      when unique_violation then exit;
      when division_by_zero then return 1;
    end;
  end loop;
  return 2;
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_handler_exit');

drop function sw_handler_exit();

create or replace function repro05_dynsql_query()
returns void as $$
begin
  execute 'select 1' || undefined_var;
end;
$$ language plpgsql;

create or replace function repro05_dynsql_param()
returns void as $$
begin
  execute 'select $1' using undefined_var;
end;
$$ language plpgsql;

create or replace function repro05_raise_errcode()
returns void as $$
begin
  raise exception 'x' using errcode = undefined_var;
end;
$$ language plpgsql;

select * from plpgsql_check_function('repro05_dynsql_query()', fatal_errors => false);
select * from plpgsql_check_function('repro05_dynsql_param()', fatal_errors => false);
select * from plpgsql_check_function('repro05_raise_errcode()', fatal_errors => false);

drop function repro05_dynsql_query();
drop function repro05_dynsql_param();
drop function repro05_raise_errcode();

-- expressions with FROM clause are not simple expressions, should not crash
create table sw_from_tab(q text);

create function sw_from_expr()
returns void as $$
declare
  x text;
  r record;
begin
  execute q from sw_from_tab;
  x := q from sw_from_tab;
  execute 'select ' || x;
  for r in execute q from sw_from_tab
  loop
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_from_expr', security_warnings => true);

drop function sw_from_expr();
drop table sw_from_tab;

-- an error in an expression should not cause other bogus errors
-- when fatal_errors is false
create procedure sw_nf_proc(inout a int)
as $$
begin
end;
$$ language plpgsql;

create function sw_nonfatal(p int)
returns setof int as $$
declare
  c refcursor;
  c2 cursor(a int) for select a;
  v int;
begin
  raise notice 'x' using errcode = nonexisting_errcode;
  raise notice 'x' using errcode = (1/0)::text;
  execute nonexisting_query into v;
  execute 'select $1' using nonexisting_param;
  execute (1/0)::text;
  open c for execute nonexisting_query;
  return query execute nonexisting_query;
  call sw_nf_proc(nonexisting_arg);
  open c for select 1/0;
  for r in c2(nonexisting_arg)
  loop
    raise notice '%', r.a + nonexisting_var;
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_nonfatal', fatal_errors => false);

drop function sw_nonfatal(int);
drop procedure sw_nf_proc(int);

-- WHEN OTHERS catches the exception, the code after the block is reachable
create function sw_others1(p int)
returns int as $$
begin
  begin
    raise exception 'error %', p;
  exception when others then
    raise notice 'caught';
  end;
  return 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_others1');

-- the end of the function is reachable, the missing RETURN is reported
create function sw_others2(p int)
returns int as $$
begin
  begin
    raise exception 'error %', p;
  exception when others then
    raise notice 'caught';
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_others2');

drop function sw_others1(int);
drop function sw_others2(int);

-- returned value or query without columns
create function sw_rq0()
returns table(a int) as $$
begin
  return query select;
end;
$$ language plpgsql;

-- should fail, the structure of query doesn't match
select * from plpgsql_check_function('sw_rq0');

create type sw_rr_type as (a int, b int);

create function sw_rr0()
returns sw_rr_type as $$
begin
  return row();
end;
$$ language plpgsql;

-- should fail, the structure of returned record doesn't match
select * from plpgsql_check_function('sw_rr0');

create function sw_rr1()
returns record as $$
begin
  return row();
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_rr1');

create function sw_rr2()
returns text as $$
begin
  return row();
end;
$$ language plpgsql;

-- is ok
select * from plpgsql_check_function('sw_rr2');

drop function sw_rq0();
drop function sw_rr0();
drop function sw_rr1();
drop function sw_rr2();
drop type sw_rr_type;

-- the plan checks should be raised only once
create table sw_plan_tab(a int primary key);

create function sw_plan_checks(p int)
returns int as $$
declare x int;
begin
  x := (select a from sw_plan_tab where a = p for update);
  raise exception using errcode = (select 'P0001' from sw_plan_tab where a = p for update);
  return x;
end;
$$ language plpgsql stable;

select * from plpgsql_check_function('sw_plan_checks', fatal_errors => false);

drop function sw_plan_checks(int);
drop table sw_plan_tab;


-- the structure of result of function returning setof record is not
-- known, and the scalar value is casted to the column of composite type
-- with one column
create type sw_onecol as (a int);

create function sw_rn_record()
returns setof record as $$
declare r record;
begin
  for r in select 1 as a, 2 as b
  loop
    return next r;
  end loop;
  return;
end;
$$ language plpgsql;

create function sw_rn_record_scalar()
returns setof record as $$
declare v int := 1;
begin
  return next v;
  return;
end;
$$ language plpgsql;

create function sw_rn_onecol()
returns setof sw_onecol as $$
declare
  v int := 1;
  d date := current_date;
begin
  return next v;
  -- should be reported
  return next d;
  return;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_rn_record()');
select * from plpgsql_check_function('sw_rn_record_scalar()');
select * from plpgsql_check_function('sw_rn_onecol()');

drop function sw_rn_record();
drop function sw_rn_record_scalar();
drop function sw_rn_onecol();
drop type sw_onecol;

--
-- RETURN of trigger variable (PROMISE datum) should be checked
-- like RETURN of any other scalar variable
--
create table sw_trg_t(a int);

create function sw_trg_ret_promise()
returns trigger as $$
begin
  return tg_op;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_trg_ret_promise()', 'sw_trg_t');

drop function sw_trg_ret_promise();
drop table sw_trg_t;

--
-- the FOUND variable is declared only once, in the namespace of the
-- function
--

-- the shadowing of the parameter is reported only once
create function sw_found_param(found int)
returns int as $$
begin
  begin
    return 1;
  end;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_found_param(int)');

-- the local variable shadows the FOUND variable
create function sw_found_var()
returns int as $$
declare found boolean := false;
begin
  begin
    perform 1;
  end;
  if not found then
    return 0;
  end if;
  return 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_found_var()');

drop function sw_found_param(int);
drop function sw_found_var();

-- the label used by EXIT and CONTINUE is not checked again
create function sw_label_keyword()
returns void as $$
begin
  <<table>>
  loop
    exit table when random() > 0.5;
    continue table;
  end loop;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_label_keyword()');

drop function sw_label_keyword();

-- the diagnostics raised after the check of nested statements are
-- related to the right statement
create function sw_err_stmt(a int)
returns int as $$
begin
  return 1;
  if a > 0 then
    a := a + 1;
    a := a + 2;
  end if;
  case
    when a > 0 then
      a := a + 1;
    when a + nonexisting_col > 0 then
      a := 0;
    else
      null;
  end case;
  return a;
end;
$$ language plpgsql;

select * from plpgsql_check_function('sw_err_stmt(int)', fatal_errors => false);

drop function sw_err_stmt(int);
