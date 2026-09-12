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

-- when the CASE has no ELSE part, the execution path is closed only
-- possibly, although every WHEN part returns
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
