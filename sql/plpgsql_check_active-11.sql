LOAD 'plpgsql';
CREATE EXTENSION  IF NOT EXISTS plpgsql_check;
set client_min_messages to notice;

create or replace procedure proc(a int)
as $$
begin
end;
$$ language plpgsql;

call proc(10);

select * from plpgsql_check_function('proc(int)');

create or replace procedure testproc()
as $$
begin
  call proc(10);
end;
$$ language plpgsql;

call testproc();

select * from plpgsql_check_function('testproc()');

-- should to fail
create or replace procedure testproc()
as $$
begin
  call proc((select count(*) from pg_class));
end;
$$ language plpgsql;

call testproc();

select * from plpgsql_check_function('testproc()');

drop procedure proc(int);

create procedure proc(in a int, inout b int, in c int)
as $$
begin
end;
$$ language plpgsql;

select * from plpgsql_check_function('proc(int,int, int)');

create or replace procedure proc(in a int, inout b int, in c int)
as $$
begin
  b := a + c;
end;
$$ language plpgsql;

select * from plpgsql_check_function('proc(int,int, int)');

create or replace procedure testproc()
as $$
declare r int;
begin
  call proc(10, r, 20);
end;
$$ language plpgsql;

call testproc();

select * from plpgsql_check_function('testproc()');

-- should to fail
create or replace procedure testproc()
as $$
declare r int;
begin
  call proc(10, r + 10, 20);
end;
$$ language plpgsql;

call testproc();

select * from plpgsql_check_function('testproc()');

create or replace procedure testproc(inout r int)
as $$
begin
  call proc(10, r, 20);
end;
$$ language plpgsql;

call testproc(10);

select * from plpgsql_check_function('testproc(int)');

drop procedure testproc(int);

-- should to raise warnings
create or replace procedure testproc2(in p1 int, inout p2 int, in p3 int, inout p4 int)
as $$
begin
  raise notice '% %', p1, p3;
end;
$$ language plpgsql;

select * from plpgsql_check_function('testproc2');

drop procedure testproc2;

-- should be ok
create or replace procedure testproc3(in p1 int, inout p2 int, in p3 int, inout p4 int)
as $$
begin
  p2 := p1;
  p4 := p3;
end;
$$ language plpgsql;

select * from plpgsql_check_function('testproc3');

drop procedure testproc3;
