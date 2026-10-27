load 'plpgsql_check';

set client_min_messages to warning;
create extension if not exists plpgsql_check;
set client_min_messages to notice;

create or replace function fx_extra_security()
returns int as $$
begin
  return (select count(relname) + 1 from pg_class);
end;
$$ language plpgsql;

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 1);

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

create or replace function fx_extra_security()
returns int as $$
begin
  return (select pg_catalog.count(relname) + 1 from pg_catalog.pg_class);
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 1);

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 3);

create or replace function fx_extra_security()
returns int as $$
begin
  return (select pg_catalog.count(c.relname) + 1 from pg_catalog.pg_class c);
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 3);

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 4);

create or replace function fx_extra_security()
returns int as $$
begin
  return (select pg_catalog.count(c.relname) OPERATOR(pg_catalog.+) 1 from pg_catalog.pg_class c);
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 4);

drop function fx_extra_security();

create or replace function fx_extra_security()
returns text as $$
begin
  return 'hello'::text;
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

create or replace function fx_extra_security()
returns text as $$
begin
  return 'hello'::pg_catalog.text;
end;
$$ language plpgsql
set search_path = 'pg_catalog';

select * from plpgsql_check_function('fx_extra_security', extra_security_warnings_level => 2);

drop function fx_extra_security();
