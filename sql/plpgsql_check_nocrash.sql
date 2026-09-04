load 'plpgsql';
create extension if not exists plpgsql_check;
set client_min_messages to notice;

set plpgsql_check.regress_test_mode = true;

create table zero_columns();

create function assign_empty_tupdesc() returns int as $$
declare
  v int;
begin
  select * from zero_columns into v;
  execute 'select * from zero_columns' into v;
  return v;
end;
$$ language plpgsql;

select * from plpgsql_check_function('assign_empty_tupdesc()');

drop function assign_empty_tupdesc();
drop table zero_columns;


create function public.int_eq(int, int) returns bool as $$
  select $1 = $2;
$$ language sql;

create operator public.=== (
  leftarg = int, rightarg = int, procedure = public.int_eq
);

create function use_custom_operator() returns bool as $$
begin
  return 1 operator(public.===) 2;
end;
$$ language plpgsql;

select type, schema, name, params from plpgsql_show_dependency_tb('use_custom_operator()');

drop function use_custom_operator();
drop function public.int_eq cascade;


create function tracked_const_oob() returns void as $$
declare
  str text;
  res int;
begin
  -- assign a string constant, so the strconstvars array is allocated
  str := 'constant';

  -- the dynamic query has much more params than the function has datums
  execute 'select $40'
     into res
    using 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
          11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
          21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
          31, 32, 33, 34, 35, 36, 37, 38, 39, 'x'::text;

  raise notice '%, %', str, res;
end;
$$ language plpgsql;

select * from plpgsql_check_function('tracked_const_oob()');

drop function tracked_const_oob();


create table returned_expr_tab(a int);

create function returned_expr_null_node() returns setof refcursor as $$
begin
  return query select a::text::refcursor from returned_expr_tab;
end;
$$ language plpgsql;

select * from plpgsql_check_function('returned_expr_null_node()',
                                     compatibility_warnings => true);

drop function returned_expr_null_node();
drop table returned_expr_tab;


create table rvalue_tab(a int);

create function rvalue_null_node() returns void as $$
declare
  c refcursor;
begin
  c := (select a::text::refcursor from rvalue_tab limit 1);
  raise notice '%', c;
end;
$$ language plpgsql;

select * from plpgsql_check_function('rvalue_null_node()',
                                     compatibility_warnings => true);

drop function rvalue_null_node();
drop table rvalue_tab;


create table assert_column_tab(a int);

create function pragma_assert_column() returns void as $$
declare
  tname text default 'assert_column_tab';
begin
  -- ASSERT-COLUMN expects two variables, only one is passed
  perform plpgsql_check_pragma('assert-column: tname');
  raise notice '%', tname;
end;
$$ language plpgsql;

select * from plpgsql_check_function('pragma_assert_column()');

drop function pragma_assert_column();
drop table assert_column_tab;


create function pragma_sequence_prefix() returns void as $$
begin
  -- 6 chars long pragma name, the sequence handler reads 3 chars behind
  -- the end of the string
  perform plpgsql_check_pragma('SEQUEN');
end;
$$ language plpgsql;

select * from plpgsql_check_function('pragma_sequence_prefix()');

drop function pragma_sequence_prefix();

create function pragma_sequence_misdispatch() returns void as $$
begin
  perform plpgsql_check_pragma('SEQUENCE_FOO');
end;
$$ language plpgsql;

select * from plpgsql_check_function('pragma_sequence_misdispatch()');


drop function pragma_sequence_misdispatch();


create function profiled_function() returns int as $$
begin
  return 1;
end;
$$ language plpgsql;


set plpgsql_check.profiler to on;

begin;
select profiled_function();
select plpgsql_profiler_reset_all();
commit;

set plpgsql_check.profiler to off;

drop function profiled_function();


do $$
declare
  src text := '';
begin
  for i in 1..3000 loop
    src := src || 'perform 1;';
  end loop;

  execute format('create or replace function too_many_statements() returns void as $q$
begin
  if false then
    %s
  end if;
end $q$ language plpgsql', src);
end;
$$;


set plpgsql_check.profiler to off;

drop function too_many_statements();


create function dynamic_query_with_params(p int) returns void as $$
begin
  execute 'select 1 where $1 > 0' using p;
end;
$$ language plpgsql;

set plpgsql_check.profiler to on;

select dynamic_query_with_params(1);

set plpgsql_check.profiler to off;

drop function dynamic_query_with_params(int);


create function case_over_record() returns int as $$
declare
  r record;
begin
  case r
    when null then return 1;
    else return 2;
  end case;
end;
$$ language plpgsql;

select * from plpgsql_check_function('case_over_record()');

drop function case_over_record();


create table dynsql_tab(a int);

create function dynamic_sql_forms(tname text) returns void as $$
declare
  query text;
begin
  execute 'select * from dynsql_tab';
  execute format('select * from %I', tname);
  query := 'select * from ' || quote_ident(tname);
  execute query;
end;
$$ language plpgsql;

select * from plpgsql_check_function('dynamic_sql_forms(text)',
                                     all_warnings => true);

drop function dynamic_sql_forms(text);
drop table dynsql_tab;


create function uninitialized_is_mp(v text) returns void as $$
begin
  -- the %L placeholder makes the query non constant, so it is checked in a
  -- subtransaction, and the check fails because the table does not exist
  execute format('select * from missing_table where a = %L', v);
end;
$$ language plpgsql;

select * from plpgsql_check_function('uninitialized_is_mp(text)',
                                     all_warnings => true);

drop function uninitialized_is_mp(text);

create table traced_tab(a int);
insert into traced_tab values(1);

create or replace function traced_trg_func() returns trigger as $$
begin
  return old;
end;
$$ language plpgsql;

create trigger traced_trg before delete on traced_tab
  for each row execute procedure traced_trg_func();

set plpgsql_check.enable_tracer to on;
set plpgsql_check.tracer to on;
set plpgsql_check.tracer_test_mode to true;

delete from traced_tab;

set plpgsql_check.tracer to off;
set plpgsql_check.enable_tracer to off;

drop table traced_tab cascade;
drop function traced_trg_func();


create function dynamic_record_param() returns setof record as $$
begin
  -- the 12th USING argument is a record, so the tuple descriptor of the
  -- returned unpinned record is deduced from the param
  return query execute 'select $12'
                 using 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, row(1, 2);
end;
$$ language plpgsql;

select * from plpgsql_check_function('dynamic_record_param()');

drop function dynamic_record_param();


create function polymorphic_out_first(out b anyelement, a anyelement) as $$
begin
  b := a;
end;
$$ language plpgsql;

create function call_polymorphic_out_first() returns void as $$
declare
  arg record;
  res record;
begin
  select 1 as x, 2 as y into arg;
  res := polymorphic_out_first(arg);
  raise notice '%', res;
end;
$$ language plpgsql;

select * from plpgsql_check_function('call_polymorphic_out_first()');

drop function call_polymorphic_out_first();
drop function polymorphic_out_first(anyelement);
