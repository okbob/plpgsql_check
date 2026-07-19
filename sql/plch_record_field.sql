load 'plpgsql';
set client_min_messages to warning;
create extension if not exists plpgsql_check;
set client_min_messages to notice;

set plpgsql_check.regress_test_mode = true;

--
-- issue #216 - checking a function that reads a field of a record
-- variable crashed when the type of the field is an anonymous record
-- (here a record typed OUT column)
--

create type rf_payload as (value integer);

create function rf_produce_row(
  out status_code integer,
  out payload record)
language plpgsql as $$
begin
  status_code := 0;
  payload := row(1)::rf_payload;
end
$$;

create function rf_consume_row()
returns void language plpgsql as $$
declare
  fetched_row record;
  typed_payload rf_payload;
  observed_status integer;
begin
  select * into strict fetched_row from rf_produce_row() as result_row;
  typed_payload := fetched_row.payload;
  observed_status := fetched_row.status_code;
end
$$;

select * from plpgsql_check_function('rf_consume_row()',
          extra_warnings := true);

select count(*)
from plpgsql_check_function_tb(
  'rf_consume_row()'::regprocedure,
  fatal_errors := false,
  other_warnings := true,
  performance_warnings := false,
  extra_warnings := true,
  security_warnings := false,
  compatibility_warnings := false,
  without_warnings := false,
  all_warnings := false,
  use_incomment_options := true,
  incomment_options_usage_warning := false,
  constant_tracing := true
);

-- same shape with a record variable as the assignment target
create function rf_consume_rec()
returns void language plpgsql as $$
declare
  fetched_row record;
  payload_rec record;
begin
  select * into strict fetched_row from rf_produce_row() as result_row;
  payload_rec := fetched_row.payload;
end
$$;

select * from plpgsql_check_function('rf_consume_rec()',
          extra_warnings := true);

-- reading only the scalar field worked before too, keep it checked
create function rf_consume_scalar()
returns integer language plpgsql as $$
declare
  fetched_row record;
begin
  select * into strict fetched_row from rf_produce_row() as result_row;
  return fetched_row.status_code;
end
$$;

select * from plpgsql_check_function('rf_consume_scalar()',
          extra_warnings := true);

drop function rf_consume_scalar();
drop function rf_consume_rec();
drop function rf_consume_row();
drop function rf_produce_row();
drop type rf_payload;
