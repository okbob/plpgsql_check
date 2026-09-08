load 'plpgsql';
create extension if not exists plpgsql_check;
set client_min_messages to notice;

set plpgsql_check.regress_test_mode = true;

set plpgsql_check.enable_tracer to on;
set plpgsql_check.tracer to on;
set plpgsql_check.trace_assert to on;
set plpgsql_check.tracer_test_mode to true;

create or replace function repro06_inner() returns void as $$
begin
  assert 1 = 2, 'boom';
end;
$$ language plpgsql;

create or replace function repro06_outer() returns void as $$
begin
  perform repro06_inner();
end;
$$ language plpgsql;

-- no outer error context frame - the loop is not entered, so this is safe
select repro06_inner();

-- called from another function, so error_context_stack->previous is set
select repro06_outer();

set plpgsql_check.tracer to off;
set plpgsql_check.trace_assert to off;
