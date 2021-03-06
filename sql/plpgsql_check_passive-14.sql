load 'plpgsql';
load 'plpgsql_check';
set client_min_messages to notice;

set plpgsql_check.mode = 'every_start';

create or replace procedure proc_test()
as $$
begin
  commit;
end;
$$ language plpgsql;

call proc_test();

drop procedure proc_test();
