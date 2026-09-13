set client_min_messages to warning;

create extension if not exists plpgsql_check;

set client_min_messages to notice;

--
-- Tests of the SQL callable entry points of plpgsql_check and of the
-- validation of their arguments.
--
-- Almost every option of the check functions is mandatory - it has a
-- default value, but an explicit NULL is rejected. Every such check is
-- executed here.
--

create function tf_f1(a int)
returns int as $$
begin
  return a;
end;
$$ language plpgsql;

create table tf_t1(a int, b int);

--
-- plpgsql_check_function
--
select * from plpgsql_check_function(null::regprocedure);
select * from plpgsql_check_function(null::text);

select * from plpgsql_check_function('tf_f1', relid := null);
select * from plpgsql_check_function('tf_f1', format := null);
select * from plpgsql_check_function('tf_f1', fatal_errors := null);
select * from plpgsql_check_function('tf_f1', other_warnings := null);
select * from plpgsql_check_function('tf_f1', performance_warnings := null);
select * from plpgsql_check_function('tf_f1', extra_warnings := null);
select * from plpgsql_check_function('tf_f1', security_warnings := null);
select * from plpgsql_check_function('tf_f1', compatibility_warnings := null);
select * from plpgsql_check_function('tf_f1', anyelememttype := null);
select * from plpgsql_check_function('tf_f1', anyenumtype := null);
select * from plpgsql_check_function('tf_f1', anyrangetype := null);
select * from plpgsql_check_function('tf_f1', anycompatibletype := null);
select * from plpgsql_check_function('tf_f1', anycompatiblerangetype := null);
select * from plpgsql_check_function('tf_f1', without_warnings := null);
select * from plpgsql_check_function('tf_f1', all_warnings := null);
select * from plpgsql_check_function('tf_f1', use_incomment_options := null);
select * from plpgsql_check_function('tf_f1', incomment_options_usage_warning := null);
select * from plpgsql_check_function('tf_f1', constant_tracing := null);

-- the two shortcuts of the set of enabled warnings are exclusive
select * from plpgsql_check_function('tf_f1', without_warnings := true, all_warnings := true);

-- the names of the transition tables can be used only for a trigger
-- function, which requires the relation
select * from plpgsql_check_function('tf_f1', oldtable := 'o');
select * from plpgsql_check_function('tf_f1', newtable := 'n');

-- the list of the pragmas can contain a NULL, such entry is ignored
select * from plpgsql_check_function('tf_f1', pragmas := array[null, 'echo:from a pragma']::text[]);

--
-- plpgsql_check_function_tb
--
select * from plpgsql_check_function_tb(null::regprocedure);
select * from plpgsql_check_function_tb(null::text);

select * from plpgsql_check_function_tb('tf_f1', relid := null);
select * from plpgsql_check_function_tb('tf_f1', fatal_errors := null);
select * from plpgsql_check_function_tb('tf_f1', other_warnings := null);
select * from plpgsql_check_function_tb('tf_f1', performance_warnings := null);
select * from plpgsql_check_function_tb('tf_f1', extra_warnings := null);
select * from plpgsql_check_function_tb('tf_f1', security_warnings := null);
select * from plpgsql_check_function_tb('tf_f1', compatibility_warnings := null);
select * from plpgsql_check_function_tb('tf_f1', anyelememttype := null);
select * from plpgsql_check_function_tb('tf_f1', anyenumtype := null);
select * from plpgsql_check_function_tb('tf_f1', anyrangetype := null);
select * from plpgsql_check_function_tb('tf_f1', anycompatibletype := null);
select * from plpgsql_check_function_tb('tf_f1', anycompatiblerangetype := null);
select * from plpgsql_check_function_tb('tf_f1', without_warnings := null);
select * from plpgsql_check_function_tb('tf_f1', all_warnings := null);
select * from plpgsql_check_function_tb('tf_f1', use_incomment_options := null);
select * from plpgsql_check_function_tb('tf_f1', incomment_options_usage_warning := null);
select * from plpgsql_check_function_tb('tf_f1', constant_tracing := null);

select * from plpgsql_check_function_tb('tf_f1', without_warnings := true, all_warnings := true);

select * from plpgsql_check_function_tb('tf_f1', oldtable := 'o');
select * from plpgsql_check_function_tb('tf_f1', newtable := 'n');

-- all_warnings enables every group of the warnings, without_warnings
-- disables them; the difference is visible on a function with an unused
-- variable
create function tf_f2()
returns void as $$
declare
  v int;
begin
  v := 1;
end;
$$ language plpgsql;

select * from plpgsql_check_function('tf_f2', all_warnings := true);
select * from plpgsql_check_function('tf_f2', without_warnings := true);
select level, message from plpgsql_check_function_tb('tf_f2', all_warnings := true);
select level, message from plpgsql_check_function_tb('tf_f2', without_warnings := true);

-- declaration locations must agree in tabular, JSON and XML output
create function tf_declarations()
returns void as $$
declare
  unused_value int;
  written_value int;
begin
  written_value := 1;
end;
$$ language plpgsql;

select lineno, statement, message
  from plpgsql_check_function_tb('tf_declarations');

select issue->'statement'->>'lineNumber' as lineno,
       issue->'statement'->>'text' as statement,
       issue->>'message' as message
  from plpgsql_check_function('tf_declarations', format => 'json') as r(doc),
       lateral jsonb_array_elements(doc::jsonb->'issues') as i(issue);

select xpath('/Function/Issue/Stmt/@lineno', doc::xml) as linenos,
       xpath('/Function/Issue/Stmt/text()', doc::xml) as statements
  from plpgsql_check_function('tf_declarations', format => 'xml') as r(doc);

drop function tf_declarations();

--
-- plpgsql_show_dependency_tb
--
select * from plpgsql_show_dependency_tb(null::regprocedure);
select * from plpgsql_show_dependency_tb(null::text);

select * from plpgsql_show_dependency_tb('tf_f1', relid := null);
select * from plpgsql_show_dependency_tb('tf_f1', anyelememttype := null);
select * from plpgsql_show_dependency_tb('tf_f1', anyenumtype := null);
select * from plpgsql_show_dependency_tb('tf_f1', anyrangetype := null);
select * from plpgsql_show_dependency_tb('tf_f1', anycompatibletype := null);
select * from plpgsql_show_dependency_tb('tf_f1', anycompatiblerangetype := null);

create function tf_f3()
returns void as $$
begin
  insert into tf_t1 values(1, 2);
  perform tf_f1(1);
end;
$$ language plpgsql;

select type, schema, name, params from plpgsql_show_dependency_tb('tf_f3');
select type, schema, name, params from plpgsql_show_dependency_tb('tf_f3()'::regprocedure);

--
-- plpgsql_make_pragma
--
select * from plpgsql_make_pragma(null::regprocedure);
select * from plpgsql_make_pragma('tf_f1(int)', relid := null);
select * from plpgsql_make_pragma('tf_f1(int)', fatal_errors := null);

create function tf_f4()
returns void as $$
declare r record;
begin
  create temp table tf_tmp as select 1 as a, 2 as b;
  select * from tf_tmp into r;
  raise notice '%', r.a;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('tf_f4()');

drop function tf_f4();
drop function tf_f3();
drop function tf_f2();
drop function tf_f1(int);
drop table tf_t1;
