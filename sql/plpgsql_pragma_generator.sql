load 'plpgsql';
set client_min_messages to warning;
create extension if not exists plpgsql_check;
set client_min_messages to notice;

set plpgsql_check.regress_test_mode = true;

--
-- tests of plpgsql_make_pragma and "pragmas" option
-- of check functions
--

create table gtp_src(a int, b text);
create type gtp_ct as (x int, y text);
create domain gtp_dom as int check (value > 0);

-- happy path - CREATE TEMP TABLE AS SELECT
create function gtp_f1()
returns void as $$
begin
  create temp table gtp_t1 as select a, b from gtp_src;
  insert into gtp_t1 values (10, 'hello');
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f1()');

-- without pragmas the check should detect missing relation
select * from plpgsql_check_function('gtp_f1()');

-- with generated pragmas the check should be ok
select * from plpgsql_check_function('gtp_f1()',
          pragmas => array(select plpgsql_make_pragma('gtp_f1()')));

-- tabular form
select * from plpgsql_check_function_tb('gtp_f1()',
          pragmas => array(select plpgsql_make_pragma('gtp_f1()')));

-- generator is idempotent - repeated call returns same result,
-- and doesn't leave any relation in system catalogue
select * from plpgsql_make_pragma('gtp_f1()');

select count(*) from pg_class where relname in ('gtp_t1');

-- happy path - one and two temporary tables created by AS VALUES and AS TABLE
create function gtp_f2()
returns void as $$
begin
  create temp table gtp_v1 as values (1, 'x'), (2, 'y');
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f2()');

create function gtp_f3()
returns void as $$
begin
  create temp table gtp_tt1 as table gtp_src;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f3()');

create function gtp_f4()
returns void as $$
begin
  create temp table gtp_v1 as values (1, 'x'), (2, 'y');
  create temp table gtp_tt1 as table gtp_src;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f4()');

drop function gtp_f2();
drop function gtp_f3();
drop function gtp_f4();

-- WITH NO DATA, IF NOT EXISTS and ON COMMIT variants
create function gtp_f5()
returns void as $$
begin
  create temp table gtp_nd as select a from gtp_src with no data;
  create temp table if not exists gtp_ine as select b from gtp_src;
  create temp table gtp_oc1 on commit preserve rows as select a from gtp_src;
  create temp table gtp_oc2 on commit delete rows as select a from gtp_src;
  create temp table gtp_oc3 on commit drop as select a from gtp_src;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f5()');

drop function gtp_f5();

-- explicitly entered column names
create function gtp_f6()
returns void as $$
begin
  create temp table gtp_cn(c1, c2) as select a, b from gtp_src;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f6()');

drop function gtp_f6();

-- star, join, subquery, CTE and recursive CTE
create function gtp_f7()
returns void as $$
begin
  create temp table gtp_star as select * from gtp_src;
  create temp table gtp_join as
    select s1.a, s2.b from gtp_src s1 join gtp_src s2 on s1.a = s2.a;
  create temp table gtp_sub as
    select a from gtp_src where a in (select a from gtp_src);
  create temp table gtp_cte as
    with x as (select a, b from gtp_src) select * from x;
  create temp table gtp_rcte as
    with recursive r(n) as (values (1) union all select n + 1 from r where n < 10)
    select n from r;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f7()');

drop function gtp_f7();

-- computed columns without aliases have name "?column?". Generated
-- pragma returns these names without any change (the pragma is not
-- applicable, and self registration fails with warning like CREATE
-- TABLE AS would fail).
create function gtp_f8()
returns void as $$
begin
  create temp table gtp_comp as select a + 1, a + 1 from gtp_src;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f8()');

drop function gtp_f8();

-- unknown type is replaced by text like CREATE TABLE AS does
create function gtp_f9()
returns void as $$
begin
  create temp table gtp_unk as select null as x;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f9()');

drop function gtp_f9();

-- array, composite, domain types and typmods
create function gtp_f10()
returns void as $$
begin
  create temp table gtp_types as
    select array[1,2] as arr,
           (1, 'x')::gtp_ct as comp,
           10::gtp_dom as dom,
           'abc'::varchar(10) as vc,
           3.14::numeric(12,2) as num;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f10()');

drop function gtp_f10();

-- identifiers that require quoting
create function gtp_f11()
returns void as $$
begin
  create temp table "MyT" as select 1 as "select", 2 as normal;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f11()');

drop function gtp_f11();

-- resjunk columns (ORDER BY) should not leak to generated pragma
create function gtp_f12()
returns void as $$
begin
  create temp table gtp_junk as select a from gtp_src order by b;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f12()');

drop function gtp_f12();

-- traversing of nested statements - IF/ELSE branches, loops, nested
-- blocks, exception handlers and unreachable code
create function gtp_f13()
returns void as $$
declare i int;
begin
  if random() > 0.5 then
    create temp table gtp_n1 as select 1 as x;
  else
    create temp table gtp_n2 as select 2 as y;
  end if;
  for i in 1..2
  loop
    create temp table gtp_n3 as select 3 as z;
  end loop;
  declare v int;
  begin
    create temp table gtp_n4 as select 4 as w;
  exception when others then
    create temp table gtp_n5 as select 5 as e;
  end;
  return;
  create temp table gtp_n6 as select 6 as u;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f13()');

drop function gtp_f13();

-- positional processing - same table name twice (no deduplication)
create function gtp_f14()
returns void as $$
begin
  create temp table gtp_dup as select 1 as x;
  drop table gtp_dup;
  create temp table gtp_dup as select 'a'::text as y;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f14()');

drop function gtp_f14();

-- one temporary table references another one
create function gtp_f15()
returns void as $$
begin
  create temp table gtp_c1 as select a, b from gtp_src;
  create temp table gtp_c2 as select * from gtp_c1 where a > 0;
  create temp table gtp_c3 as
    select gtp_c1.a, gtp_c2.b from gtp_c1 join gtp_c2 on gtp_c1.a = gtp_c2.a;
  create temp table gtp_c4 as
    select a from gtp_src where a in (select a from gtp_c1);
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f15()');

drop function gtp_f15();

-- temporary table shadows an existing table with same name
create table gtp_shadow(t text);

create function gtp_f16()
returns void as $$
begin
  create temp table gtp_shadow as select 1 as id;
  create temp table gtp_from_shadow as select * from gtp_shadow;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f16()');

drop function gtp_f16();
drop table gtp_shadow;

-- explicitly used pg_temp schema
create function gtp_f17()
returns void as $$
begin
  create temp table pg_temp.gtp_q1 as select 1 as z;
  create table pg_temp.gtp_q2 as select 2 as w;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f17()');

drop function gtp_f17();

-- these forms are ignored - column definition list, LIKE clause,
-- OF type clause, dynamic SQL, not temporary tables and materialized
-- views
create function gtp_f18()
returns void as $$
begin
  create temp table gtp_s1(id int, name text);
  create temp table gtp_s2(like gtp_src);
  create temp table gtp_s3 of gtp_ct;
  execute 'create temp table gtp_s4 as select 1 as x';
  create table gtp_s5 as select a from gtp_src;
  create unlogged table gtp_s6 as select a from gtp_src;
  create materialized view gtp_s7 as select a from gtp_src;
  drop table gtp_s1, gtp_s2, gtp_s3, gtp_s4, gtp_s5, gtp_s6;
  drop materialized view gtp_s7;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f18()');

drop function gtp_f18();

-- function without temporary tables
create function gtp_f19()
returns int as $$
begin
  return (select count(*) from gtp_src);
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f19()');

drop function gtp_f19();

-- zero column table - pragma is generated, but the table pragma
-- mechanism cannot to apply it (warning is expected)
create function gtp_f20()
returns void as $$
begin
  create temp table gtp_zero as select from gtp_src;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f20()');

drop function gtp_f20();

-- CREATE TABLE AS EXECUTE is ignored
create function gtp_f21()
returns void as $$
begin
  create temp table gtp_pe as execute some_prepared_stmt;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f21()');

drop function gtp_f21();

-- inner query can use plpgsql variables
create function gtp_f22()
returns void as $$
declare
  v_id int default 10;
  v_txt text default 'x';
begin
  create temp table gtp_vars as
    select v_id as id, v_txt as txt, a from gtp_src where a > v_id;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f22()');

drop function gtp_f22();

-- regprocedure argument selects one function from overloaded set,
-- function can use OUT arguments
create function gtp_over(p int)
returns void as $$
begin
  create temp table gtp_o1 as select p as x;
end;
$$ language plpgsql;

create function gtp_over(p text)
returns void as $$
begin
  create temp table gtp_o2 as select p as y;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_over(int)');
select * from plpgsql_make_pragma('gtp_over(text)');

drop function gtp_over(int);
drop function gtp_over(text);

create function gtp_out(in p int, out o1 int, out o2 text)
as $$
begin
  create temp table gtp_ot as select p + 1 as q;
  o1 := p; o2 := 'x';
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_out(int)');

drop function gtp_out(int);

-- trigger function requires relid argument
create table gtp_trg_tbl(id int, val text);

create function gtp_trg()
returns trigger as $$
begin
  create temp table gtp_trg_snapshot as select new.id, new.val;
  return new;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_trg()', 'gtp_trg_tbl');

drop function gtp_trg();
drop table gtp_trg_tbl;

-- explicitly written pragmas are respected by generator
create function gtp_f23()
returns void as $$
begin
  execute 'create temp table gtp_dyn(a int, b text)';
  perform plpgsql_check_pragma('table: gtp_dyn(a int, b text)');
  create temp table gtp_from_dyn as select * from gtp_dyn;
end;
$$ language plpgsql;

select * from plpgsql_make_pragma('gtp_f23()');

drop function gtp_f23();

-- error cases
create function gtp_err1()
returns void as $$
begin
  create temp table gtp_e1 as select * from gtp_nonexistent;
end;
$$ language plpgsql;

-- generator reports same error like plpgsql_check_function
select * from plpgsql_check_function('gtp_err1()');

\set VERBOSITY terse

select * from plpgsql_make_pragma('gtp_err1()');

\set VERBOSITY default

do $$
declare s text; m text;
begin
  perform plpgsql_make_pragma('gtp_err1()');
exception when others then
  get stacked diagnostics s = returned_sqlstate, m = message_text;
  raise notice 'sqlstate: %, message: %', s, m;
end;
$$;

drop function gtp_err1();

-- error is raised on first failed statement only, but when
-- fatal_errors is false, then the scanning continues
create function gtp_err2()
returns void as $$
begin
  create temp table gtp_e21 as select * from gtp_missing1;
  create temp table gtp_e22 as select * from gtp_missing2;
  create temp table gtp_e23 as select a from gtp_src;
end;
$$ language plpgsql;

do $$
declare s text; m text;
begin
  perform plpgsql_make_pragma('gtp_err2()');
exception when others then
  get stacked diagnostics s = returned_sqlstate, m = message_text;
  raise notice 'sqlstate: %, message: %', s, m;
end;
$$;

select * from plpgsql_make_pragma('gtp_err2()', fatal_errors => false);

drop function gtp_err2();

-- temporary table cannot be created in non-temporary schema
create function gtp_err3()
returns void as $$
begin
  create temporary table public.gtp_e3 as select 1::int, 'JOHN'::text;
end;
$$ language plpgsql;

do $$
declare s text; m text;
begin
  perform plpgsql_make_pragma('gtp_err3()');
exception when others then
  get stacked diagnostics s = returned_sqlstate, m = message_text;
  raise notice 'sqlstate: %, message: %', s, m;
end;
$$;

drop function gtp_err3();

-- same check is done for ignored forms too (column definition list,
-- LIKE clause, OF type clause), and it respects fatal_errors option
create function gtp_err4()
returns void as $$
begin
  create temp table public.gtp_e41 (id int);
  create temp table public.gtp_e42 (like gtp_src);
  create temp table public.gtp_e43 of gtp_ct;
  create temp table gtp_e44 as select a from gtp_src;
end;
$$ language plpgsql;

do $$
declare s text; m text;
begin
  perform plpgsql_make_pragma('gtp_err4()');
exception when others then
  get stacked diagnostics s = returned_sqlstate, m = message_text;
  raise notice 'sqlstate: %, message: %', s, m;
end;
$$;

select * from plpgsql_make_pragma('gtp_err4()', fatal_errors => false);

drop function gtp_err4();

-- too many column names
create function gtp_err5()
returns void as $$
begin
  create temp table gtp_e5(c1, c2, c3) as select a, b from gtp_src;
end;
$$ language plpgsql;

do $$
declare s text; m text;
begin
  perform plpgsql_make_pragma('gtp_err5()');
exception when others then
  get stacked diagnostics s = returned_sqlstate, m = message_text;
  raise notice 'sqlstate: %, message: %', s, m;
end;
$$;

drop function gtp_err5();

-- "pragmas" option of check functions
create function gtp_f24()
returns void as $$
begin
  insert into gtp_manual values (1, 'x');
end;
$$ language plpgsql;

-- NULL and empty array have no effect
select * from plpgsql_check_function('gtp_f24()', pragmas => null);
select * from plpgsql_check_function('gtp_f24()', pragmas => '{}'::text[]);

-- manually written (or fixed) pragma can be used, NULL elements
-- are ignored
select * from plpgsql_check_function('gtp_f24()',
          pragmas => array[null, 'table: gtp_manual(a int, b text)']);

-- broken pragma raises warning only, and check continues
select * from plpgsql_check_function('gtp_f24()',
          pragmas => array['table: gtp_manual(int int int)']);

-- any pragma type is allowed
select * from plpgsql_check_function('gtp_f24()',
          pragmas => array['disable: check']);

drop function gtp_f24();

drop table gtp_src;
drop type gtp_ct;
drop domain gtp_dom;
