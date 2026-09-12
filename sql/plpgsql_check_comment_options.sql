--
-- regress tests of the in-comment options parser (src/parser.c)
--
-- plpgsql_check reads options from the source code of the checked
-- function, from lines tagged by "@plpgsql_check_options:" inside line
-- comments or block comments. Nothing of that parser was covered by the
-- regress tests.
--
load 'plpgsql_check';

set client_min_messages to warning;
create extension if not exists plpgsql_check;
set client_min_messages to notice;

-- Some errors reported by the options parser contain the oid of the
-- checked function. This wrapper replaces the oid, so the output is
-- stable.
create function co_check(fnname text, rel regclass default 0)
returns setof text as $$
begin
  return query select * from plpgsql_check_function(fnname, rel);
exception when others then
  return next 'ERROR: ' || regexp_replace(sqlerrm, 'fnoid: [0-9]+', 'fnoid: NNN');
end;
$$ language plpgsql;

--
-- boolean options
--

-- an option with no value is true
create function co_bool1()
returns void as $$
-- @plpgsql_check_options: extra_warnings
declare x int;
begin
  x := 1;
end;
$$ language plpgsql;

select * from co_check('co_bool1');

-- the value can be written with or without the equal sign, and all the
-- accepted spellings of true and false are used here
create function co_bool2()
returns void as $$
-- @plpgsql_check_options: extra_warnings = true, other_warnings = yes
-- @plpgsql_check_options: performance_warnings t, security_warnings on
declare x int;
begin
  x := 1;
end;
$$ language plpgsql;

select * from co_check('co_bool2');

create function co_bool3()
returns void as $$
-- @plpgsql_check_options: extra_warnings = false, other_warnings = no
-- @plpgsql_check_options: performance_warnings f, security_warnings off
-- @plpgsql_check_options: compatibility_warnings = off, fatal_errors = off
declare x int;
begin
  x := 1;
end;
$$ language plpgsql;

select * from co_check('co_bool3');

-- an option which is directly followed by a comma keeps its true value
create function co_bool4()
returns void as $$
-- @plpgsql_check_options: extra_warnings, other_warnings
declare x int;
begin
  x := 1;
end;
$$ language plpgsql;

select * from co_check('co_bool4');

-- all_warnings and without_warnings are handled at the very end of the
-- parsing, and they cannot be used together
create function co_all_warnings()
returns void as $$
-- @plpgsql_check_options: all_warnings
declare x int;
begin
  x := 1;
end;
$$ language plpgsql;

select * from co_check('co_all_warnings');

create function co_without_warnings()
returns void as $$
-- @plpgsql_check_options: without_warnings
declare x int;
begin
  x := 1;
end;
$$ language plpgsql;

select * from co_check('co_without_warnings');

create function co_warnings_conflict()
returns void as $$
-- @plpgsql_check_options: all_warnings = on, without_warnings = on
begin
end;
$$ language plpgsql;

select * from co_check('co_warnings_conflict');

-- the options can be disabled by the use_incomment_options argument
select * from plpgsql_check_function('co_warnings_conflict',
                                     use_incomment_options := false);

--
-- comment forms
--

-- a block comment, and more than one tagged line in a single block
-- comment
create function co_blockcomment()
returns void as $$
/*
 * some description of the function
 * @plpgsql_check_options: extra_warnings = off
 * @plpgsql_check_options: other_warnings = off
 */
declare x int;
begin
  x := 1;
end;
$$ language plpgsql;

select * from co_check('co_blockcomment');

-- The source code is scanned for the tag outside of the string
-- literals, the quoted identifiers and the dollar quoted strings, so the
-- tags written there are not options - if they were parsed, the check
-- would fail on an unsupported option. The only real tag is the one on
-- the last line, and it turns the extra warnings off; the same function
-- checked with use_incomment_options = false reports them.
create function co_skipping()
returns void as $func$
declare
  s1 text = 'a string with -- @plpgsql_check_options: nonsense and '' inside';
  s2 text = $tag$dollar quoted -- @plpgsql_check_options: nonsense$tag$;
  s3 text = $$another dollar quoted -- @plpgsql_check_options: nonsense$$;
  "quoted ""x"" -- @plpgsql_check_options: nonsense" int = 1;
  x int;
begin
  x := 1; -- some note @plpgsql_check_options: extra_warnings = off
  raise notice '%', coalesce(s1, s2, s3);
end;
$func$ language plpgsql;

select * from co_check('co_skipping');

select * from plpgsql_check_function('co_skipping',
                                     use_incomment_options := false);

--
-- name options
--

-- the argument of a name option can be an identifier, a quoted
-- identifier or a string. The names of the transition tables of a
-- trigger function are usually passed as arguments of
-- plpgsql_check_function(), the options replace them.
create table co_trig_tab(a int, b int);
create table co_trig_tab2(c int, d int);

create function co_newtable()
returns trigger as $$
-- @plpgsql_check_options: newtable = co_new
begin
  perform count(*) from co_new;
  return null;
end;
$$ language plpgsql;

select * from co_check('co_newtable', 'co_trig_tab');

create function co_oldtable()
returns trigger as $$
-- @plpgsql_check_options: oldtable "co_old", newtable = 'co_new'
begin
  perform count(*) from co_old;
  perform count(*) from co_new;
  return null;
end;
$$ language plpgsql;

select * from co_check('co_oldtable', 'co_trig_tab');

--
-- table option
--

-- relid is used for the resolution of the NEW and OLD records of a
-- trigger function. The relation given by the option replaces the
-- relation passed as an argument, so the check passes although the
-- fields used by the function do not exist in co_trig_tab.
create function co_relid()
returns trigger as $$
-- @plpgsql_check_options: relid = co_trig_tab2
begin
  if new.c > 10 then
    new.d := new.c;
  end if;
  return new;
end;
$$ language plpgsql;

select * from co_check('co_relid', 'co_trig_tab');

-- the value can be a qualified name, with quoted parts
create function co_relid_qualified()
returns trigger as $$
-- @plpgsql_check_options: relid = public."co_trig_tab2"
begin
  new.d := new.c;
  return new;
end;
$$ language plpgsql;

select * from co_check('co_relid_qualified', 'co_trig_tab');

--
-- type options
--

-- the polymorphic types are substituted by the types given by the
-- options, and a qualified type name is used too
create function co_anyelement(anyelement)
returns text as $$
-- @plpgsql_check_options: anyelementtype = text
begin
  return $1;
end;
$$ language plpgsql;

select * from co_check('co_anyelement');

create function co_anyelement2(anyelement)
returns text as $$
-- @plpgsql_check_options: anyelementtype pg_catalog.text
begin
  return $1;
end;
$$ language plpgsql;

select * from co_check('co_anyelement2');

create type co_enum as enum ('a', 'b');

create function co_anyenum(anyenum)
returns text as $$
-- @plpgsql_check_options: anyenumtype = co_enum
begin
  return $1::text;
end;
$$ language plpgsql;

select * from co_check('co_anyenum');

create function co_anyrange(anyrange)
returns text as $$
-- @plpgsql_check_options: anyrangetype = numrange
begin
  return lower($1)::text;
end;
$$ language plpgsql;

select * from co_check('co_anyrange');

create function co_anycompatible(anycompatible)
returns text as $$
-- @plpgsql_check_options: anycompatibletype = text
begin
  return $1;
end;
$$ language plpgsql;

select * from co_check('co_anycompatible');

create function co_anycompatiblerange(anycompatiblerange)
returns text as $$
-- @plpgsql_check_options: anycompatiblerangetype = numrange
begin
  return lower($1)::text;
end;
$$ language plpgsql;

select * from co_check('co_anycompatiblerange');

--
-- echo option
--

-- the argument of echo can be of any token type, and the @@name and
-- @@signature placeholders are substituted
create function co_echo(a int)
returns void as $$
-- @plpgsql_check_options: echo = 'a string with @@name'
-- @plpgsql_check_options: echo = 'a signature @@signature'
-- @plpgsql_check_options: echo = '@@unknown is not a placeholder'
-- @plpgsql_check_options: echo an_identifier
-- @plpgsql_check_options: echo = "a quoted identifier"
-- @plpgsql_check_options: echo = 10.1
-- @plpgsql_check_options: echo = *
begin
end;
$$ language plpgsql;

select * from co_check('co_echo');

--
-- syntax errors
--

create function co_err_unknown()
returns void as $$
-- @plpgsql_check_options: unknown_option = 1
begin
end;
$$ language plpgsql;

select * from co_check('co_err_unknown');

create function co_err_nooption()
returns void as $$
-- @plpgsql_check_options: = 1
begin
end;
$$ language plpgsql;

select * from co_check('co_err_nooption');

create function co_err_nocomma()
returns void as $$
-- @plpgsql_check_options: extra_warnings = on other_warnings = on
begin
end;
$$ language plpgsql;

select * from co_check('co_err_nocomma');

create function co_err_badbool()
returns void as $$
-- @plpgsql_check_options: extra_warnings = maybe
begin
end;
$$ language plpgsql;

select * from co_check('co_err_badbool');

create function co_err_nobool()
returns void as $$
-- @plpgsql_check_options: extra_warnings =
begin
end;
$$ language plpgsql;

select * from co_check('co_err_nobool');

create function co_err_notrange(anyrange)
returns void as $$
-- @plpgsql_check_options: anyrangetype = int
begin
end;
$$ language plpgsql;

select * from co_check('co_err_notrange');

create function co_err_notrange2(anycompatiblerange)
returns void as $$
-- @plpgsql_check_options: anycompatiblerangetype = int
begin
end;
$$ language plpgsql;

select * from co_check('co_err_notrange2');

create function co_err_badtype(anyelement)
returns void as $$
-- @plpgsql_check_options: anyelementtype = 'text'
begin
end;
$$ language plpgsql;

select * from co_check('co_err_badtype');

create function co_err_notype(anyelement)
returns void as $$
-- @plpgsql_check_options: anyelementtype
begin
end;
$$ language plpgsql;

select * from co_check('co_err_notype');

create function co_err_badname()
returns trigger as $$
-- @plpgsql_check_options: newtable = 10
begin
  return null;
end;
$$ language plpgsql;

select * from co_check('co_err_badname', 'co_trig_tab');

create function co_err_noname()
returns trigger as $$
-- @plpgsql_check_options: newtable
begin
  return null;
end;
$$ language plpgsql;

select * from co_check('co_err_noname', 'co_trig_tab');

create function co_err_badtable()
returns trigger as $$
-- @plpgsql_check_options: relid = 10
begin
  return null;
end;
$$ language plpgsql;

select * from co_check('co_err_badtable', 'co_trig_tab');

create function co_err_notable()
returns trigger as $$
-- @plpgsql_check_options: relid
begin
  return null;
end;
$$ language plpgsql;

select * from co_check('co_err_notable', 'co_trig_tab');

create function co_err_noecho()
returns void as $$
-- @plpgsql_check_options: echo
begin
end;
$$ language plpgsql;

select * from co_check('co_err_noecho');

create function co_err_noechovalue()
returns void as $$
-- @plpgsql_check_options: echo =
begin
end;
$$ language plpgsql;

select * from co_check('co_err_noechovalue');

drop function co_err_noechovalue();
drop function co_err_noecho();
drop function co_err_notable();
drop function co_err_badtable();
drop function co_err_noname();
drop function co_err_badname();
drop function co_err_notype(anyelement);
drop function co_err_badtype(anyelement);
drop function co_err_notrange2(anycompatiblerange);
drop function co_err_notrange(anyrange);
drop function co_err_nobool();
drop function co_err_badbool();
drop function co_err_nocomma();
drop function co_err_nooption();
drop function co_err_unknown();
drop function co_echo(int);
drop function co_anycompatiblerange(anycompatiblerange);
drop function co_anycompatible(anycompatible);
drop function co_anyrange(anyrange);
drop function co_anyenum(anyenum);
drop type co_enum;
drop function co_anyelement2(anyelement);
drop function co_anyelement(anyelement);
drop function co_relid_qualified();
drop function co_relid();
drop function co_oldtable();
drop function co_newtable();
drop table co_trig_tab2;
drop table co_trig_tab;
drop function co_skipping();
drop function co_blockcomment();
drop function co_warnings_conflict();
drop function co_without_warnings();
drop function co_all_warnings();
drop function co_bool4();
drop function co_bool3();
drop function co_bool2();
drop function co_bool1();
drop function co_check(text, regclass);
