# $PostgreSQL: pgsql/contrib/plpgsql_check/Makefile

MODULE_big = plpgsql_check
OBJS = format.o catalog.o tablefunc.o plpgsql_setup.o expr_walk.o assign.o check_expr.o report.o stmtwalk.o
DATA =  plpgsql_check--1.0--1.1.sql plpgsql_check--1.4.sql plpgsql_check--1.1--1.2.sql plpgsql_check--1.2--1.3.sql plpgsql_check--1.3--1.4.sql
EXTENSION = plpgsql_check

ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

REGRESS_OPTS = --dbname=$(PL_TESTDB)
REGRESS = plpgsql_check_passive plpgsql_check_active plpgsql_check_active-$(MAJORVERSION) plpgsql_check_passive-$(MAJORVERSION)

ifdef NO_PGXS
subdir = contrib/plpgsql_check
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif

ifeq ($(PORTNAME), darwin)
override CFLAGS += -undefined dynamic_lookup
endif

override CFLAGS += -I$(top_builddir)/src/pl/plpgsql/src
