# $PostgreSQL: pgsql/contrib/plpgsql_check/Makefile,v 1.1 2008/07/29 18:31:20 tgl Exp $

MODULE_big = plpgsql_check
OBJS = plpgsql_check.o
DATA = plpgsql_check--0.1.sql
EXTENSION = plpgsql_check

ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

REGRESS_OPTS = --dbname=$(PL_TESTDB) --load-language=plpgsql
REGRESS = plpgsql_check

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/plpgsql_check
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

override CFLAGS += -I$(top_builddir)/src/pl/plpgsql/src

