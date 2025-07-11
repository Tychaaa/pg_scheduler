# contrib/scheduler/Makefile

MODULES = scheduler
EXTENSION = scheduler
DATA = scheduler--1.0.sql

ifdef USE_PGXS
  PG_CONFIG := pg_config
  PGXS := $(shell $(PG_CONFIG) --pgxs)
  include $(PGXS)
else
  subdir = contrib/scheduler
  top_builddir = ../..
  include $(top_builddir)/src/Makefile.global
  include $(top_srcdir)/contrib/contrib-global.mk
endif
