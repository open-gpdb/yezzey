# gpcontrib/yezzey/Makefile

override CFLAGS = -Wall -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Wno-format-truncation  -g -ggdb -std=gnu99 -Werror=uninitialized -Werror=implicit-function-declaration -DGPBUILD

COMMON_LINK_OPTIONS = -lstdc++

COMMON_CPP_FLAGS = -std=c++11 -fPIC -I/usr/include/libxml2 -I/usr/local/opt/openssl/include -DENABLE_NLS 

override CPPFLAGS = -fPIC -lstdc++ -g3 -ggdb -Wall -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Werror=format-security -Wno-format-truncation -g -std=c++11 -fPIC -Iinclude -Ilib -g -I. -I../../src/include -D_GNU_SOURCE

SHLIB_LINK += $(COMMON_LINK_OPTIONS)
PG_CPPFLAGS += $(COMMON_CPP_FLAGS) -I./include -Iinclude -Ilib -I$(libpq_srcdir) -I$(libpq_srcdir)/postgresql/server/utils

MODULE_big = yezzey

OBJS = \
	$(WIN32RES) \
	src/storage.o src/proxy.o \
	src/virtual_index.o \
	src/expire_hint.o \
	src/util.o \
	src/url.o \
	src/io.o \
	src/io_adv.o \
	src/offload_tablespace_map.o \
	src/offload_policy.o \
	src/offload.o \
	src/virtual_tablespace.o \
	src/partition.o \
	src/xvacuum.o \
	src/yproxy.o \
	src/meta.o \
	src/binary_upgrade.o \
	src/msgproto.o \
	src/yproxy_connector.o \
	src/yproxy_deleter.o \
	src/yproxy_lister.o \
	src/yproxy_reader.o \
	src/yproxy_writer.o \
	src/yproxy_deleter_v2.o\
	smgr.o yezzey.o

EXTENSION = yezzey
DATA = yezzey--1.0.sql yezzey--1.0--1.8.sql yezzey--1.8--1.8.1.sql yezzey--1.8.1--1.8.2.sql yezzey--1.8.2--1.8.3.sql yezzey--1.8.3--1.8.4.sql
PGFILEDESC = "yezzey - external storage tables offloading extension"

REGRESS = simple versions drop-column yezzey-alter yezzey-vacuum yezzey-trunc yezzey-expand load_offload_load yezzey-reorg yezzey-vac-relation

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/yezzey
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

cleanall:
	@-$(MAKE) clean # incase PGXS not included
	rm -f *.o *.so *.a
	rm -f *.gcov src/*.gcov src/*.gcda src/*.gcno
	rm -f src/*.o src/*.d


apply_fmt:
	clang-format -i ./src/*.cpp ./include/*.h

format:
	@-[ -n "`command -v dos2unix`" ] && dos2unix -k -q src/*.cpp bin/gpcheckcloud/*.cpp test/*.cpp include/*.h
	@-[ -n "`command -v clang-format`" ] && clang-format -style="{BasedOnStyle: Google, IndentWidth: 4, ColumnLimit: 100, AllowShortFunctionsOnASingleLine: None}" -i src/*.cpp bin/gpcheckcloud/*.cpp test/*.cpp include/*.h


test:
	@-$(MAKE) -C test test

.PHONY: test
