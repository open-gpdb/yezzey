# gpcontrib/yezzey/Makefile

override CFLAGS = -Wall -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Wno-format-truncation  -g -ggdb -std=gnu99 -Werror=uninitialized -Werror=implicit-function-declaration -DGPBUILD

COMMON_LINK_OPTIONS = -lstdc++

COMMON_CPP_FLAGS = -fPIC -I/usr/include/libxml2 -I/usr/local/opt/openssl/include -DENABLE_NLS 

override CPPFLAGS = -fPIC -lstdc++ -g3 -ggdb -Wall -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Werror=format-security -Wno-format-truncation -g -fPIC -Iinclude -Ilib -g -I. -I../../src/include -D_GNU_SOURCE

SHLIB_LINK += $(COMMON_LINK_OPTIONS)
PG_CPPFLAGS += $(COMMON_CPP_FLAGS) -I./include -Iinclude -Ilib -I$(libpq_srcdir) -I$(libpq_srcdir)/postgresql/server/utils

MODULE_big = yezzey

OBJS = \
	$(WIN32RES) \
	src/storage.o src/proxy.o \
	src/virtual_index.o \
	src/util.o \
	src/url.o \
	src/io.o \
	src/io_adv.o \
	src/offload_tablespace_map.o \
	src/offload_policy.o \
	src/offload.o \
	src/virtual_tablespace.o \
	src/xvacuum.o \
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
DATA = yezzey--2.0.sql

PGFILEDESC = "yezzey - external storage tables offloading extension"

ifdef IS_CLOUDBERRY_3
REGRESS = \
          simple_cbdb_3
else
REGRESS = \
          simple \
          drop-column \
          yezzey-alter\
          yezzey-alter-toast\
          yezzey-vacuum \
          yezzey-vacuum-garbage \
          yezzey-trunc \
          yezzey-expand \
          load_offload_load \
          yezzey_feat_last \
          yezzey-reorg \
          yezzey-vac-relation \
          yezzey-vac-relation-187 \
          yezzey-offload-errors
#          yezzey-otm-feat \
 #         yezzey-otm-deletion \
  #        yezzey-vi-eh-unique \
   #       yezzey-stat \
    #      yezzey-alter-ts \
     #     yezzey-create-offloaded \
      #    yezzey-offload-errors
          
endif

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

# -std=c++11 applies to the C++ compiles only. It used to sit in CPPFLAGS,
# which the build shares with the C compiles of smgr.c and yezzey.c. CXXFLAGS
# is not defined until the includes above have run, hence the placement here.
override CXXFLAGS += -std=c++11

cleanall:
	@-$(MAKE) clean # incase PGXS not included
	rm -f *.o *.so *.a
	rm -f src/*.o src/*.d

apply_fmt:
	clang-format -i ./src/*.cpp ./include/*.h *.c *.h

test:
	@-$(MAKE) -C test test

.PHONY: test apply_fmt check-format format
