#ifndef YEZZEY_PG_H
#define YEZZEY_PG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "c.h"
#include "postgres.h"

#include "ygpver.h"

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#if PG_VERSION_NUM >= 100000
#include "common/md5.h"
#else
#include "libpq/md5.h"
#endif

#include "utils/timestamp.h"
#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#endif

#if IsModernYezzey
#include "access/relation.h"
#endif

#include "access/aocssegfiles.h"
#include "access/aosegfiles.h"
#include "access/htup_details.h"
#include "access/xlog.h"

#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"

#include "storage/smgr.h"

#include "commands/extension.h"

#include "common/relpath.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "pgstat.h"

#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/lmgr.h"

#if PG_VERSION_NUM < 100000
#include "utils/tqual.h"
#endif

#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

#include "utils/guc.h"

#include "fmgr.h"

#include "access/xact.h"

// For GpIdentity
#if IsGreenplum6 || IsModernYezzey
#include "cdb/cdbappendonlyxlog.h"
#include "cdb/cdbvars.h"
#endif

#include "catalog/heap.h"

#include "access/multixact.h"
#include "nodes/primnodes.h"
#include "utils/ps_status.h"

#include "catalog/index.h"
#include "catalog/oid_dispatch.h"

#include "catalog/pg_opclass.h"

#include "utils/pg_lsn.h"

#if PG_VERSION_NUM >= 120000
#include "access/heapam.h"
#include "access/table.h"
#include "access/tupdesc.h"
#endif

#include "libpq/pqformat.h"

#include "miscadmin.h"
#include "postmaster/postmaster.h"

#include "commands/tablespace.h"

#include "catalog/pg_collation.h"
#include "commands/dbcommands.h"
#ifdef __cplusplus
}
#endif

#endif /* YEZZEY_PG_H */
