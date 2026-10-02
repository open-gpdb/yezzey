
/*
 * Yezzey.
 */

#include "pg.h"

#include "binary_upgrade.h"
#include "offload_policy.h"
#include "virtual_index.h"
#include "yezzey_heap_api.h"
#include "yezzey_meta.h"

static void YezzeyCreateVirtualSpc() {

  Relation rel;
  Datum values[Natts_pg_tablespace];
  bool nulls[Natts_pg_tablespace];
  HeapTuple tuple;
  Oid tablespaceoid;
  Oid ownerId;
  HeapTuple tp;

  ownerId = GetUserId();

  tablespaceoid = YEZZEYTABLESPACE_OID;
  /*
   * Not found in TableSpace cache.  Check catcache.  If we don't find a
   * valid HeapTuple, it must mean someone has managed to request tablespace
   * details for a non-existent tablespace.  We'll just treat that case as
   * if no options were specified.
   */
  tp = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(tablespaceoid));
  if (HeapTupleIsValid(tp)) {
    ReleaseSysCache(tp);
    return;
  }

  /*
   * Insert tuple into pg_tablespace.  The purpose of doing this first is to
   * lock the proposed tablename against other would-be creators. The
   * insertion will roll back if we find problems below.
   */
  rel = table_open(TableSpaceRelationId, RowExclusiveLock);

  MemSet(nulls, false, sizeof(nulls));

  values[Anum_pg_tablespace_oid - 1] = ObjectIdGetDatum(tablespaceoid);
  values[Anum_pg_tablespace_spcname - 1] =
      DirectFunctionCall1(namein, CStringGetDatum("yezzey"));
  values[Anum_pg_tablespace_spcowner - 1] = ObjectIdGetDatum(ownerId);
  nulls[Anum_pg_tablespace_spcacl - 1] = true;

  /* No filehandler support. */
  nulls[Anum_pg_tablespace_spcfilehandlersrc - 1] = true;
  nulls[Anum_pg_tablespace_spcfilehandlerbin - 1] = true;

  /* No spcoptions */
  nulls[Anum_pg_tablespace_spcoptions - 1] = true;

  tuple = heap_form_tuple(rel->rd_att, values, nulls);

  CatalogTupleInsert(rel, tuple);

  heap_freetuple(tuple);

  /*
   * No tag description.
   */

  /* Post creation hook for new tablespace */
  InvokeObjectPostCreateHook(TableSpaceRelationId, tablespaceoid, 0);

  /* Record the filesystem change in XLOG */
  {
    xl_tblspc_create_rec xlrec;
    char location[] = "";

    xlrec.ts_id = tablespaceoid;

    XLogBeginInsert();
    XLogRegisterData(reinterpret_cast<char *>(&xlrec),
                     offsetof(xl_tblspc_create_rec, ts_path));
    XLogRegisterData(location, strlen(location) + 1);

    (void)XLogInsert(RM_TBLSPC_ID, XLOG_TBLSPC_CREATE);
  }

  /*
   * Force synchronous commit, to minimize the window between creating the
   * symlink on-disk and marking the transaction committed.  It's not great
   * that there is any window at all, but definitely we don't want to make
   * it larger than necessary.
   */
  ForceSyncCommit();

  /* We keep the lock on pg_tablespace until commit */
  table_close(rel, NoLock);
}

void YezzeyInitMetadata(void) {

  YezzeyCreateVirtualSpc();

  (void)YezzeyCreateOffloadPolicyRelation();
  (void)YezzeyCreateVirtualIndex();

  (void)YezzeyCreateVirtualIndexIdx();
}
