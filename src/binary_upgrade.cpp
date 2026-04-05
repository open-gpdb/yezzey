
/*
 * Yezzey.
 */

#include "pg.h"

#include "binary_upgrade.h"
#include "expire_hint.h"
#include "offload_policy.h"
#include "virtual_index.h"
#include "virtual_schema.h"
#include "yezzey_heap_api.h"
#include "yezzey_meta.h"

void YezzeyBinaryUpgrade(void) {
  /**/
  ScanKeyData skey[2];
  HeapTuple newTuple;
  Datum values[Natts_pg_class];
  bool nulls[Natts_pg_class];
  bool replaces[Natts_pg_class];

  auto prevAllowSystableMods = allowSystemTableMods;

  allowSystemTableMods = true;

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  auto classrel = relation_open(RelationRelationId, RowExclusiveLock);

  ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
              CStringGetDatum("yezzey_virtual_index"));

  ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(YEZZEY_AUX_NAMESPACE));

  auto scan = yezzey_systable_beginscan(classrel, ClassNameNspIndexId, true,
                                        snap, 2, skey);

  auto systuple = yezzey_systable_getnext(scan);

  /* No map relation created. return invalid oid */
  if (!HeapTupleIsValid(systuple)) {
    yezzey_systable_endscan(scan);
    UnregisterSnapshot(snap);
    yezzey_relation_close(classrel, RowExclusiveLock);

    allowSystemTableMods = prevAllowSystableMods;
    elog(ERROR, "failed to upgrade yezzey virtual index relation");
  }

#if PG_VERSION_NUM >= 120000
  Oid yezzey_vi_oid = ((Form_pg_class)GETSTRUCT(systuple))->oid;
#else
  Oid yezzey_vi_oid = HeapTupleGetOid(systuple);
#endif

  if (yezzey_vi_oid != YEZZEY_VIRTUAL_INDEX_RELATION) {
    yezzey_systable_endscan(scan);
    UnregisterSnapshot(snap);
    yezzey_relation_close(classrel, RowExclusiveLock);

    allowSystemTableMods = prevAllowSystableMods;
    elog(ERROR, "wrong oid when upgrade yezzey virtual index relation");
  }

  auto tupform = ((Form_pg_class)GETSTRUCT(systuple));
  tupform->relkind = RELKIND_RELATION;

  /* Replace the ACL value */
  MemSet(values, 0, sizeof(values));
  MemSet(nulls, false, sizeof(nulls));
  MemSet(replaces, false, sizeof(replaces));

  replaces[Anum_pg_class_relkind - 1] = true;
  values[Anum_pg_class_relkind - 1] = ObjectIdGetDatum(RELKIND_RELATION);
  nulls[Anum_pg_class_relkind - 1] = false;

  newTuple = heap_modify_tuple(systuple, RelationGetDescr(classrel), values,
                               nulls, replaces);

#if IsGreenplum6
  simple_heap_update(classrel, &newTuple->t_self, newTuple);
  /* keep the catalog indexes up to date */
  CatalogUpdateIndexes(classrel, newTuple);
#else
  CatalogTupleUpdate(classrel, &newTuple->t_self, newTuple);
#endif

  yezzey_systable_endscan(scan);
  UnregisterSnapshot(snap);
  yezzey_relation_close(classrel, RowExclusiveLock);

  /* make changes visible*/
  CommandCounterIncrement();

  allowSystemTableMods = prevAllowSystableMods;
}

#if IsModernYezzey

static void YezzeyCreateVirtualSpc() {

  Relation rel;
  Datum values[Natts_pg_tablespace];
  bool nulls[Natts_pg_tablespace];
  HeapTuple tuple;
  Oid tablespaceoid;
  char *location = "";
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

    xlrec.ts_id = tablespaceoid;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, offsetof(xl_tblspc_create_rec, ts_path));
    XLogRegisterData((char *)location, strlen(location) + 1);

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
#endif

void YezzeyInitMetadata(void) {

#if IsModernYezzey
  YezzeyCreateVirtualSpc();
#endif

  (void)YezzeyCreateVirtualSchema();
  (void)YezzeyCreateOffloadPolicyRelation();
  (void)YezzeyCreateVirtualIndex();

#if IsModernYezzey
  (void)YezzeyCreateVirtualIndexIdx();
  (void)YezzeyCreateExpireHint();
  (void)YezzeyCreateExpireHintIdx();
#endif
}

void YezzeyBinaryUpgrade183(void) {
#if IsModernYezzey
#else
  (void)YezzeyCreateVirtualIndexIdx();
#endif
}

void YezzeyBinaryUpgrade184(void) {
#if IsModernYezzey
#else
  (void)YezzeyCreateExpireHint();
  (void)YezzeyCreateExpireHintIdx();
#endif
}