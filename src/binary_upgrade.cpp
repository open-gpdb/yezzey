#include "pg.h"
#include "yezzey_heap_api.h"
#include "virtual_index.h"
#include "binary_upgrade.h"

void YezzeyBinaryUpgrade(void) {
  /**/
  ScanKeyData skey[2];
  HeapTuple	newTuple;
  Datum		values[Natts_pg_class];
  bool		nulls[Natts_pg_class];
  bool		replaces[Natts_pg_class];
 
  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  auto classrel = relation_open(RelationRelationId, RowExclusiveLock);

  ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
              CStringGetDatum("yezzey_virtual_index"));

  ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(YEZZEY_AUX_NAMESPACE));

  auto scan = yezzey_beginscan(classrel, snap, 2, skey);
  auto systuple = heap_getnext(scan, ForwardScanDirection);

  /* No map relation created. return invalid oid */
  if (!HeapTupleIsValid(systuple)) {
    yezzey_endscan(scan);
    UnregisterSnapshot(snap);
    heap_close(classrel, RowExclusiveLock);

    elog(ERROR, "failed to upgrade yezzey virtual index relation");
  }

  Oid yezzey_vi_oid = HeapTupleGetOid(systuple);
  if (yezzey_vi_oid != YEZZEY_TEMP_INDEX_RELATION) {
    yezzey_endscan(scan);
    UnregisterSnapshot(snap);
    heap_close(classrel, RowExclusiveLock);

    elog(ERROR, "wrong oid when upgrade yezzey virtual index relation");
  }
  
  auto tupform = ((Form_pg_class) GETSTRUCT(systuple));
  tupform->relkind = RELKIND_RELATION;


  /* Replace the ACL value */
  MemSet(values, 0, sizeof(values));
  MemSet(nulls, false, sizeof(nulls));
  MemSet(replaces, false, sizeof(replaces));

  replaces[Anum_pg_class_relkind - 1] = true;
  values[Anum_pg_class_relkind - 1] = ObjectIdGetDatum(RELKIND_RELATION);
  nulls[Anum_pg_class_relkind - 1] = false;

  newTuple = heap_modify_tuple(systuple, RelationGetDescr(classrel),
                                values, nulls, replaces);


#if IsGreenplum6
  simple_heap_update(classrel, &newTuple->t_self, newTuple);
  /* keep the catalog indexes up to date */
  CatalogUpdateIndexes(classrel, newTuple);
#else
  CatalogTupleUpdate(classrel, &newTuple->t_self, newTuple);
#endif

  yezzey_endscan(scan);
  UnregisterSnapshot(snap);
  heap_close(classrel, RowExclusiveLock);

  /* make changes visible*/
  CommandCounterIncrement();
}