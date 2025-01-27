
#include "virtual_index.h"

#include "yezzey_heap_api.h"
#include <algorithm>

Oid YezzeyFindAuxIndex_internal(Oid reloid);

static inline Oid
yezzey_create_virtual_index_relation_internal(Oid relid, const std::string &relname,
                             Oid relowner, char relpersistence,
                             bool shared_relation, bool mapped_relation) {
#if IsGreenplum6
  auto tupdesc = CreateTemplateTupleDesc(Natts_yezzey_virtual_index, false);
#else
  auto tupdesc = CreateTemplateTupleDesc(Natts_yezzey_virtual_index);
#endif

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_index_reloid,
                     "relation", OIDOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_index_filenode,
                     "filenode", OIDOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_index_blkno,
                     "blkno", INT4OID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_start_off,
                     "offset_start", INT8OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_finish_off,
                     "offset_finish", INT8OID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_encrypted,
                     "encrypted", INT4OID, -1, 0);
  TupleDescInitEntry(tupdesc,
                     (AttrNumber)Anum_yezzey_virtual_reused_from_backup,
                     "reused", INT4OID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_modcount,
                     "modcount", INT8OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_lsn, "lsn",
                     LSNOID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_x_path, "x_path",
                     TEXTOID, -1, 0);

#if IsGreenplum6
  auto yezzey_ao_auxiliary_relid = heap_create_with_catalog(
      relname.c_str() /* relname */, YEZZEY_AUX_NAMESPACE /* namespace */,
      0 /* tablespace */, relid /* relid */, GetNewObjectId() /* reltype oid */,
      InvalidOid /* reloftypeid */, relowner /* owner */,
      tupdesc /* rel tuple */, NIL, InvalidOid /* relam */, RELKIND_YEZZEYINDEX /*relkind*/,
      relpersistence, RELSTORAGE_HEAP, shared_relation, mapped_relation, true,
      0, ONCOMMIT_NOOP, NULL /* GP Policy */, (Datum)0,
      false /* use_user_acl */, true, true, false /* valid_opts */,
      false /* is_part_child */, false /* is part parent */, NULL);
#else
  auto yezzey_ao_auxiliary_relid = heap_create_with_catalog(
      relname.c_str() /* relname */, YEZZEY_AUX_NAMESPACE /* namespace */,
      0 /* tablespace */, relid /* relid */, GetNewObjectId() /* reltype oid */,
      InvalidOid /* reloftypeid */, relowner /* owner */,
      HEAP_TABLE_AM_OID /* access method*/, tupdesc /* rel tuple */, NIL,
      RELKIND_RELATION /*relkind*/, RELPERSISTENCE_PERMANENT, false /*shared*/,
      false /*mapped*/, ONCOMMIT_NOOP, NULL /* GP Policy */, (Datum)0,
      false /* use_user_acl */, true, true, InvalidOid /*relrewrite*/, NULL,
      false /* valid_opts */);
#endif

  /* Make this table visible, else yezzey virtual index creation will fail */
  CommandCounterIncrement();

  return yezzey_ao_auxiliary_relid;
}


static inline void
yezzey_create_virtual_index_idx_internal(Oid relid, const std::string &relname,
                             Oid relowner, char relpersistence)
{


  { /* check existed, if no, return */
  }

  /* ShareLock is not really needed here, but take it anyway */
  auto yezzey_rel = heap_open(YEZZEY_VIRTUAL_INDEX_RELATION, ShareLock);
  char *colname_fn = "filenode";
  char *colname_blkno = "blkno";
  auto indexColNames = list_make2(colname_fn, colname_blkno);

  auto indexInfo = makeNode(IndexInfo);

  Oid collationObjectId[2];
  Oid classObjectId[2];
  int16 coloptions[2];

  indexInfo->ii_NumIndexAttrs = 2;
#if IsGreenplum6
  indexInfo->ii_KeyAttrNumbers[0] = Anum_yezzey_virtual_index_filenode;
  indexInfo->ii_KeyAttrNumbers[1] = Anum_yezzey_virtual_index_blkno;
#else
  indexInfo->ii_IndexAttrNumbers[0] = Anum_yezzey_virtual_index_filenode;
  indexInfo->ii_IndexAttrNumbers[1] = Anum_yezzey_virtual_index_blkno;
  indexInfo->ii_NumIndexKeyAttrs = indexInfo->ii_NumIndexAttrs;
#endif
  indexInfo->ii_Expressions = NIL;
  indexInfo->ii_ExpressionsState = NIL;
  indexInfo->ii_Predicate = NIL;
#if IsGreenplum6
  indexInfo->ii_PredicateState = NIL;
#else
  indexInfo->ii_PredicateState = NULL;
#endif
  indexInfo->ii_Unique = false;
  indexInfo->ii_Concurrent = true;

  collationObjectId[0] = InvalidOid;
  collationObjectId[1] = InvalidOid;

  classObjectId[0] = OID_BTREE_OPS_OID;
  coloptions[0] = 0;

  classObjectId[1] = INT4_BTREE_OPS_OID;
  coloptions[1] = 0;

#if IsGreenplum6
  (void)index_create(yezzey_rel, relname.c_str(),
                     relid, InvalidOid,
                     InvalidOid, InvalidOid, indexInfo, indexColNames,
                     BTREE_AM_OID, 0 /* tablespace */, collationObjectId,
                     classObjectId, coloptions, (Datum)0, true, false, false,
                     false, true, false, false, true, NULL);
#else
  bits16 flags, constr_flags;
  flags = constr_flags = 0;
  (void)index_create(yezzey_rel, relname.c_str(),
                     relid, InvalidOid,
                     InvalidOid, InvalidOid, indexInfo, indexColNames,
                     BTREE_AM_OID, 0 /* tablespace */, collationObjectId,
                     classObjectId, coloptions, (Datum)0, flags, constr_flags,
                     true, true, NULL);
#endif

  /* Unlock target table -- no one can see it */
  heap_close(yezzey_rel, ShareLock);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();
}

void YezzeyCreateVirtualIndexIdx() {
  auto yezzey_ao_auxiliary_idxname = std::string("yezzey_virtual_index_idx");

  (void)yezzey_create_virtual_index_idx_internal(YEZZEY_VIRTUAL_INDEX_IDX_RELATION,
                                     yezzey_ao_auxiliary_idxname, GetUserId(),
                                     RELPERSISTENCE_PERMANENT);

  ObjectAddress baseobject;
  ObjectAddress yezzey_ao_auxiliaryobject;

  baseobject.classId = ExtensionRelationId;
  baseobject.objectId = get_extension_oid("yezzey", false);
  baseobject.objectSubId = 0;
  yezzey_ao_auxiliaryobject.classId = RelationRelationId;
  yezzey_ao_auxiliaryobject.objectId = YEZZEY_VIRTUAL_INDEX_IDX_RELATION;
  yezzey_ao_auxiliaryobject.objectSubId = 0;

  recordDependencyOn(&yezzey_ao_auxiliaryobject, &baseobject,
                     DEPENDENCY_INTERNAL);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();
}

void YezzeyCreateVirtualIndex() {
  auto yezzey_ao_auxiliary_relname = std::string("yezzey_virtual_index");

  (void)yezzey_create_virtual_index_relation_internal(YEZZEY_VIRTUAL_INDEX_RELATION,
                                     yezzey_ao_auxiliary_relname, GetUserId(),
                                     RELPERSISTENCE_PERMANENT, false, false);

  ObjectAddress baseobject;
  ObjectAddress yezzey_ao_auxiliaryobject;

  baseobject.classId = ExtensionRelationId;
  baseobject.objectId = get_extension_oid("yezzey", false);
  baseobject.objectSubId = 0;
  yezzey_ao_auxiliaryobject.classId = RelationRelationId;
  yezzey_ao_auxiliaryobject.objectId = YEZZEY_VIRTUAL_INDEX_RELATION;
  yezzey_ao_auxiliaryobject.objectSubId = 0;

  recordDependencyOn(&yezzey_ao_auxiliaryobject, &baseobject,
                     DEPENDENCY_INTERNAL);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();
}

Oid YezzeyFindAuxIndex_internal(Oid reloid) {
  HeapTuple tup;
  ScanKeyData skey[2];

  auto yezzey_virtual_index_oid = InvalidOid;

  auto yezzey_ao_auxiliary_relname =
      std::string("yezzey_virtual_index") + std::to_string(reloid);

  /*
   * Check the pg_appendonly relation to be certain the ao table
   * is there.
   */
  auto pg_class = heap_open(RelationRelationId, AccessShareLock);

  ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
              CStringGetDatum(yezzey_ao_auxiliary_relname.c_str()));

  ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(YEZZEY_AUX_NAMESPACE));

  auto scan =
      systable_beginscan(pg_class, ClassNameNspIndexId, true, NULL, 2, skey);

  if (HeapTupleIsValid(tup = systable_getnext(scan))) {

#if IsGreenplum6
    yezzey_virtual_index_oid = HeapTupleGetOid(tup);
#else
    auto ytup = ((FormData_pg_class *)GETSTRUCT(tup));
    yezzey_virtual_index_oid = ytup->oid;
#endif
  } else {
    // use separate index for relations, offloaded without yezzey api
    // this may happen during expand process and maybe some other cases
    yezzey_virtual_index_oid = YEZZEY_VIRTUAL_INDEX_RELATION;
  }

  systable_endscan(scan);
  heap_close(pg_class, AccessShareLock);

  return yezzey_virtual_index_oid;
}

Oid YezzeyFindAuxIndex(Oid reloid) { return YEZZEY_VIRTUAL_INDEX_RELATION; }

void emptyYezzeyIndex(Oid yezzey_index_oid, Oid relfilenode) {
  HeapTuple tuple;
  ScanKeyData skey[1];

  /* DELETE FROM yezzey.yezzey_virtual_index_<oid> WHERE reloid = <reloid> */
  auto rel = heap_open(yezzey_index_oid, RowExclusiveLock);

  ScanKeyInit(&skey[0], Anum_yezzey_virtual_index_filenode,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relfilenode));

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  auto desc = yezzey_beginscan(rel, snap, 1, skey);

  while (HeapTupleIsValid(tuple = heap_getnext(desc, ForwardScanDirection))) {
    simple_heap_delete(rel, &tuple->t_self);
  }

  yezzey_endscan(desc);
  heap_close(rel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();
} /* end emptyYezzeyIndex */

/* Update this settings if where clause expr changes */
#define YezzeyVirtualIndexScanCols 2

void emptyYezzeyIndexBlkno(Oid yezzey_index_oid, Oid reloid /* not used */,
                           Oid relfilenode, int blkno) {
  HeapTuple tuple;
  ScanKeyData skey[YezzeyVirtualIndexScanCols];

  /*
   * DELETE
   *     FROM yezzey.yezzey_virtual_index_<oid>
   * WHERE
   *   reloid = <redloi> and md5 = <md5> AND
   *   blkno = <blkno> AND relfilenode = <relfilenode> */
  auto rel = heap_open(yezzey_index_oid, RowExclusiveLock);

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  ScanKeyInit(&skey[0], Anum_yezzey_virtual_index_filenode,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relfilenode));

  ScanKeyInit(&skey[1], Anum_yezzey_virtual_index_blkno, BTEqualStrategyNumber,
              F_INT4EQ, Int32GetDatum(blkno));

  auto desc = yezzey_beginscan(rel, snap, YezzeyVirtualIndexScanCols, skey);

  while (HeapTupleIsValid(tuple = heap_getnext(desc, ForwardScanDirection))) {
    simple_heap_delete(rel, &tuple->t_self);
  }

  yezzey_endscan(desc);
  heap_close(rel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();
} /* end emptyYezzeyIndexBlkno */


void YezzeyFixupVirtualIndex_internal(Oid yezzey_index_oid, Relation relation) {
  
  HeapTuple tuple;
  ScanKeyData skey[1];
  bool nulls[Natts_yezzey_virtual_index];
  Datum values[Natts_yezzey_virtual_index];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  auto rel = heap_open(yezzey_index_oid, RowExclusiveLock);

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  ScanKeyInit(&skey[0], Anum_yezzey_virtual_index_filenode,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relation->rd_node.relNode));

  auto desc = yezzey_beginscan(rel, snap, YezzeyVirtualIndexScanCols, skey);

  while (HeapTupleIsValid(tuple = heap_getnext(desc, ForwardScanDirection))) {
        // update
    auto meta = (Form_yezzey_virtual_index)GETSTRUCT(tuple);

    // Assert(meta->yrelfileoid == relfileoid);
    if (meta->reloid == RelationGetRelid(relation)) {
      continue;
    }

    values[Anum_yezzey_virtual_index_reloid - 1] =
        ObjectIdGetDatum(RelationGetRelid(relation));

    auto yandxtuple = heap_form_tuple(RelationGetDescr(relation), values, nulls);

#if IsGreenplum6
    simple_heap_update(relation, &tuple->t_self, yandxtuple);
    CatalogUpdateIndexes(relation, yandxtuple);
#else
    CatalogTupleUpdate(relation, &tuple->t_self, yandxtuple);
#endif
  }

  yezzey_endscan(desc);
  heap_close(rel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();
} 

void YezzeyFixupVirtualIndex(Relation relation) {
  (void) YezzeyFixupVirtualIndex_internal(YezzeyFindAuxIndex(RelationGetRelid(relation)), relation);
} /* end YezzeyFixupVirtualIndex */

void YezzeyVirtualIndexInsert(Oid yandexoid /*yezzey auxiliary index oid*/,
                              Oid reloid, Oid relfilenodeOid, int64_t blkno,
                              int64_t offset_start, int64_t offset_finish,
                              int32_t encrypted, int32_t reused,
                              int64_t modcount, XLogRecPtr lsn,
                              const char *x_path /* external path */) {
  bool nulls[Natts_yezzey_virtual_index];
  Datum values[Natts_yezzey_virtual_index];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  /* INSERT INTO  yezzey.yezzey_virtual_index_<oid> VALUES(segno, start_offset,
   * 0, modcount, external_path) */

  auto yandxrel = heap_open(yandexoid, RowExclusiveLock);

  values[Anum_yezzey_virtual_index_reloid - 1] = Int64GetDatum(reloid);
  values[Anum_yezzey_virtual_index_filenode - 1] =
      ObjectIdGetDatum(relfilenodeOid);

  values[Anum_yezzey_virtual_index_blkno - 1] = Int64GetDatum(blkno);

  values[Anum_yezzey_virtual_start_off - 1] = Int64GetDatum(offset_start);
  values[Anum_yezzey_virtual_finish_off - 1] = Int64GetDatum(offset_finish);

  values[Anum_yezzey_virtual_encrypted - 1] = Int32GetDatum(encrypted);
  values[Anum_yezzey_virtual_reused_from_backup - 1] = Int32GetDatum(reused);

  values[Anum_yezzey_virtual_modcount - 1] = Int64GetDatum(modcount);
  values[Anum_yezzey_virtual_lsn - 1] = LSNGetDatum(lsn);
  values[Anum_yezzey_virtual_x_path - 1] =
      PointerGetDatum(cstring_to_text(x_path));

  auto yandxtuple = heap_form_tuple(RelationGetDescr(yandxrel), values, nulls);

  /* send tuple messages to master */

#if IsModernYezzey

#if 0 /* Yezzey 3 */
  auto mt_bind = create_memtuple_binding(
      RelationGetDescr(yandxrel), RelationGetNumberOfAttributes(yandxrel));

  auto memtup = memtuple_form(mt_bind, values, nulls);

  /*
   * get space to insert our next item (tuple)
   */
  auto itemLen = memtuple_get_size(memtup);

  StringInfoData buf;

  pq_beginmessage(&buf, 'z');
  pq_sendint(&buf, itemLen, sizeof(itemLen));
  pq_sendbytes(&buf, (const char *)memtup, itemLen);
  pq_endmessage(&buf);

#else
  CatalogTupleInsert(yandxrel, yandxtuple);
#endif
#else
  /* if gp6 insert tuples locally */
  simple_heap_insert(yandxrel, yandxtuple);
  CatalogUpdateIndexes(yandxrel, yandxtuple);

#endif

  heap_freetuple(yandxtuple);
  heap_close(yandxrel, RowExclusiveLock);

  CommandCounterIncrement();
}

std::vector<ChunkInfo>
YezzeyVirtualGetOrder(Oid yandexoid /*yezzey auxiliary index oid*/,
                      Oid reloid /* not used */, Oid relfilenode, int blkno) {
  /* SELECT external_path
   * FROM yezzey.yezzey_virtual_index_<oid>
   * WHERE segno = .. and filenode = ..
   * <>; */
  HeapTuple tuple;

  ScanKeyData skey[YezzeyVirtualIndexScanCols];

  std::vector<ChunkInfo> res;

  auto rel = heap_open(yandexoid, RowExclusiveLock);

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  // ScanKeyInit(&skey[0], Anum_yezzey_virtual_index_reloid,
  // BTEqualStrategyNumber,
  //             F_OIDEQ, ObjectIdGetDatum(reloid));

  ScanKeyInit(&skey[0], Anum_yezzey_virtual_index_filenode,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relfilenode));

  ScanKeyInit(&skey[1], Anum_yezzey_virtual_index_blkno, BTEqualStrategyNumber,
              F_INT4EQ, Int32GetDatum(blkno));

  /* TBD: Read index */
  auto desc = yezzey_beginscan(rel, snap, YezzeyVirtualIndexScanCols, skey);

  while (HeapTupleIsValid(tuple = heap_getnext(desc, ForwardScanDirection))) {
    auto ytup = ((FormData_yezzey_virtual_index *)GETSTRUCT(tuple));
    // unpack text to str
    res.push_back(ChunkInfo(ytup->lsn, ytup->modcount,
                            text_to_cstring(&ytup->x_path),
                            ytup->finish_offset - ytup->start_offset,
                            ytup->start_offset, ytup->encrypted));
  }

  yezzey_endscan(desc);
  heap_close(rel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();

  /* sort by modcount - they are unic */
  std::sort(res.begin(), res.end(),
            [](const ChunkInfo &lhs, const ChunkInfo &rhs) {
              return lhs.modcount == rhs.modcount ? lhs.lsn < rhs.lsn
                                                  : lhs.modcount < rhs.modcount;
            });

  std::vector<ChunkInfo> modcnt_uniqres;

  /* remove duplicated data chunks while read.
   * report correuption in case of offset mismatch.
   */

  for (uint i = 0; i < res.size(); ++i) {
    if (res[i].size == 0) {
      continue;
    }
    if (i + 1 < res.size() && res[i + 1].modcount == res[i].modcount) {
      if (res[i + 1].start_off != res[i].start_off) {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg_internal("found duplicated modcount data chunk with "
                                 "diffferent offsets: %lu vs %lu",
                                 res[i].start_off, res[i + 1].start_off)));
      } else {
        ereport(NOTICE, (errcode(ERRCODE_DATA_CORRUPTED),
                         errmsg_internal(
                             "found duplicated modcount data chunk, skip")));
      }
      continue;
    }
    modcnt_uniqres.push_back(res[i]);
  }

  return modcnt_uniqres;
}