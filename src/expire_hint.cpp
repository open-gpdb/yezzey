/* STAGE delete */

#include "expire_hint.h"

#include "yezzey_meta.h"

#include "yezzey_heap_api.h"
#include <algorithm>

// STAGE needed?
Oid YezzeyFindAuxIndex_internal(Oid reloid);

static inline Oid yezzey_create_expire_hint_relation_internal(
    Oid relid, const std::string &relname, Oid relowner, char relpersistence,
    bool shared_relation, bool mapped_relation) {
#if IsGreenplum6
  auto tupdesc = CreateTemplateTupleDesc(Natts_yezzey_expire_hint, false);
#else
  auto tupdesc = CreateTemplateTupleDesc(Natts_yezzey_expire_hint);
#endif

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_hint_lsn, "lsn",
                     LSNOID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_hint_x_path,
                     "x_path", TEXTOID, -1, 0);
#if IsGreenplum6
  auto yezzey_ao_auxiliary_relid = heap_create_with_catalog(
      relname.c_str() /* relname */, YEZZEY_AUX_NAMESPACE /* namespace */,
      0 /* tablespace */, relid /* relid */, GetNewObjectId() /* reltype oid */,
      InvalidOid /* reloftypeid */, relowner /* owner */,
      tupdesc /* rel tuple */, NIL, InvalidOid /* relam */,
      RELKIND_RELATION /*relkind*/, relpersistence, RELSTORAGE_HEAP,
      shared_relation, mapped_relation, true, 0, ONCOMMIT_NOOP,
      NULL /* GP Policy */, (Datum)0, false /* use_user_acl */, true, true,
      false /* valid_opts */, false /* is_part_child */,
      false /* is part parent */, NULL);
#else
  auto yezzey_ao_auxiliary_relid = heap_create_with_catalog(
      relname.c_str() /* relname */, PG_EXTAUX_NAMESPACE /* namespace */,
      0 /* tablespace */, relid /* relid */, InvalidOid /* reltype oid */,
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
yezzey_create_expire_hint_idx_internal(Oid relid, const std::string &relname,
                                       Oid relowner, char relpersistence) {

  { /* check existed, if no, return */
  }

  /* ShareLock is not really needed here, but take it anyway */
  auto yezzey_rel = heap_open(YEZZEY_EXPIRE_HINT_RELATION, ShareLock);
  const char *colname_x_path = "x_path";
  auto indexColNames = list_make1((void *)colname_x_path);

  auto indexInfo = makeNode(IndexInfo);

  Oid collationObjectId[1];
  Oid classObjectId[1];
  int16 coloptions[1];

  indexInfo->ii_NumIndexAttrs = 1;
#if IsGreenplum6
  indexInfo->ii_KeyAttrNumbers[0] = Anum_yezzey_expire_hint_x_path;
#else
  indexInfo->ii_IndexAttrNumbers[0] = Anum_yezzey_expire_hint_x_path;
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
  indexInfo->ii_Unique = true;
  indexInfo->ii_Concurrent = true;

  collationObjectId[0] = DEFAULT_COLLATION_OID;

  classObjectId[0] = TEXT_BTREE_OPS_OID;
  coloptions[0] = 0;

#if IsGreenplum6
  (void)index_create(yezzey_rel, relname.c_str(), relid, InvalidOid, InvalidOid,
                     InvalidOid, indexInfo, indexColNames, BTREE_AM_OID,
                     0 /* tablespace */, collationObjectId, classObjectId,
                     coloptions, (Datum)0, true, false, false, false, true,
                     false, false, true, NULL);
#else
  bits16 flags, constr_flags;
  flags = constr_flags = 0;
  (void)index_create(yezzey_rel, relname.c_str(), relid, InvalidOid, InvalidOid,
                     InvalidOid, indexInfo, indexColNames, BTREE_AM_OID,
                     0 /* tablespace */, collationObjectId, classObjectId,
                     coloptions, (Datum)0, flags, constr_flags, true, true,
                     NULL);
#endif

  /* Unlock target table -- no one can see it */
  heap_close(yezzey_rel, ShareLock);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();
}

void YezzeyCreateExpireHintIdx() {
  auto yezzey_ao_auxiliary_idxname = std::string("yezzey_expire_hint_idx");

  (void)yezzey_create_expire_hint_idx_internal(
      YEZZEY_EXPIRE_HINT_IDX_RELATION, yezzey_ao_auxiliary_idxname, GetUserId(),
      RELPERSISTENCE_PERMANENT);

  ObjectAddress baseobject;
  ObjectAddress yezzey_ao_auxiliaryobject;

  baseobject.classId = ExtensionRelationId;
  baseobject.objectId = get_extension_oid("yezzey", false);
  baseobject.objectSubId = 0;
  yezzey_ao_auxiliaryobject.classId = RelationRelationId;
  yezzey_ao_auxiliaryobject.objectId = YEZZEY_EXPIRE_HINT_IDX_RELATION;
  yezzey_ao_auxiliaryobject.objectSubId = 0;

  recordDependencyOn(&yezzey_ao_auxiliaryobject, &baseobject,
                     DEPENDENCY_INTERNAL);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();
}

void YezzeyCreateExpireHint() {
  auto yezzey_ao_auxiliary_relname = std::string("yezzey_expire_hint");

  (void)yezzey_create_expire_hint_relation_internal(
      YEZZEY_EXPIRE_HINT_RELATION, yezzey_ao_auxiliary_relname, GetUserId(),
      RELPERSISTENCE_PERMANENT, false, false);

  ObjectAddress baseobject;
  ObjectAddress yezzey_ao_auxiliaryobject;

  baseobject.classId = ExtensionRelationId;
  baseobject.objectId = get_extension_oid("yezzey", false);
  baseobject.objectSubId = 0;
  yezzey_ao_auxiliaryobject.classId = RelationRelationId;
  yezzey_ao_auxiliaryobject.objectId = YEZZEY_EXPIRE_HINT_RELATION;
  yezzey_ao_auxiliaryobject.objectSubId = 0;

  recordDependencyOn(&yezzey_ao_auxiliaryobject, &baseobject,
                     DEPENDENCY_INTERNAL);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();
}
