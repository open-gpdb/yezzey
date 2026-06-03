/*
 * file: src/offload_tablespace_map.cpp
 */

#include "offload.h"
#include "offload_policy.h"
#include "pg.h"
#include "yezzey_heap_api.h"
#include <unistd.h>

#include "gucs.h"

#include <map>

#include "cdb/cdbvars.h"

#include "offload_tablespace_map.h"

const std::string offload_tablespace_map_relname = "offload_tablespace_map";

static Oid YezzeyResolveTablespaceMapOid() {
  if (!use_otm_feature) {
    return InvalidOid;
  }

  /* SELECT FROM pg_catalog.pg_class WHERE relname = 'offload_tablespace_map'
   * and relnamespace = 8001; */
  auto snap = RegisterSnapshot(GetTransactionSnapshot());
  /**/
  ScanKeyData skey[2];

  auto classrel = yezzey_relation_open(RelationRelationId, RowExclusiveLock);

  ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
              CStringGetDatum(offload_tablespace_map_relname.c_str()));

  ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(YEZZEY_AUX_NAMESPACE));

  auto scan = yezzey_systable_beginscan(classrel, ClassNameNspIndexId, true,
                                        snap, 2, skey);

  auto oldtuple = yezzey_systable_getnext(scan);

  /* No map relation created. return invalid oid */
  if (!HeapTupleIsValid(oldtuple)) {
    yezzey_systable_endscan(scan);
    UnregisterSnapshot(snap);
    yezzey_relation_close(classrel, RowExclusiveLock);
    return InvalidOid;
  }

#if PG_VERSION_NUM >= 120000
  Oid yezzey_tablespace_map_oid = ((Form_pg_class)GETSTRUCT(oldtuple))->oid;
#else
  Oid yezzey_tablespace_map_oid = HeapTupleGetOid(oldtuple);
#endif

  yezzey_systable_endscan(scan);
  UnregisterSnapshot(snap);
  yezzey_relation_close(classrel, RowExclusiveLock);

  return yezzey_tablespace_map_oid;
}

static std::map<std::string, std::string> yezzey_otm_hint;

static std::string y_stringify_rv(const char *nspname, const char *relname) {
  std::string ret;

  ret += nspname;
  ret += '.';
  ret += relname;

  return ret;
}

Oid YezzeyGetRelationOriginTablespaceOid(const char *nspname,
                                         const char *relname, Oid i_reloid) {
  auto scpname = YezzeyGetRelationOriginTablespace(nspname, relname, i_reloid);
  return get_tablespace_oid(scpname.c_str(), false);
}

std::string YezzeyGetRelationOriginTablespace(const char *nspname,
                                              const char *relname,
                                              Oid i_reloid) {
  HeapTuple offtuple;

  if (nspname != NULL && relname != NULL) {
    auto key = y_stringify_rv(nspname, relname);
    if (yezzey_otm_hint.count(key)) {
      return yezzey_otm_hint[key];
    }
  }

  auto yezzey_tablespace_map_oid = YezzeyResolveTablespaceMapOid();

  /* No map relation created. Assume pg_default by default */
  if (yezzey_tablespace_map_oid == InvalidOid) {
    return "pg_default";
  }

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  /* SELECT FROM yezzey.offload_tablespace_map WHERE reloid = i_reloid; */
  auto offload_tablespace_map_rel =
      yezzey_relation_open(yezzey_tablespace_map_oid, RowExclusiveLock);

  ScanKeyData offskey[1];

  ScanKeyInit(&offskey[0], Anum_offload_tablespace_map_reloid,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(i_reloid));

  auto scanoff = yezzey_beginscan(offload_tablespace_map_rel, snap, 1, offskey);
#if IsModernYezzey
  auto slot = table_slot_create(offload_tablespace_map_rel, NULL);
  /* No map tuple created. Assume 'pg_default' by default */
  if (!table_scan_getnextslot(scanoff, ForwardScanDirection, slot)) {
    ExecDropSingleTupleTableSlot(slot);

    heap_close(offload_tablespace_map_rel, RowExclusiveLock);

    yezzey_endscan(scanoff);
    UnregisterSnapshot(snap);

    /* should be OK */
    if (Gp_role == GP_ROLE_UTILITY || Gp_role == GP_ROLE_DISPATCH) {
      return "pg_default";
    }

    /* XXX: todo - fix OTM */
    return "pg_default";

    elog(ERROR, "failed to map relation %d (%s.%s) to its origin tablespace",
         i_reloid, nspname, relname);
  }

  bool shouldFree;

  offtuple = ExecFetchSlotHeapTuple(slot, false, &shouldFree);
  Assert(!shouldFree);
#else
  offtuple = heap_getnext(scanoff, ForwardScanDirection);
  /* No map tuple created. Assume 'pg_default' by default */
  if (!HeapTupleIsValid(offtuple)) {
    heap_close(offload_tablespace_map_rel, RowExclusiveLock);

    yezzey_endscan(scanoff);
    UnregisterSnapshot(snap);

    /* should be OK */
    if (Gp_role == GP_ROLE_UTILITY || Gp_role == GP_ROLE_DISPATCH) {
      return "pg_default";
    }

    elog(ERROR, "failed to map relation %d (%s.%s) to its origin tablespace",
         i_reloid, nspname, relname);
  }
#endif

  auto rv = ((Form_offload_tablespace_map)GETSTRUCT(offtuple))
                ->origin_tablespace_name;

  auto tablespaceName = rv.data;

  elog(DEBUG3, "YezzeyGetRelationOriginTablespace: resolved name %s",
       tablespaceName);

  auto tablespace_val = std::string(tablespaceName);

  heap_close(offload_tablespace_map_rel, RowExclusiveLock);

  yezzey_endscan(scanoff);
  UnregisterSnapshot(snap);

#if IsModernYezzey
  ExecDropSingleTupleTableSlot(slot);
#endif

  return tablespace_val;
}

void YezzeyRegisterRelationOriginTablespaceName(Oid i_reloid, Name i_spcname) {
  auto yezzey_tablespace_map_oid = YezzeyResolveTablespaceMapOid();

  if (yezzey_tablespace_map_oid == InvalidOid) {
    /* no map relation created, NOOP */
    return;
  }

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  bool nulls[Natts_offload_tablespace_map];
  Datum values[Natts_offload_tablespace_map];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  /* SELECT FROM yezzey.offload_tablespace_map WHERE reloid = i_reloid; */
  auto offload_tablespace_map_rel =
      yezzey_relation_open(yezzey_tablespace_map_oid, RowExclusiveLock);

  ScanKeyData offskey[1];

  ScanKeyInit(&offskey[0], Anum_offload_tablespace_map_reloid,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(i_reloid));

  auto scanoff = yezzey_beginscan(offload_tablespace_map_rel, snap, 1, offskey);

#if IsModernYezzey
  auto slot = table_slot_create(offload_tablespace_map_rel, NULL);

  /* Already registered, from previous offloads */
  if (table_scan_getnextslot(scanoff, ForwardScanDirection, slot)) {
    ExecDropSingleTupleTableSlot(slot);

    heap_close(offload_tablespace_map_rel, RowExclusiveLock);

    yezzey_endscan(scanoff);
    UnregisterSnapshot(snap);
    return;
  }

#else
  auto offtuple = heap_getnext(scanoff, ForwardScanDirection);

  /* Already registered, from previous offloads */
  if (HeapTupleIsValid(offtuple)) {
    heap_close(offload_tablespace_map_rel, RowExclusiveLock);

    yezzey_endscan(scanoff);
    UnregisterSnapshot(snap);
    return;
  }
#endif
  yezzey_endscan(scanoff);

  values[Anum_offload_tablespace_map_reloid - 1] = ObjectIdGetDatum(i_reloid);
  values[Anum_offload_tablespace_map_origin_tablespace_name - 1] =
      NameGetDatum(i_spcname);

  auto nofftuple = heap_form_tuple(RelationGetDescr(offload_tablespace_map_rel),
                                   values, nulls);

#if IsGreenplum6
  simple_heap_insert(offload_tablespace_map_rel, nofftuple);
  CatalogUpdateIndexes(offload_tablespace_map_rel, nofftuple);
#else
  CatalogTupleInsert(offload_tablespace_map_rel, nofftuple);
#endif

  heap_close(offload_tablespace_map_rel, RowExclusiveLock);

  heap_freetuple(nofftuple);

#if IsModernYezzey
  ExecDropSingleTupleTableSlot(slot);
#endif

  UnregisterSnapshot(snap);
}

void YezzeyRegisterRelationOriginTablespace(Oid i_reloid, Oid i_reltablespace) {
  /* Search syscache for pg_tablespace */
  auto spctuple =
      SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(i_reltablespace));
  if (!HeapTupleIsValid(spctuple))
    ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
             errmsg("tablespace with OID %u does not exist", i_reltablespace)));

  if (!use_otm_feature && i_reltablespace != DEFAULTTABLESPACE_OID)
    ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
             errmsg("tablespace with OID %u is non-default, offload rejrected",
                    i_reltablespace),
             errdetail("turn yezzey.use_otm_feature GUC on")));

  auto spcname = &((Form_pg_tablespace)GETSTRUCT(spctuple))->spcname;

  YezzeyRegisterRelationOriginTablespaceName(i_reloid, spcname);

  ReleaseSysCache(spctuple);
}

void YezzeyCopyOTM(const RangeVar *rv, Oid sourceRelationOid) {
  auto val = YezzeyGetRelationOriginTablespace(rv->schemaname, rv->relname,
                                               sourceRelationOid);

  auto key = y_stringify_rv(rv->schemaname, rv->relname);

  auto r = relation_open(sourceRelationOid, NoLock);

  auto key_origin = y_stringify_rv(get_namespace_name(r->rd_rel->relnamespace),
                                   RelationGetRelationName(r));
  yezzey_otm_hint[key_origin] = val;

  yezzey_otm_hint[key] = val;

  relation_close(r, NoLock);
}

void YezzeyPreassignOTM(Oid targRelationOid, Oid sourceRelationOid) {

#if IsModernYezzey
  return;
  if (IsCatalogRelationOid(targRelationOid) ||
      IsCatalogRelationOid(sourceRelationOid)) {
    return;
  }
#else
  /* TODO */
#endif

  auto r = try_relation_open(sourceRelationOid, NoLock, false);

  if (r == NULL)
    return;

  /* If not yezzey, we do not care */
  if (r->rd_rel->reltablespace == YEZZEYTABLESPACE_OID) {

    auto val = YezzeyGetRelationOriginTablespace(NULL, NULL, sourceRelationOid);

    auto r2 = relation_open(targRelationOid, NoLock);

    auto key = y_stringify_rv(get_namespace_name(r2->rd_rel->relnamespace),
                              RelationGetRelationName(r2));

    auto key_origin =
        y_stringify_rv(get_namespace_name(r->rd_rel->relnamespace),
                       RelationGetRelationName(r));
    yezzey_otm_hint[key_origin] = val;

    yezzey_otm_hint[key] = val;

    relation_close(r2, NoLock);
  }
  relation_close(r, NoLock);
}

void YezzeyTruncateOTMHint(void) { /*yezzey_otm_hint.clear();*/ }
