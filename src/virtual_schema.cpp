#include "virtual_index.h"
#include "yezzey_heap_api.h"

#include "yezzey_meta.h"

static const char *nspName = "yezzey";

void YezzeyCreateVirtualSchema(void) {
  Relation nspdesc;
  HeapTuple tup;
  Oid nspoid;
  bool nulls[Natts_pg_namespace];
  Datum values[Natts_pg_namespace];
  NameData nname;
  TupleDesc tupDesc;
  ObjectAddress myself;
  int i;
  Acl *nspacl;
  Oid ownerId = GetUserId();

  /* make sure there is no existing namespace of same name */
  if (SearchSysCacheExists1(NAMESPACENAME, PointerGetDatum(nspName)))
    ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_SCHEMA),
             errmsg("yezzey_creta: schema \"%s\" already exists", nspName)));

#if IsModernYezzey
  nspacl = get_user_default_acl(OBJECT_SCHEMA, ownerId, InvalidOid);
#else
  nspacl = get_user_default_acl(ACL_OBJECT_RELATION, ownerId, InvalidOid);
#endif

  nspdesc = yezzey_relation_open(NamespaceRelationId, RowExclusiveLock);
  tupDesc = nspdesc->rd_att;

  /* initialize nulls and values */
  for (i = 0; i < Natts_pg_namespace; i++) {
    nulls[i] = false;
    values[i] = (Datum)NULL;
  }

  nspoid = YEZZEY_AUX_NAMESPACE;

#if IsModernYezzey
  values[Anum_pg_namespace_oid - 1] = ObjectIdGetDatum(nspoid);
#endif
  namestrcpy(&nname, nspName);
  values[Anum_pg_namespace_nspname - 1] = NameGetDatum(&nname);
  values[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(ownerId);
  if (nspacl != NULL)
    values[Anum_pg_namespace_nspacl - 1] = PointerGetDatum(nspacl);
  else
    nulls[Anum_pg_namespace_nspacl - 1] = true;

  tup = heap_form_tuple(tupDesc, values, nulls);

#if !IsModernYezzey
  HeapTupleSetOid(tup, nspoid);
#endif

#if IsModernYezzey
  CatalogTupleInsert(nspdesc, tup);
#else
  /* if gp6 insert tuples locally */
  simple_heap_insert(nspdesc, tup);
  CatalogUpdateIndexes(nspdesc, tup);
#endif
  Assert(OidIsValid(nspoid));

  yezzey_relation_close(nspdesc, RowExclusiveLock);

  /* Record dependencies */
  myself.classId = NamespaceRelationId;
  myself.objectId = nspoid;
  myself.objectSubId = 0;

  /* dependency on owner */
  recordDependencyOnOwner(NamespaceRelationId, nspoid, ownerId);

  /* dependencies on roles mentioned in default ACL */
  recordDependencyOnNewAcl(NamespaceRelationId, nspoid, 0, ownerId, nspacl);

  /* dependency on extension ... but not for magic temp schemas */
  recordDependencyOnCurrentExtension(&myself, false);

  CommandCounterIncrement();

  return;
}