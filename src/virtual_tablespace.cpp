
/*
 * Yezzey
 */

#include "virtual_tablespace.h"
#include "yezzey_meta.h"
extern "C" {
#include "access/reloptions.h"
#include "nodes/makefuncs.h"
}
/*
 * Execute ALTER TABLE SET TABLESPACE for cases where there is no tuple
 * rewriting to be done, so we just want to copy the data as fast as possible.
 */
void YezzeyATExecSetTableSpace(Relation aorel, Oid reloid,
                               Oid desttablespace_oid) {
  /*
   * Need lock here in case we are recursing to toast table or index
   */

  /*
   * We cannot support moving mapped relations into different tablespaces.
   * (In particular this eliminates all shared catalogs.)
   */
  if (RelationIsMapped(aorel))
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot move system relation \"%s\"",
                           RelationGetRelationName(aorel))));

  /*
   * Don't allow moving temp tables of other backends ... their local buffer
   * manager is not going to cope.
   */
  if (RELATION_IS_OTHER_TEMP(aorel))
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot move temporary tables of other sessions")));

  /* We do not take care of TOAST relations here */

  /* Get a modifiable copy of the relation's pg_class row */
  auto pg_class = heap_open(RelationRelationId, RowExclusiveLock);

  auto tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(reloid));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for relation %u", reloid);

  auto rd_rel = (Form_pg_class)GETSTRUCT(tuple);

  /*
   * Since we copy the file directly without looking at the shared buffers,
   * we'd better first flush out any pages of the source relation that are
   * in shared buffers.  We assume no new changes will be made while we are
   * holding exclusive lock on the rel.
   */
  FlushRelationBuffers(aorel);

  /*
   * Relfilenodes are not unique in databases across tablespaces, so we need
   * to allocate a new one in the new tablespace.
   */
  /* Open old and new relation */
  /*
   * Create and copy all forks of the relation, and schedule unlinking of
   * old physical files.
   *
   * NOTE: any conflict in relfilenode value will be caught in
   * RelationCreateStorage().
   */
  /* data already copied */
  /*
   * Append-only tables now include init forks for unlogged tables, so we copy
   * over all forks. AO tables, so far, do not have visimap or fsm forks.
   */

  if (desttablespace_oid == YEZZEYTABLESPACE_OID) {
  /* drop old relation, and close new one */
    RelationDropStorage(aorel);

    bool isnull;
    Datum datum =
        SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
    List *defs = list_make1(makeDefElem(pstrdup("offloaded"), NULL));
    Datum newOptions = transformRelOptions(isnull ? (Datum)0 : datum, defs,
                                           NULL, NULL, false, false);

    Datum repl_val[Natts_pg_class];
    bool repl_null[Natts_pg_class];
    bool repl_repl[Natts_pg_class];
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    if (newOptions != (Datum)0)
      repl_val[Anum_pg_class_reloptions - 1] = newOptions;
    else
      repl_null[Anum_pg_class_reloptions - 1] = true;
    repl_repl[Anum_pg_class_reloptions - 1] = true;

    HeapTuple newtuple = heap_modify_tuple(tuple, RelationGetDescr(pg_class),
                                           repl_val, repl_null, repl_repl);

    simple_heap_update(pg_class,  &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(pg_class, newtuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(aorel), 0);
    heap_freetuple(newtuple);
  } else {
    bool isnull;
    Datum datum =
        SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
    List *defs = list_make1(makeDefElem(pstrdup("offloaded"), NULL));
    Datum newOptions = transformRelOptions(isnull ? (Datum)0 : datum, defs,
                                           NULL, NULL, false, true);

    Datum repl_val[Natts_pg_class];
    bool repl_null[Natts_pg_class];
    bool repl_repl[Natts_pg_class];
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    if (newOptions != (Datum)0)
      repl_val[Anum_pg_class_reloptions - 1] = newOptions;
    else
      repl_null[Anum_pg_class_reloptions - 1] = true;
    repl_repl[Anum_pg_class_reloptions - 1] = true;

    HeapTuple newtuple = heap_modify_tuple(tuple, RelationGetDescr(pg_class),
                                           repl_val, repl_null, repl_repl);

    simple_heap_update(pg_class,  &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(pg_class, newtuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(aorel), 0);
    heap_freetuple(newtuple);
  }

  heap_freetuple(tuple);

  heap_close(pg_class, RowExclusiveLock);

  /* Make sure the reltablespace change is visible */
  CommandCounterIncrement();
  /* Clean up */
}
