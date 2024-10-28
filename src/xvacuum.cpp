/*
 * xvacuum - external storage (Garbage, stale files) VACUUM
 */

#include "xvacuum.h"
#include "gucs.h"
#include "pg.h"
#include "yproxy.h"
#include <string>
#include <url.h>
#include <util.h>

/*
 * yezzey_delete_chunk_internal:
 * Given external chunk path, remove it from external storage
 * TBD: check, that chunk status is obsolete and other sanity checks
 * to avoid deleting chunk, which can we needed to read relation data
 */
int yezzey_delete_chunk_internal(const char *external_chunk_path) {
  try {
    auto ioadv = std::make_shared<IOadv>(
        "", "", std::string(storage_class /*storage_class*/), multipart_chunksize,
        DEFAULTTABLESPACE_OID, "" /* coords */, InvalidOid /* reloid */,
        use_gpg_crypto, yproxy_socket);

    std::string storage_path(external_chunk_path);

    auto deleter = std::make_shared<YProxyDeleter>(ioadv);

    if (deleter->deleteChunk(storage_path)) {
      return 0;
    }

    return -1;
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage reader for chunk");
    return 0;
  }
  return 0;
}

/*
 * yezzey_vacuum_garbage_internal:
 * Given external storage path prefix AND segment ID, remove all garbage from
 * external storage.
 * TBD: check, that chunk status is obsolete and other sanity checks
 * to avoid deleting chunk, which can we needed to read relation data
 */
int yezzey_vacuum_garbage_internal(int segindx, bool confirm, bool crazyDrop) {
  try {
    auto ioadv = std::make_shared<IOadv>(
        "", "",
        std::string(storage_class /*storage_class*/), multipart_chunksize,
        DEFAULTTABLESPACE_OID, "" /* coords */, InvalidOid /* reloid */,
        use_gpg_crypto, yproxy_socket);

    std::string storage_path(yezzey_block_namespace_path(segindx));

    auto deleter =
        std::make_shared<YProxyDeleter>(ioadv, ssize_t(segindx), confirm);

    if (deleter->deleteChunk(storage_path)) {
      return 0;
    }

    return -1;
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage reader for chunk");
    return 0;
  }
  return 0;
}
int yezzey_vacuum_garbage_relation_internal(Relation rel,int segindx, bool confirm,bool crazyDrop){
  try {
    auto ioadv = std::make_shared<IOadv>("", "",
        std::string(storage_class /*storage_class*/), multipart_chunksize,
        DEFAULTTABLESPACE_OID, "" /* coords */, InvalidOid /* reloid */,
        use_gpg_crypto, yproxy_socket);

    auto tp = SearchSysCache1(NAMESPACEOID,
                              ObjectIdGetDatum(RelationGetNamespace(rel)));

    if (!HeapTupleIsValid(tp)) {
      elog(ERROR, "yezzey: failed to get namescape name of relation %d",
           RelationGetNamespace(rel));
    }
    
    relnodeCoord coords{1663,rel->rd_node.dbNode,rel->rd_node.relNode,segindx};
    Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
    auto nspname = std::string(nsptup->nspname.data);
    std::string relname = RelationGetRelationName(rel);

    std::string storage_path(yezzey_block_db_file_path(nspname,relname,coords,segindx));

    auto deleter =
        std::make_shared<YProxyDeleter>(ioadv, ssize_t(segindx), confirm);
    ReleaseSysCache(tp);

    if (deleter->deleteChunk(storage_path)) {
      return 0;
    }

    return -1;
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage reader for chunk");
    return 0;
  }
  return 0;
}
int yezzey_vacuum_garbage_relation_internal(Oid reloid,int segindx, bool confirm, bool crazyDrop) {
    Relation rel = relation_open(reloid,NoLock);
    int rc = yezzey_vacuum_garbage_relation_internal(rel,segindx,confirm,crazyDrop);
    relation_close(rel,NoLock);
    return rc;
}
