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
#include "storage.h"
#include "offload_tablespace_map.h"
/*
 * yezzey_delete_chunk_internal:
 * Given external chunk path, remove it from external storage
 * TBD: check, that chunk status is obsolete and other sanity checks
 * to avoid deleting chunk, which can we needed to read relation data
 */
int yezzey_delete_chunk_internal(const char *external_chunk_path) {
  try {
    auto ioadv = std::make_shared<IOadv>(
        "", "", std::string(storage_class /*storage_class*/),
        multipart_chunksize, DEFAULTTABLESPACE_OID, "" /* coords */,
        InvalidOid /* reloid */, use_gpg_crypto, yproxy_socket);

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
        "", "", std::string(storage_class /*storage_class*/),
        multipart_chunksize, DEFAULTTABLESPACE_OID, "" /* coords */,
        InvalidOid /* reloid */, use_gpg_crypto, yproxy_socket);

    std::string storage_path(yezzey_block_namespace_path(segindx));

    auto deleter = std::make_shared<YProxyDeleter>(ioadv, ssize_t(segindx),
                                                   confirm, crazyDrop);

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

int yezzey_vacuum_garbage_relation_internal(Relation aorel, int segindx,
                                            bool confirm, bool crazyDrop) {
  try {
    auto rnode = aorel->rd_node;

    auto tp = SearchSysCache1(NAMESPACEOID,
                              ObjectIdGetDatum(RelationGetNamespace(aorel)));

    if (!HeapTupleIsValid(tp)) {
      elog(ERROR, "yezzey: failed to get namescape name of relation %s",
           RelationGetRelationName(aorel));
    }

    auto nsptup = (Form_pg_namespace)GETSTRUCT(tp);
    auto nspname = std::string(NameStr(nsptup->nspname));

    auto spcNode = resolveTablespaceOidByName(
      YezzeyGetRelationOriginTablespace(NULL, NULL, RelationGetRelid(aorel)));
    
    relnodeCoord coords{spcNode, rnode.dbNode,
                        rnode.relNode, segindx};
    relnodeCoord coords_old{rnode.spcNode, rnode.dbNode,
                        rnode.relNode, segindx};
    ReleaseSysCache(tp);

    std::string relname = RelationGetRelationName(aorel);

    std::string storage_path(
        yezzey_block_db_file_path(nspname, relname, coords, segindx));
    std::string storage_path_old(
        yezzey_block_db_file_path(nspname, relname, coords_old, segindx));
    elog(NOTICE, "storage_path: %s", storage_path.c_str());
    elog(NOTICE, "storage_path_old: %s", storage_path_old.c_str());
    auto ioadv = std::make_shared<IOadv>(
        nspname, relname, std::string(storage_class),
        multipart_chunksize, coords, aorel->rd_id, use_gpg_crypto, yproxy_socket);
    auto deleter = std::make_shared<YProxyDeleter>(ioadv, ssize_t(segindx),
                                                   confirm, crazyDrop);

    if (deleter->deleteChunk(storage_path) && deleter->deleteChunk(storage_path_old)) {
      return 0;
    }

    return -1;
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage reader for chunk");
    return 0;
  }
  return 0;
}

int yezzey_vacuum_garbage_relation_internal_oid(Oid reloid, int segindx,
                                                bool confirm, bool crazyDrop) {
  Relation rel = relation_open(reloid, NoLock);
  int rc =
      yezzey_vacuum_garbage_relation_internal(rel, segindx, confirm, crazyDrop);
  relation_close(rel, NoLock);
  return rc;
}

int yezzey_delele_obsolete_internal(int segindx, bool crazy_drop, char *dbname,
                                    Oid nspoid, Oid dboid) {
  try {
    auto ioadv = std::make_shared<IOadv>(
        "", "", std::string(storage_class /*storage_class*/),
        multipart_chunksize, DEFAULTTABLESPACE_OID, "" /* coords */,
        InvalidOid /* reloid */, use_gpg_crypto, yproxy_socket);

    std::string storage_path(yezzey_block_db_path(nspoid, dboid, segindx));

    auto deleter = std::make_shared<YProxyDeleterV2>(
        ioadv, ssize_t(segindx), std::string(dbname), crazy_drop);

    if (deleter->Delete(storage_path)) {
      return 0;
    }

    return -1;
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage delete");
    return 0;
  }
  return 0;
}

int yezzey_collect_obsolete_internal(int segindx, char *dbname, Oid nspoid,
                                     Oid dboid) {
  try {
    auto ioadv = std::make_shared<IOadv>(
        "", "", std::string(storage_class /*storage_class*/),
        multipart_chunksize, DEFAULTTABLESPACE_OID, "" /* coords */,
        InvalidOid /* reloid */, use_gpg_crypto, yproxy_socket);

    std::string storage_path(yezzey_block_db_path(nspoid, dboid, segindx));

    auto deleter = std::make_shared<YProxyDeleterV2>(ioadv, ssize_t(segindx),
                                                     std::string(dbname));
    // TODO get lock on smthng
    if (deleter->Collect(storage_path)) {
      return 0;
    }

    return -1;
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage obsolete");
    return 0;
  }
  return 0;
}
