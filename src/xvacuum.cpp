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
        "", "", std::string(storage_host /*host*/),
        std::string(storage_bucket /*bucket*/),
        std::string(storage_prefix /*prefix*/),
        std::string(storage_class /*storage_class*/), multipart_chunksize,
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
        "", "", std::string(storage_host /*host*/),
        std::string(storage_bucket /*bucket*/),
        std::string(storage_prefix /*prefix*/),
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
