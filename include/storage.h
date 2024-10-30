#ifndef YEZZEY_STORAGE_H
#define YEZZEY_STORAGE_H

// XXX: todo proder interface for external storage offloading

#include <unistd.h>

#include "pg.h"

#include "gucs.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

typedef struct yezzeyChunkMeta {
  const char *chunkName;
  size_t chunkSize;
} yezzeyChunkMeta;

#ifdef __cplusplus
#include "string"

#include "io_adv.h"
#include "types.h"
#include <memory>

std::string getlocalpath(std::string local_path, int segno);
bool ensureFilepathLocal(const std::string &filepath);
std::string getlocalpath(const relnodeCoord &coords);

int offloadRelationSegmentPath(Relation aorel, std::shared_ptr<IOadv> ioadv,
                               int64 modcount, int64 logicalEof,
                               const std::string &external_storage_path);
#endif

EXTERNC int offloadRelationSegment(Relation aorel, int segno, int64 modcount,
                                   int64 logicalEof,
                                   const char *external_storage_path);

EXTERNC int loadRelationSegment(Relation aorel, Oid loadSpcOid, Oid orig_relnode, int segno,
                                const char *dest_path);

EXTERNC bool ensureFileLocal(RelFileNode rnode, BackendId backend,
                             ForkNumber forkNum, BlockNumber blkno);

EXTERNC int statRelationSpaceUsage(Relation aorel, int segno, int64 modcount,
                                   int64 logicalEof, size_t *local_bytes,
                                   size_t *local_commited_bytes,
                                   size_t *external_bytes);

EXTERNC int statRelationSpaceUsagePerExternalChunk(
    Relation aorel, int segno, int64 modcount, int64 logicalEof,
    size_t *local_bytes, size_t *local_commited_bytes, yezzeyChunkMeta **list,
    size_t *cnt_chunks);

#endif /* YEZZEY_STORAGE_H */