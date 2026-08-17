

#include "storage.h"
#include "util.h"

#include <fstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

#include "pg.h"

#include "utils/rel.h"

#include "cdb/cdbappendonlyxlog.h"
#include "cdb/cdbvars.h"
#include "gucs.h"
#include "io.h"
#include "offload_tablespace_map.h"
#include "relfilelocator.h"
#include "url.h"
#include "virtual_index.h"
#include "yezzey_heap_api.h"
#include "yezzey_meta.h"
#include "ygpver.h"
#include "yproxy.h"

#define USE_YPX_LISTER = 1

int yezzey_log_level = DEBUG1;
int yezzey_ao_log_level = DEBUG1;

/*
 * This function used by AO-related realtion functions
 */
bool ensureFilepathLocal(const std::string &filepath) {
  struct stat buffer;
  return (stat(filepath.c_str(), &buffer) == 0);
}

int offloadRelationSegmentPath(Relation aorel, std::shared_ptr<IOadv> ioadv,
                               int64 modcount, int64 logicalEof,
                               const std::string &external_storage_path) {
  const std::string localPath = getlocalpath(ioadv->coords_);

  if (!ensureFilepathLocal(localPath)) {
    // nothing to do
    // elog(ERROR, "attempt to offload non-local relation");
    return 0;
  }

  int rc;
  int tot;
  /* XXX: chunk size should normally be configurable. */
  const size_t chunkSize = 1 << 20;
  int64 curr_read_chunk;

  std::vector<char> buffer(chunkSize);
#if IsGreenplum6
  const auto vfd =
      PathNameOpenFile((FileName)localPath.c_str(), O_RDONLY, 0600);
#else
  const auto vfd = PathNameOpenFile(localPath.c_str(), O_RDONLY);
#endif
  if (vfd <= 0) {
    elog(ERROR,
         "yezzey: failed to open %s file to transfer to external storage",
         localPath.c_str());
  }

  auto iohandler =
      YIO(ioadv, GpIdentity.segindex, modcount, external_storage_path);

  /* Create external storage reader handle to calculate total external files
   * size. this is needed to skip offloading of data already present in external
   * storage.
   */
  const auto virtual_size = yezzey_calc_virtual_relation_size(
      ioadv, GpIdentity.segindex, modcount, external_storage_path);

  if (virtual_size == -1) {
    elog(NOTICE, "yezzey: failed to calculate virtual size");
    return -1;
  }

  elog(NOTICE, "yezzey: relation virtual size calculated: %ld", virtual_size);
  auto progress = virtual_size;
  const auto offset_start = progress;

#if PG_VERSION_NUM < 120000
  const auto fLen = FileSeek(vfd, 0L, SEEK_END);

  if (fLen < logicalEof) {
    elog(ERROR,
         "yezzey: failed to offload corrupt relation, partial data file %s: "
         "%lu < %lu",
         localPath.c_str(), fLen, logicalEof);
  }

  /* reset seek to beginning */
  FileSeek(vfd, progress, SEEK_SET);

#else
  const auto fLen = FileSize(vfd);

  if (fLen < logicalEof) {
    elog(ERROR,
         "yezzey: failed to offload corrupt relation, partial data file %s: "
         "%lu < %lu",
         localPath.c_str(), fLen, logicalEof);
  }

#endif

  ioadv->multipart_upload = fLen > multipart_threshold;

  while (progress < logicalEof) {
    CHECK_FOR_INTERRUPTS();
    curr_read_chunk = chunkSize;
    if (progress + curr_read_chunk > logicalEof) {
      /* should not read beyond logical eof */
      curr_read_chunk = logicalEof - progress;
    }
    /* code */
#if IsGreenplum6
    rc = FileRead(vfd, buffer.data(), curr_read_chunk);
#else
    rc = FileRead(vfd, buffer.data(), curr_read_chunk, progress,
                  WAIT_EVENT_DATA_FILE_READ);
#endif
    if (rc < 0) {
      FileClose(vfd);
      return rc;
    }
    if (rc == 0) {
      /* maube file whipped away, maybe not, retry */
      continue;
    }

    tot = 0;
    char *bptr = buffer.data();

    while (tot < rc) {
      size_t currptrtot = rc - tot;
      if (!iohandler.io_write(bptr, &currptrtot)) {
        FileClose(vfd);
        return -1;
      }

      tot += currptrtot;
      bptr += currptrtot;
    }

    progress += rc;
  }

  const auto offset_finish = progress;

  /* data persisted in external storage, we can update out metadata relations */
  /* insert chunk metadata in virtual index  */
  YezzeyUpdateMetadataRelations(
      YezzeyFindAuxIndex(aorel->rd_id), ioadv->reloid, ioadv->coords_.filenode,
      ioadv->coords_.blkno /* blkno*/, offset_start, offset_finish,
      iohandler.adv_->use_gpg_crypto /* encrypted */, iohandler.use_kek(),
      0 /* reused */, modcount, iohandler.writer_->getInsertionStorageLsn(),
      iohandler.writer_->getExternalStoragePath().c_str() /* path */,
      yezzey_fqrelname_md5(ioadv->nspname, ioadv->relname).c_str());

  if (!iohandler.io_close()) {
    elog(ERROR, "yezzey: failed to complete %s offloading", localPath.c_str());
  } else {
    // debug output
    elog(DEBUG1, "yezzey: complete %s offloading", localPath.c_str());
  }

  FileClose(vfd);
  return rc;
}

void loadSegmentFromExternalStorage(Relation rel, const std::string &nspname,
                                    const std::string &relname, int segno,
                                    const relnodeCoord &coords,
                                    const std::string &dest_path) {
  /* TODO: pass this as argument? */
  const size_t chunkSize = 1 << 20;
  std::vector<char> buffer(chunkSize);

  std::ofstream ostrm(dest_path, std::ios::binary);

  auto ioadv = std::make_shared<IOadv>(
      nspname, relname, storage_class /* storage_class */, multipart_chunksize,
      coords /* filename */, rel->rd_id /* reloid */, use_gpg_crypto,
      yproxy_socket);

  /*
   * Create external storage reader handle to read segment files
   */
  auto iohandler = YIO(ioadv, GpIdentity.segindex);
  size_t position = 0;

  YezzeyLocator rnode;
  /* coords does contain origin tablespace */
  YezzeyGetRelSpcOid(rnode) = coords.spcNode;
  YezzeyGetRelDbOid(rnode) = YezzeyGetRelDbOid(YezzeyGetRelFileLocator(rel));
  YezzeyGetRelNode(rnode) = YezzeyGetRelNode(YezzeyGetRelFileLocator(rel));

  /*WAL-create new segfile */
  xlog_ao_insert(rnode, segno, 0, NULL, 0);

  while (!iohandler.reader_empty()) {
    size_t amount = chunkSize;
    if (!iohandler.io_read(buffer.data(), &amount)) {
      elog(ERROR, "failed to read file from external storage");
    }

    /* code */

    ostrm.write(buffer.data(), amount);
    if (ostrm.fail()) {
      elog(ERROR, "failed to read file from external storage");
    }

    xlog_ao_insert(rnode, segno, position, buffer.data(), amount);
    position += amount;
  }

  if (!iohandler.io_close()) {
    elog(ERROR, "yezzey: failed to complete %s offloading", dest_path.c_str());
  } else {
    elog(DEBUG1, "yezzey: complete %s offloading", dest_path.c_str());
  }
}

void loadRelationSegment(Relation aorel, Oid loadSpcOid, Oid orig_relnode,
                         int segno, const char *dest_path) {
  const auto rnode = YezzeyGetRelFileLocator(aorel);

  const auto coords = relnodeCoord(
      YezzeyGetRelSpcOid(rnode), YezzeyGetRelDbOid(rnode), orig_relnode, segno);

  std::string nspname;
  std::string relname;
  {
    /* c-function calls, need to release memory by-hand */
    const auto tp = SearchSysCache1(
        NAMESPACEOID, ObjectIdGetDatum(aorel->rd_rel->relnamespace));

    if (!HeapTupleIsValid(tp)) {
      elog(ERROR, "yezzey: failed to get namescape name of relation %s",
           RelationGetRelationName(aorel));
    }

    Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
    nspname = std::string(NameStr(nsptup->nspname));
    relname = std::string(RelationGetRelationName(aorel));
    ReleaseSysCache(tp);
  }

  std::string path;
  if (dest_path) {
    path = std::string(dest_path);
  } else {
    path = getlocalpath(relnodeCoord(loadSpcOid, YezzeyGetRelDbOid(rnode),
                                     YezzeyGetRelNode(rnode), segno));
  }

  elog(yezzey_ao_log_level, "contructed path %s", path.c_str());
  if (ensureFilepathLocal(path)) {
    return;
  }

  loadSegmentFromExternalStorage(aorel, nspname, relname, segno, coords, path);
}

int removeLocalFile(const char *localPath) {
  const auto res = std::remove(localPath);
  elog(yezzey_ao_log_level,
       "[YEZZEY_SMGR_BG] remove local file \"%s\", result: %d", localPath, res);
  return res;
}

std::string getlocalpath(const std::string &local_path, int segno) {
  if (segno != 0) {
    return local_path + "." + std::to_string(segno);
  }
  return local_path;
}

std::string getlocalpath(const relnodeCoord &coords) {
  std::string local_path(GetRelationPath(coords.dboid, coords.spcNode,
                                         coords.filenode, InvalidBackendId,
                                         MAIN_FORKNUM));

  return getlocalpath(local_path, coords.blkno);
}

void offloadRelationSegment(Relation aorel, int segno, int64 modcount,
                            int64 logicalEof,
                            const char *external_storage_path) {
  const auto rnode = YezzeyGetRelFileLocator(aorel);

  const auto coords =
      relnodeCoord(YezzeyGetRelSpcOid(rnode), YezzeyGetRelDbOid(rnode),
                   YezzeyGetRelNode(rnode), segno);

  /* xlog goes first */
  // xlog_smgr_local_truncate(rnode, MAIN_FORKNUM, 'a');

  auto tp = SearchSysCache1(NAMESPACEOID,
                            ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         RelationGetRelationName(aorel));
  }

  const auto nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  const auto nspname = std::string(NameStr(nsptup->nspname));
  const auto relname = std::string(RelationGetRelationName(aorel));
  const auto storage_path =
      !external_storage_path ? "" : std::string(external_storage_path);
  ReleaseSysCache(tp);

  const auto ioadv = std::make_shared<IOadv>(
      nspname, relname, storage_class /* storage_class */, multipart_chunksize,
      coords, aorel->rd_id /* reloid */, use_gpg_crypto, yproxy_socket);

  try {
    offloadRelationSegmentPath(aorel, ioadv, modcount, logicalEof,
                               storage_path);
  } catch (...) {
    elog(ERROR, "Caught an unexpected exception.");
  }

  /* we dont need to interact with s3 while in recovery*/

  const int64_t virtual_sz = 0;

#if 0
  if (/* support this feature */)
    auto fb = yezzey_virtual_relation_size(ioadv, GpIdentity.segindex);
    virtual_sz = fb.first;
#endif

  if (virtual_sz == -1) {
    elog(ERROR, "yezzey: failed to stat size of relation %s",
         RelationGetRelationName(aorel));
  }

  elog(NOTICE,
       "yezzey: relation segment reached external storage (blkno=%ld), up to "
       "logical eof %ld",
       coords.blkno, logicalEof);
}

Oid resolveTablespaceOidByName(const std::string &tablespacename) {
  Relation rel;
  SysScanDesc scan;
  HeapTuple tuple;
  ScanKeyData entry[1];
  Oid resOid;
  /*
   * Find the target tuple
   */
  rel = yezzey_relation_open(TableSpaceRelationId, RowExclusiveLock);

  const auto snap = RegisterSnapshot(GetTransactionSnapshot());

  ScanKeyInit(&entry[0], Anum_pg_tablespace_spcname, BTEqualStrategyNumber,
              F_NAMEEQ, CStringGetDatum(tablespacename.c_str()));
  scan = yezzey_systable_beginscan(rel, InvalidOid, false, snap, 1, entry);

  tuple = yezzey_systable_getnext(scan);

  if (!HeapTupleIsValid(tuple)) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("tablespace \"%s\" does not exist",
                           tablespacename.c_str())));
    /* never reached */
    return InvalidOid;
  }

#if PG_VERSION_NUM >= 120000
  resOid = ((Form_pg_class)GETSTRUCT(tuple))->oid;
#else
  resOid = HeapTupleGetOid(tuple);
#endif

  yezzey_systable_endscan(scan);
  UnregisterSnapshot(snap);
  yezzey_relation_close(rel, RowExclusiveLock);

  return resOid;
}

int statExternalTotal(Relation aorel, int segindx) {
  const auto rnode = YezzeyGetRelFileLocator(aorel);

  auto tp = SearchSysCache1(NAMESPACEOID,
                            ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         RelationGetRelationName(aorel));
  }

  Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  const auto nspname = std::string(NameStr(nsptup->nspname));

  ReleaseSysCache(tp);

  /* YezzeyGetRelSpcOid(rnode) == YEZZEYTABLESPACEOID here. we need
  to lookup in metadata table to resolve origin tablespace */

  const auto spcNode = resolveTablespaceOidByName(
      YezzeyGetRelationOriginTablespace(NULL, NULL, RelationGetRelid(aorel)));

  const auto coords = relnodeCoord(spcNode, YezzeyGetRelDbOid(rnode),
                                   YezzeyGetRelNode(rnode), -1 /* not used */);

  const auto ioadv = std::make_shared<IOadv>(
      nspname, std::string(RelationGetRelationName(aorel)),
      std::string(storage_class /*storage_class*/), multipart_chunksize,
      coords /* coords */, aorel->rd_id /* reloid */, use_gpg_crypto,
      yproxy_socket);
  return yezzey_virtual_relation_size(ioadv, segindx);
}

int statRelationSpaceUsage(Relation aorel, int segno, int64 modcount,
                           int64 logicalEof, size_t *local_bytes,
                           size_t *local_committed_bytes,
                           size_t *external_bytes) {

  const auto rnode = YezzeyGetRelFileLocator(aorel);

  auto tp = SearchSysCache1(NAMESPACEOID,
                            ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         RelationGetRelationName(aorel));
  }

  Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  const auto nspname = std::string(NameStr(nsptup->nspname));

  ReleaseSysCache(tp);

  /* YezzeyGetRelSpcOid(rnode) == YEZZEYTABLESPACEOID here. we need
  to lookup in metadata table to resolve origin tablespace */

  const auto spcNode = resolveTablespaceOidByName(
      YezzeyGetRelationOriginTablespace(NULL, NULL, RelationGetRelid(aorel)));

  const auto coords = relnodeCoord(spcNode, YezzeyGetRelDbOid(rnode),
                                   YezzeyGetRelNode(rnode), segno);

  const auto ioadv = std::make_shared<IOadv>(
      nspname, std::string(RelationGetRelationName(aorel)),
      std::string(storage_class /*storage_class*/), multipart_chunksize,
      coords /* coords */, aorel->rd_id /* reloid */, use_gpg_crypto,
      yproxy_socket);
  /* we dont need to interact with s3 while in recovery*/
  /* stat external storage usage */
  const auto virtual_sz = yezzey_relation_metadata_size(ioadv);
  if (virtual_sz == -1)
    elog(ERROR, "yezzey: failed to stat size of relation %s",
         RelationGetRelationName(aorel));

  *external_bytes = virtual_sz;

  /* No local storage cache logic for now */
  const auto local_path = getlocalpath(coords);

  *local_bytes = 0;

  if (YezzeyGetRelSpcOid(rnode) != YEZZEYTABLESPACE_OID) {

#if IsGreenplum6
    const auto f = PathNameOpenFile((FileName)local_path.c_str(),
                                    O_RDONLY | PG_BINARY, S_IRUSR);
#else
    const auto f = PathNameOpenFile(local_path.c_str(), O_RDONLY | PG_BINARY);
#endif

    if (f < 0)
      elog(ERROR, "could not open file \"%s\": %m", local_path.c_str());

#if PG_VERSION_NUM < 120000
    *local_bytes = FileSeek(f, 0L, SEEK_END);
#else
    *local_bytes = FileSize(f);
#endif

    FileClose(f);
  }

  //
  *local_committed_bytes = 0;
  // the following will not work since files in externakl storage may be
  // encrypted & compressed.
  // *local_commited_bytes = logicalEof - virtual_sz;
  return 0;
}

int statRelationChunksSpaceUsage(Relation aorel, size_t *local_bytes,
                                 size_t *local_commited_bytes,
                                 yezzeyChunkMeta **list, size_t *cnt_chunks) {
  const auto rnode = YezzeyGetRelFileLocator(aorel);

  /* YezzeyGetRelSpcOid(rnode) == YEZZEYTABLESPACEOID here. we need
  to lookup in metadata table to resolve origin tablespace */

  const auto spcNode = resolveTablespaceOidByName(
      YezzeyGetRelationOriginTablespace(NULL, NULL, RelationGetRelid(aorel)));

  const auto coords = relnodeCoord(spcNode, YezzeyGetRelDbOid(rnode),
                                   YezzeyGetRelNode(rnode), 0);

  auto tp = SearchSysCache1(NAMESPACEOID,
                            ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         RelationGetRelationName(aorel));
  }

  const auto nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  const auto nspname = std::string(NameStr(nsptup->nspname));

  ReleaseSysCache(tp);

  const auto ioadv = std::make_shared<IOadv>(
      nspname, std::string(RelationGetRelationName(aorel)),
      std::string(storage_class /*storage_class*/), multipart_chunksize,
      coords /* coords */, aorel->rd_id /* reloid */, use_gpg_crypto,
      yproxy_socket);
  /* we dont need to interact with s3 while in recovery*/

#ifdef USE_YPX_LISTER
  auto lister = YProxyLister(ioadv, GpIdentity.segindex);
#else
#error "listing feature not supported"
#endif

  /* stat external storage usage */

  const auto meta = lister.list_relation_chunks();
  *cnt_chunks = meta.size();

  Assert((*cnt_chunks) >= 0);

  // do copy;
  *list = (struct yezzeyChunkMeta *)palloc(sizeof(struct yezzeyChunkMeta) *
                                           (*cnt_chunks));

  for (size_t i = 0; i < *cnt_chunks; ++i) {
    (*list)[i].chunkSize = meta[i].chunkSize;
    (*list)[i].chunkName = pstrdup(meta[i].chunkName.c_str());
  }

  /* No local storage cache logic for now */
  const auto local_path = getlocalpath(coords);
  *local_bytes = 0;

  if (YezzeyGetRelSpcOid(rnode) != YEZZEYTABLESPACE_OID) {

#if IsGreenplum6
    const auto f = PathNameOpenFile((FileName)local_path.c_str(),
                                    O_RDONLY | PG_BINARY, S_IRUSR);
#else
    const auto f = PathNameOpenFile(local_path.c_str(), O_RDONLY | PG_BINARY);
#endif

    if (f < 0)
      elog(ERROR, "could not open file \"%s\": %m", local_path.c_str());

#if PG_VERSION_NUM < 120000
    *local_bytes = FileSeek(f, 0L, SEEK_END);
#else
    *local_bytes = FileSize(f);
#endif

    FileClose(f);
  }

  // *local_bytes =
  // std::filesystem::file_size(std::filesystem::path(local_path));

  *local_commited_bytes = 0;
  return 0;
}

int yezzey_get_block_from_file_path(const char *path) {
  const std::string pathstr = path;
  int i = 0;
  int previ = 0;
  for (int n = 0; n < 7; ++n) {
    previ = i;
    i = pathstr.find('_', i + 1);
  }
  const auto blkno = pathstr.substr(previ + 1, i - previ);
  return atoi(blkno.c_str());
}
