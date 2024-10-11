

#include "storage.h"
#include "util.h"

#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

#include "pg.h"

#include "io.h"
#include <iostream>

#include "virtual_index.h"

#include "storage_lister.h"
#include "url.h"
#include "yproxy.h"

#include "yezzey_meta.h"

#include "offload_tablespace_map.h"

#include "cdb/cdbvars.h"

#include "cdb/cdbappendonlyxlog.h"

#include "ygpver.h"

#define USE_YPX_LISTER = 1

int yezzey_log_level = INFO;
int yezzey_ao_log_level = INFO;

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
  size_t chunkSize = 1 << 20;
  File vfd;
  int64 curr_read_chunk;
  int64 virtual_size;

  std::vector<char> buffer(chunkSize);
#if IsGreenplum6
  vfd = PathNameOpenFile((FileName)localPath.c_str(), O_RDONLY, 0600);
#else
  vfd = PathNameOpenFile(localPath.c_str(), O_RDONLY);
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
  virtual_size = yezzey_calc_virtual_relation_size(
      ioadv, GpIdentity.segindex, modcount, external_storage_path);

  if (virtual_size == -1) {
    elog(NOTICE, "yezzey: failed to calculate virtual size");
    return -1;
  }

  elog(NOTICE, "yezzey: relation virtual size calculated: %ld", virtual_size);
  auto progress = virtual_size;
  auto offset_start = progress;

#if PG_VERSION_NUM < 120000
  auto fLen = FileSeek(vfd, 0L, SEEK_END);

  if (fLen < logicalEof) {
    elog(ERROR,
         "yezzey: failed to offload corrupt relation, partial data file %s: "
         "%lu < %lu",
         localPath.c_str(), fLen, logicalEof);
  }

  /* reset seek to beginning */
  FileSeek(vfd, progress, SEEK_SET);

#else
  auto fLen = FileSize(vfd);

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

  auto offset_finish = progress;

  /* data persisted in external storage, we can update out metadata relations */
  /* insert chunk metadata in virtual index  */
  YezzeyUpdateMetadataRelations(
      YezzeyFindAuxIndex(aorel->rd_id), ioadv->reloid, ioadv->coords_.filenode,
      ioadv->coords_.blkno /* blkno*/, offset_start, offset_finish,
      ioadv->use_gpg_crypto /* encrypted */, 0 /* reused */, modcount,
      iohandler.writer_->getInsertionStorageLsn(),
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

int loadSegmentFromExternalStorage(Relation rel, const std::string &nspname,
                                   const std::string &relname, int segno,
                                   const relnodeCoord &coords,
                                   const std::string &dest_path) {
  size_t chunkSize;

  chunkSize = 1 << 20;
  std::vector<char> buffer(chunkSize);

  std::ofstream ostrm(dest_path, std::ios::binary);

  /* FIXME */

  auto ioadv = std::make_shared<IOadv>(
      gpg_engine_path, gpg_key_id, storage_config, nspname, relname,
      storage_host /* host */, storage_bucket /*bucket*/,
      storage_prefix /*prefix*/, storage_class /* storage_class */,
      multipart_chunksize, coords /* filename */, rel->rd_id /* reloid */,
      walg_bin_path, walg_config_path, use_gpg_crypto, yproxy_socket);

  /*
   * Create external storage reader handle to read segment files
   */
  auto iohandler = YIO(ioadv, GpIdentity.segindex);
  size_t position = 0;

  RelFileNode rnode;
  /* coords does not contain tablespace */
  rnode.spcNode = DEFAULTTABLESPACE_OID;
  rnode.dbNode = rel->rd_node.dbNode;
  rnode.relNode = rel->rd_node.relNode;

  /*WAL-create new segfile */
  xlog_ao_insert(rnode, segno, 0, NULL, 0);

  while (!iohandler.reader_empty()) {
    size_t amount = chunkSize;
    if (!iohandler.io_read(buffer.data(), &amount)) {
      elog(ERROR, "failed to read file from external storage");
      return -1;
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
  return 0;
  // return std::rename(tmp_path.c_str(), dest_path.c_str());
}

int loadRelationSegment(Relation aorel, Oid orig_relnode, int segno,
                        const char *dest_path) {
  auto rnode = aorel->rd_node;

  auto coords = relnodeCoord(rnode.spcNode, rnode.dbNode, orig_relnode, segno);

  std::string path;
  if (dest_path) {
    path = std::string(dest_path);
  } else {
    path = getlocalpath(coords);
  }

  elog(yezzey_ao_log_level, "contructed path %s", path.c_str());
  if (ensureFilepathLocal(path)) {
    // nothing to do
    return 0;
  }

  std::string nspname;
  std::string relname;
  {
    /* c-function calls, need to release memory by-hand */
    auto tp = SearchSysCache1(NAMESPACEOID,
                              ObjectIdGetDatum(aorel->rd_rel->relnamespace));

    if (!HeapTupleIsValid(tp)) {
      elog(ERROR, "yezzey: failed to get namescape name of relation %s",
           aorel->rd_rel->relname.data);
    }

    Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
    nspname = std::string(NameStr(nsptup->nspname));
    relname = std::string(aorel->rd_rel->relname.data);
    ReleaseSysCache(tp);
  }

  return loadSegmentFromExternalStorage(aorel, nspname, relname, segno, coords,
                                        path);
}

bool ensureFileLocal(RelFileNode rnode, BackendId backend, ForkNumber forkNum,
                     BlockNumber blkno) {
  /* MDB-19689: do not consult catalog */

  elog(yezzey_log_level, "ensuring %d is local", rnode.relNode);
  bool result = true;

  auto path = std::string(relpathbackend(rnode, backend, forkNum));
  /* TBD: construct path*/

  // result = ensureFilepathLocal(path);

  return result;
}

int removeLocalFile(const char *localPath) {
  auto res = std::remove(localPath);
  elog(yezzey_ao_log_level,
       "[YEZZEY_SMGR_BG] remove local file \"%s\", result: %d", localPath, res);
  return res;
}

std::string getlocalpath(std::string local_path, int segno) {
  if (segno != 0) {
    local_path = local_path + "." + std::to_string(segno);
  }
  return local_path;
}

std::string getlocalpath(const relnodeCoord &coords) {
  std::string local_path(GetRelationPath(coords.dboid, coords.spcNode,
                                         coords.filenode, InvalidBackendId,
                                         MAIN_FORKNUM));

  return getlocalpath(local_path, coords.blkno);
}

int offloadRelationSegment(Relation aorel, int segno, int64 modcount,
                           int64 logicalEof,
                           const char *external_storage_path) {
  auto rnode = aorel->rd_node;
  int rc;

  auto coords = relnodeCoord(rnode.spcNode, rnode.dbNode, rnode.relNode, segno);

  /* xlog goes first */
  // xlog_smgr_local_truncate(rnode, MAIN_FORKNUM, 'a');

  auto tp = SearchSysCache1(NAMESPACEOID,
                            ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         aorel->rd_rel->relname.data);
  }

  auto nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  auto nspname = std::string(NameStr(nsptup->nspname));
  auto relname = std::string(aorel->rd_rel->relname.data);
  auto storage_path =
      !external_storage_path ? "" : std::string(external_storage_path);
  ReleaseSysCache(tp);

  auto ioadv = std::make_shared<IOadv>(
      gpg_engine_path, gpg_key_id, storage_config, nspname, relname,
      storage_host /* host */, storage_bucket /*bucket*/,
      storage_prefix /*prefix*/, storage_class /* storage_class */,
      multipart_chunksize, coords, aorel->rd_id /* reloid */, walg_bin_path,
      walg_config_path, use_gpg_crypto, yproxy_socket);

  try {
    if ((rc = offloadRelationSegmentPath(aorel, ioadv, modcount, logicalEof,
                                         storage_path)) < 0) {
      return rc;
    }
  } catch (...) {
    elog(ERROR, "Caught an unexpected exception.");
    return -1;
  }

  /* we dont need to interact with s3 while in recovery*/

  auto virtual_sz = yezzey_virtual_relation_size(ioadv, GpIdentity.segindex);

  if (virtual_sz == -1) {
    elog(ERROR, "yezzey: failed to stat size of relation %s",
         aorel->rd_rel->relname.data);
  }

  elog(INFO,
       "yezzey: relation segment reached external storage (blkno=%ld), up to "
       "logical eof %ld",
       coords.blkno, logicalEof);

  // elog(INFO,
  //      "yezzey: relation segment reached external storage (blkno=%ld),
  //      virtual " "size %ld, logical eof %ld", coords.blkno, virtual_sz,
  //      logicalEof);
  return 0;
}

static Oid resolveTablespaceOidByName(std::string tablespacename) {

  Relation rel;
  HeapScanDesc scan;
  HeapTuple tuple;
  ScanKeyData entry[1];
  Oid resOid;
  /*
   * Find the target tuple
   */
  rel = heap_open(TableSpaceRelationId, RowExclusiveLock);

  ScanKeyInit(&entry[0], Anum_pg_tablespace_spcname, BTEqualStrategyNumber,
              F_NAMEEQ, CStringGetDatum(tablespacename.c_str()));
  scan = heap_beginscan_catalog(rel, 1, entry);
  tuple = heap_getnext(scan, ForwardScanDirection);

  if (!HeapTupleIsValid(tuple)) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("tablespace \"%s\" does not exist",
                           tablespacename.c_str())));
    /* never reached */
    return InvalidOid;
  }

  resOid = HeapTupleGetOid(tuple);

  heap_endscan(scan);
  heap_close(rel, RowExclusiveLock);

  return resOid;
}

int statRelationSpaceUsage(Relation aorel, int segno, int64 modcount,
                           int64 logicalEof, size_t *local_bytes,
                           size_t *local_commited_bytes,
                           size_t *external_bytes) {

  auto rnode = aorel->rd_node;

  auto tp = SearchSysCache1(NAMESPACEOID,
                            ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         aorel->rd_rel->relname.data);
  }

  Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  auto nspname = std::string(NameStr(nsptup->nspname));

  ReleaseSysCache(tp);

  /* rnode.spcNode == YEZZEYTABLESPACEOID here. we need
  to lookup in metadata table to resolve origin tablespace */

  auto spcNode = resolveTablespaceOidByName(
      YezzeyGetRelationOriginTablespace(RelationGetRelid(aorel)));

  auto coords = relnodeCoord(spcNode, rnode.dbNode, rnode.relNode, segno);

  auto ioadv = std::make_shared<IOadv>(
      std::string(gpg_engine_path), std::string(gpg_key_id),
      std::string(storage_config), nspname,
      std::string(aorel->rd_rel->relname.data),
      std::string(storage_host /*host*/),
      std::string(storage_bucket /*bucket*/),
      std::string(storage_prefix /*prefix*/),
      std::string(storage_class /*storage_class*/), multipart_chunksize,
      coords /* coords */, aorel->rd_id /* reloid */,
      std::string(walg_bin_path), std::string(walg_config_path), use_gpg_crypto,
      yproxy_socket);
  /* we dont need to interact with s3 while in recovery*/
  /* stat external storage usage */
  auto virtual_sz = yezzey_virtual_relation_size(ioadv, GpIdentity.segindex);
  if (virtual_sz == -1) {
    elog(ERROR, "yezzey: failed to stat size of relation %s",
         aorel->rd_rel->relname.data);
  }

  *external_bytes = virtual_sz;

  /* No local storage cache logic for now */
  auto local_path = getlocalpath(coords);

  *local_bytes = 0;
  // *local_bytes =
  // std::filesystem::file_size(std::filesystem::path(local_path));

  // Assert(virtual_sz <= logicalEof);
  //
  *local_commited_bytes = 0;
  // the following will not work since files in externakl storage may be
  // encrypted & compressed.
  // *local_commited_bytes = logicalEof - virtual_sz;
  return 0;
}

int statRelationSpaceUsagePerExternalChunk(Relation aorel, int segno,
                                           int64 modcount, int64 logicalEof,
                                           size_t *local_bytes,
                                           size_t *local_commited_bytes,
                                           yezzeyChunkMeta **list,
                                           size_t *cnt_chunks) {
  auto rnode = aorel->rd_node;

  /* rnode.spcNode == YEZZEYTABLESPACEOID here. we need
  to lookup in metadata table to resolve origin tablespace */

  auto spcNode = resolveTablespaceOidByName(
      YezzeyGetRelationOriginTablespace(RelationGetRelid(aorel)));

  auto coords = relnodeCoord(spcNode, rnode.dbNode, rnode.relNode, segno);

  auto tp = SearchSysCache1(NAMESPACEOID,
                            ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         aorel->rd_rel->relname.data);
  }

  auto nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  auto nspname = std::string(NameStr(nsptup->nspname));

  ReleaseSysCache(tp);

  auto ioadv = std::make_shared<IOadv>(
      std::string(gpg_engine_path), std::string(gpg_key_id),
      std::string(storage_config), nspname,
      std::string(aorel->rd_rel->relname.data),
      std::string(storage_host /*host*/),
      std::string(storage_bucket /*bucket*/),
      std::string(storage_prefix /*prefix*/),
      std::string(storage_class /*storage_class*/), multipart_chunksize,
      coords /* coords */, aorel->rd_id /* reloid */,
      std::string(walg_bin_path), std::string(walg_config_path), use_gpg_crypto,
      yproxy_socket);
  /* we dont need to interact with s3 while in recovery*/

#ifdef USE_YPX_LISTER
  auto lister = YProxyLister(ioadv, GpIdentity.segindex);
#else
#error "listing feature not supported"
#endif

  /* stat external storage usage */

  auto meta = lister.list_relation_chunks();
  *cnt_chunks = meta.size();

  Assert((*cnt_chunks) >= 0);

  // do copy;
  // list will be allocated via malloc, not PostgreSQL memory context, so should
  // be free in the end of function call
  // this actually may lead to memory leak in multiple ways
  *list = (struct yezzeyChunkMeta *)palloc(sizeof(struct yezzeyChunkMeta) *
                                           (*cnt_chunks));

  for (size_t i = 0; i < *cnt_chunks; ++i) {
    (*list)[i].chunkSize = meta[i].chunkSize;
    (*list)[i].chunkName = strdup(meta[i].chunkName.c_str());
  }

  /* No local storage cache logic for now */
  auto local_path = getlocalpath(coords);
  *local_bytes = 0;

  // *local_bytes =
  // std::filesystem::file_size(std::filesystem::path(local_path));

  *local_commited_bytes = 0;
  return 0;
}
