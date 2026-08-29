
/**
 * @file util.cpp
 *
 */

#include "util.h"

#include <string>
#include <vector>

#include "virtual_index.h"
#include "yproxy.h"

#include "io.h"
#include "io_adv.h"

#include "url.h"

#include "relpath_parse.h"
#include "yezzey_standalone.h"

#define DEFAULTTABLESPACE_OID 1663 /* FIXME */

const char *baseYezzeyPath = "/basebackups_005/yezzey/";

relnodeCoord getRelnodeCoordinate(Oid spcNode, const std::string &fileName) {
  uint32_t dbOid = 0;
  uint32_t relfilenodeOid = 0;
  int64_t blkno = 0;

  if (!parseRelnodePath(fileName, &dbOid, &relfilenodeOid, &blkno)) {
    elog(ERROR, "yezzey: corrupted data file path %s", fileName.c_str());
  }

  return relnodeCoord(spcNode, dbOid, relfilenodeOid, blkno);
}

void getYezzeyExternalStoragePathByCoords(const char *nspname,
                                          const char *relname, Oid spcNode,
                                          Oid dbNode, Oid relNode,
                                          int32_t segblockno /* segment no*/,
                                          int32_t segid, char **dest) {

  /* FIXME: Support for non-default table space? */
  auto coords = relnodeCoord(spcNode, dbNode, relNode, segblockno);
  auto prefix = getYezzeyRelationUrl_internal(nspname, relname, coords, segid);

  /* +1 for the terminating null byte that strcpy writes */
  *dest = (char *)palloc(sizeof(char) * (prefix.size() + 1));
  strcpy(*dest, prefix.c_str());
  return;
}

std::vector<int64_t> parseModcounts(const std::string &prefix,
                                    std::string name) {
  std::vector<int64_t> res;
  auto indx = name.find(prefix);
  if (indx == std::string::npos) {
    return res;
  }
  indx += prefix.size();
  auto endindx = name.find("_aoseg", indx);

  size_t prev = 0;

  /* name[endindx] -> not digit */
  /* mc1_D_mc2_D_mc3_D_mc4 */
  for (size_t it = indx; it <= endindx; ++it) {
    if (!isdigit((unsigned char)name[it])) {
      if (prev) {
        res.push_back(prev);
      }
      prev = 0;
      continue;
    }
    if (prev > SIZE_MAX / 10) {
      elog(ERROR, "yezzey: modcount overflow in path %s", name.c_str());
    }
    prev *= 10;
    prev += name[it] - '0';
  }

  return res;
}

/* calc size of external files */
int64_t yezzey_virtual_relation_size(std::shared_ptr<IOadv> adv,
                                     int32_t segid) {
  try {
    auto lister = YProxyLister(adv, segid);
    int64_t sz = 0;
    auto chunks = lister.list_relation_chunks();
    for (auto chunk : chunks) {
      sz += chunk.chunkSize;
    }
    /* external reader destruct */
    return sz;
  } catch (...) {
    return -1;
  }
}

int64_t yezzey_relation_metadata_size(std::shared_ptr<IOadv> adv) {
  auto order =
      YezzeyVirtualGetOrder(YezzeyFindAuxIndex(adv->reloid), adv->reloid,
                            adv->coords_.filenode, adv->coords_.blkno);

  int64_t sz = 0;

  for (auto &o : order) {
    sz += o.size;
  }

  /* external reader destruct */
  return sz;
}

/* calc total offset of external files */
int64_t yezzey_calc_virtual_relation_size(std::shared_ptr<IOadv> adv,
                                          ssize_t segindx, ssize_t modcount,
                                          const std::string &storage_path) {
#if USE_WALG_BACKUPS
  try {
    auto ioh = YIO(adv, segindx, modcount, storage_path);
    int64_t sz = 0;
    auto buf = std::vector<char>(1 << 20);
    /* fix this */
    for (;;) {
      auto rc = buf.size();
      if (!ioh.io_read(buf.data(), &rc)) {
        break;
      }
      sz += rc;
    }

    ioh.io_close();
    return sz;
  } catch (...) {
    return -1;
  }
#else
  /* TODO: better import logic */
  return 0;
#endif
}
/*XXX: fix cleanup*/

XLogRecPtr yezzeyGetXStorageInsertLsn(void) {
  if (RecoveryInProgress())
    ereport(
        ERROR,
        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
         errmsg("recovery is in progress"),
         errhint("WAL control functions cannot be executed during recovery.")));

  return GetXLogWriteRecPtr();
}

std::string resolve_temp_relname(const char *tempname) {
  std::string name(tempname);
  if (strncmp(name.c_str(), "pg_temp_", 8) == 0) {
    int oid = atoi(name.substr(8, name.find('_', 8)).c_str());
    return std::string(get_rel_name(oid));
  }
  return tempname;
}
