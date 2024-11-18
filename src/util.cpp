
/**
 * @file util.cpp
 *
 */

#include "util.h"

#include <map>
#include <string>
#include <vector>

#include "virtual_index.h"
#include "yproxy.h"

#include "io.h"
#include "io_adv.h"

#include "url.h"

#define DEFAULTTABLESPACE_OID 1663 /* FIXME */

const char *baseYezzeyPath = "/basebackups_005/yezzey/";

std::string storage_url_add_options(const std::string &s3path,
                                    const char *config_path) {
  auto ret = s3path;

  ret += " config=";
  ret += config_path;
  ret += " region=us-east-1";

  return ret;
}

relnodeCoord getRelnodeCoordinate(Oid spcNode, const std::string &fileName) {
  Oid dbOid = 0, relfilenodeOid = 0;
  int64_t blkno = 0;

  auto len = fileName.size();

  for (size_t it = 0; it < len;) {
    if (!isdigit(fileName[it])) {
      ++it;
      continue;
    }
    if (dbOid && relfilenodeOid && blkno) {
      break; // seg num follows
    }
    if (dbOid == 0) {
      while (it < len && isdigit(fileName[it])) {
        dbOid *= 10;
        dbOid += fileName[it++] - '0';
      }
    } else if (relfilenodeOid == 0) {
      while (it < len && isdigit(fileName[it])) {
        relfilenodeOid *= 10;
        relfilenodeOid += fileName[it++] - '0';
      }
    } else if (blkno == 0) {
      while (it < len && isdigit(fileName[it])) {
        blkno *= 10;
        blkno += fileName[it++] - '0';
      }
    }
  }

  return relnodeCoord(spcNode, dbOid, relfilenodeOid, blkno);
}

void getYezzeyExternalStoragePathByCoords(const char *nspname,
                                          const char *relname,
                                          Oid spcNode, Oid dbNode, Oid relNode,
                                          int32_t segblockno /* segment no*/,
                                          int32_t segid, char **dest) {

  /* FIXME: Support for non-default table space? */
  auto coords = relnodeCoord(spcNode, dbNode, relNode, segblockno);
  auto prefix = getYezzeyRelationUrl_internal(nspname, relname,
                                              coords, segid);
  auto path = prefix;

  *dest = (char *)malloc(sizeof(char) * path.size());
  strcpy(*dest, path.c_str());
  return;
}

/*
 * fileName is in form 'base=DEFAULTTABLESPACE_OID/<dboid>/<tableoid>.<seg>'
 */

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
    if (!isdigit(name[it])) {
      if (prev) {
        res.push_back(prev);
      }
      prev = 0;
      continue;
    }
    prev *= 10;
    prev += name[it] - '0';
  }

  return res;
}

std::string make_yezzey_url(const std::string &prefix, int64_t modcount,
                            XLogRecPtr current_recptr) {
  auto rv = prefix + ("_DY_" + std::to_string(modcount));
  if (current_recptr != InvalidXLogRecPtr) {
    rv += "_xlog_" + std::to_string(current_recptr);
  }
  return rv;
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

std::string resolve_temp_relname(char* tempname) {
  std::string name(tempname);
  if (strncmp(name.c_str(), "pg_temp_", 8) == 0)
  {
    int oid = atoi(name.substr(8, name.find('_', 8)).c_str());
    return std::string(get_rel_name(oid));
  }
  return tempname;
}
