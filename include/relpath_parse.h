#pragma once

#include <cctype>
#include <cstdint>
#include <string>

/*
 *
 * A data file path looks like
 *
 *     <tablespace>/<dboid>/<relfilenode>.<segno>
 *
 */
inline bool parseRelnodePath(const std::string &fileName, uint32_t *dbOidOut,
                             uint32_t *relfilenodeOidOut, int64_t *blknoOut) {
  uint32_t dbOid = 0, relfilenodeOid = 0;
  int64_t blkno = 0;

  auto len = fileName.size();

  size_t start_off = len - 1;
  int slash_cntr = 0;
  while (start_off >= 0) {
    if (fileName[start_off] == '/') {
      ++slash_cntr;
      if (slash_cntr == 2)
        break;
    }
    --start_off;
  }

  if (slash_cntr != 2) {
    return false;
  }

  for (size_t it = start_off; it < len;) {
    if (!isdigit((unsigned char)fileName[it])) {
      ++it;
      continue;
    }
    if (dbOid && relfilenodeOid && blkno) {
      break; // seg num follows
    }
    if (dbOid == 0) {
      while (it < len && isdigit((unsigned char)fileName[it])) {
        if (dbOid > UINT32_MAX / 10) {
          return false;
        }
        dbOid *= 10;
        dbOid += fileName[it++] - '0';
      }
    } else if (relfilenodeOid == 0) {
      while (it < len && isdigit((unsigned char)fileName[it])) {
        if (relfilenodeOid > UINT32_MAX / 10) {
          return false;
        }
        relfilenodeOid *= 10;
        relfilenodeOid += fileName[it++] - '0';
      }
    } else if (blkno == 0) {
      while (it < len && isdigit((unsigned char)fileName[it])) {
        if (blkno > INT64_MAX / 10) {
          return false;
        }
        blkno *= 10;
        blkno += fileName[it++] - '0';
      }
    }
  }

  *dbOidOut = dbOid;
  *relfilenodeOidOut = relfilenodeOid;
  *blknoOut = blkno;
  return true;
}
