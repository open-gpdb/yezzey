#pragma once

#include <cctype>
#include <cstdint>
#include <string>
#include <vector>

/*
 * Header-only pure utility functions, testable without PostgreSQL headers.
 * Defined here so they can be unit-tested standalone (cf. relpath_parse.h).
 * util.cpp / url.cpp include this header; the symbols are defined inline.
 */

#ifndef InvalidXLogRecPtr
#define InvalidXLogRecPtr ((uint64_t)0)
#endif

extern const char *baseYezzeyPath;

inline std::string storage_url_add_options(const std::string &s3path,
                                           const char *config_path) {
  auto ret = s3path;

  ret += " config=";
  ret += config_path;
  ret += " region=us-east-1";

  return ret;
}

inline std::vector<int64_t>
parseModcountsInternal(const std::string &prefix, std::string name) {
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
      break;
    }
    prev *= 10;
    prev += name[it] - '0';
  }

  return res;
}

inline std::string make_yezzey_url(const std::string &prefix, int64_t modcount,
                                   uint64_t current_recptr) {
  std::string rv = prefix + ("_DY_" + std::to_string(modcount));
  if (current_recptr != InvalidXLogRecPtr) {
    rv += "_xlog_" + std::to_string(current_recptr);
  }
  return rv;
}

inline std::string yezzey_block_namespace_path(int32_t segid) {
  return "/segments_005/seg" + std::to_string(segid) + baseYezzeyPath;
}
