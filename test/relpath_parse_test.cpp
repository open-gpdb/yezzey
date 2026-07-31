#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "relpath_parse.h"

namespace {

struct Parsed {
  bool ok;
  uint32_t dbOid;
  uint32_t relfilenode;
  int64_t blkno;
};

Parsed run(const std::string &path) {
  Parsed p{};
  p.ok = parseRelnodePath(path, &p.dbOid, &p.relfilenode, &p.blkno);
  return p;
}

} // namespace

/*
 * A canonical base-tablespace data file path:
 *   base/<dboid>/<relfilenode>.<segno>
 * The routine must pick the three integers following the second-to-last '/'.
 */
TEST(RelpathParse, ParsesCanonicalBasePath) {
  auto p = run("base/16384/32769.3");
  ASSERT_TRUE(p.ok);
  EXPECT_EQ(p.dbOid, 16384u);
  EXPECT_EQ(p.relfilenode, 32769u);
  EXPECT_EQ(p.blkno, 3);
}

/* A deep absolute path: only the last two '/' segments are significant. */
TEST(RelpathParse, ParsesDeepAbsolutePath) {
  auto p = run("/data/primary/gpseg0/base/12345/67890.7");
  ASSERT_TRUE(p.ok);
  EXPECT_EQ(p.dbOid, 12345u);
  EXPECT_EQ(p.relfilenode, 67890u);
  EXPECT_EQ(p.blkno, 7);
}

/*
 * A non-default tablespace layout keeps the same trailing
 * <dboid>/<relfilenode>.<seg> shape.
 */
TEST(RelpathParse, ParsesTablespacePath) {
  auto p = run("pg_tblspc/16400/GPDB_7_301/16384/40960.1");
  ASSERT_TRUE(p.ok);
  EXPECT_EQ(p.dbOid, 16384u);
  EXPECT_EQ(p.relfilenode, 40960u);
  EXPECT_EQ(p.blkno, 1);
}

/* Segment file 0 has no ".<seg>" suffix; blkno must come out as 0. */
TEST(RelpathParse, HandlesZeroSegment) {
  auto p = run("base/16384/32769");
  ASSERT_TRUE(p.ok);
  EXPECT_EQ(p.dbOid, 16384u);
  EXPECT_EQ(p.relfilenode, 32769u);
  EXPECT_EQ(p.blkno, 0);
}

/*
 * A trailing slash after the segment file still leaves two separators before
 * the numeric fields, so parsing succeeds.
 */
TEST(RelpathParse, ParsesLargeOids) {
  auto p = run("base/4000000000/3999999999.42");
  ASSERT_TRUE(p.ok);
  EXPECT_EQ(p.dbOid, 4000000000u);
  EXPECT_EQ(p.relfilenode, 3999999999u);
  EXPECT_EQ(p.blkno, 42);
}
