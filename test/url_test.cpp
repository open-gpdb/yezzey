#include "gtest/gtest.h"

#include "yezzey_standalone.h"

const char *baseYezzeyPath = "/basebackups_005/yezzey/";

TEST(YezzeyBlockNamespacePath, SegZero) {
  auto result = yezzey_block_namespace_path(0);
  EXPECT_EQ(result, "/segments_005/seg0/basebackups_005/yezzey/");
}

TEST(YezzeyBlockNamespacePath, SegOne) {
  auto result = yezzey_block_namespace_path(1);
  EXPECT_EQ(result, "/segments_005/seg1/basebackups_005/yezzey/");
}

TEST(YezzeyBlockNamespacePath, LargeSegId) {
  auto result = yezzey_block_namespace_path(999);
  EXPECT_EQ(result, "/segments_005/seg999/basebackups_005/yezzey/");
}

TEST(YezzeyBlockNamespacePath, NegativeSegId) {
  auto result = yezzey_block_namespace_path(-1);
  EXPECT_EQ(result, "/segments_005/seg-1/basebackups_005/yezzey/");
}
