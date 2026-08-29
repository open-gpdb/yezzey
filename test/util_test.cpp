#include "gtest/gtest.h"

#include "yezzey_standalone.h"

#include <string>
#include <vector>

TEST(StorageUrlAddOptions, Basic) {
  auto result = storage_url_add_options("s3://bucket/path", "/etc/conf");
  EXPECT_EQ(result, "s3://bucket/path config=/etc/conf region=us-east-1");
}

TEST(StorageUrlAddOptions, EmptyPath) {
  auto result = storage_url_add_options("", "/etc/conf");
  EXPECT_EQ(result, " config=/etc/conf region=us-east-1");
}

TEST(StorageUrlAddOptions, EmptyConfig) {
  auto result = storage_url_add_options("s3://bucket", "");
  EXPECT_EQ(result, "s3://bucket config= region=us-east-1");
}

TEST(MakeYezzeyUrl, WithoutLsn) {
  auto result = make_yezzey_url("prefix", 42, 0);
  EXPECT_EQ(result, "prefix_DY_42");
}

TEST(MakeYezzeyUrl, WithLsn) {
  auto result = make_yezzey_url("prefix", 42, 12345);
  EXPECT_EQ(result, "prefix_DY_42_xlog_12345");
}

TEST(MakeYezzeyUrl, ZeroModcount) {
  auto result = make_yezzey_url("p", 0, 0);
  EXPECT_EQ(result, "p_DY_0");
}

TEST(MakeYezzeyUrl, NegativeModcount) {
  auto result = make_yezzey_url("p", -5, 0);
  EXPECT_EQ(result, "p_DY_-5");
}

TEST(ParseModcounts, SingleModcount) {
  auto result = parseModcountsInternal("p_", "p_42_D_aoseg");
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], 42);
}

TEST(ParseModcounts, MultipleModcounts) {
  auto result = parseModcountsInternal("p_", "p_1_D_2_D_3_D_4_aoseg");
  ASSERT_EQ(result.size(), 4u);
  EXPECT_EQ(result[0], 1);
  EXPECT_EQ(result[1], 2);
  EXPECT_EQ(result[2], 3);
  EXPECT_EQ(result[3], 4);
}

TEST(ParseModcounts, MissingPrefix) {
  auto result = parseModcountsInternal("xyz_", "p_1_D_2_aoseg");
  EXPECT_TRUE(result.empty());
}

TEST(ParseModcounts, MissingAoseg) {
  auto result = parseModcountsInternal("p_", "p_1_D_2");
  ASSERT_EQ(result.size(), 2u);
  EXPECT_EQ(result[0], 1);
  EXPECT_EQ(result[1], 2);
}

TEST(ParseModcounts, EmptyModcounts) {
  auto result = parseModcountsInternal("p_", "p__D__aoseg");
  EXPECT_TRUE(result.empty());
}
