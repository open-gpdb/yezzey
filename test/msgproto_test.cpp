#include "gtest/gtest.h"
#include "msgproto.cpp"

/* Helper: read a big-endian uint64 out of the serialized buffer. */
static uint64_t readBE64(const std::vector<char> &buf, size_t off) {
  uint64_t v = 0;
  for (size_t i = 0; i < UINT64_SZ; ++i) {
    v = (v << 8) | (uint8_t)buf[off + i];
  }
  return v;
}

TEST(Unit, example) {
  auto mb = MsgBuilder().fieldUInt64().fieldString(10).endDescription();
  ASSERT_EQ(mb.get().size(), 8 + 8 + 11);
}

/*
 * endDescription must reserve room for every declared field and stamp the
 * total message length into the first 8 bytes (big-endian).
 */
TEST(MsgBuilder, EndDescriptionWritesTotalLength) {
  auto mb = MsgBuilder().fieldUInt64().endDescription();
  auto buf = mb.get();

  /* header (8) + one uint64 field (8) */
  ASSERT_EQ(buf.size(), MSG_HEADER_SIZE + UINT64_SZ);
  ASSERT_EQ(readBE64(buf, 0), (uint64_t)buf.size());
}

TEST(MsgBuilder, EmptyMessageIsHeaderOnly) {
  auto mb = MsgBuilder().endDescription();
  auto buf = mb.get();

  ASSERT_EQ(buf.size(), MSG_HEADER_SIZE);
  ASSERT_EQ(readBE64(buf, 0), (uint64_t)MSG_HEADER_SIZE);
}

/*
 * addUInt64 must serialize in big-endian order. Using a value with distinct
 * bytes above 0xFF guards against the historical `1 << 8` masking bug.
 */
TEST(MsgBuilder, AddUInt64IsBigEndian) {
  const uint64_t val = 0x0102030405060708ULL;
  auto mb = MsgBuilder().fieldUInt64().endDescription().addUInt64(val);
  auto buf = mb.get();

  ASSERT_EQ(buf.size(), MSG_HEADER_SIZE + UINT64_SZ);
  ASSERT_EQ(readBE64(buf, MSG_HEADER_SIZE), val);

  /* explicit byte-by-byte check of the big-endian layout */
  EXPECT_EQ((uint8_t)buf[MSG_HEADER_SIZE + 0], 0x01);
  EXPECT_EQ((uint8_t)buf[MSG_HEADER_SIZE + 1], 0x02);
  EXPECT_EQ((uint8_t)buf[MSG_HEADER_SIZE + 7], 0x08);
}

TEST(MsgBuilder, AddUInt64HandlesMaxValue) {
  const uint64_t val = UINT64_MAX;
  auto mb = MsgBuilder().fieldUInt64().endDescription().addUInt64(val);
  auto buf = mb.get();

  ASSERT_EQ(readBE64(buf, MSG_HEADER_SIZE), val);
}

/*
 * addString copies the raw characters; fieldString reserves len + CHAR_SZ,
 * so the trailing byte stays zero when the string is shorter than declared.
 */
TEST(MsgBuilder, AddStringCopiesContent) {
  const std::string payload = "hello";
  auto mb = MsgBuilder()
                .fieldString(payload.size())
                .endDescription()
                .addString(payload);
  auto buf = mb.get();

  ASSERT_EQ(buf.size(), MSG_HEADER_SIZE + payload.size() + CHAR_SZ);
  for (size_t i = 0; i < payload.size(); ++i) {
    EXPECT_EQ(buf[MSG_HEADER_SIZE + i], payload[i]);
  }
  /* reserved trailing byte remains zero */
  EXPECT_EQ(buf[MSG_HEADER_SIZE + payload.size()], 0);
}

TEST(MsgBuilder, EmptyStringLeavesReservedByteZero) {
  auto mb = MsgBuilder().fieldString(0).endDescription().addString("");
  auto buf = mb.get();

  ASSERT_EQ(buf.size(), MSG_HEADER_SIZE + CHAR_SZ);
  EXPECT_EQ(buf[MSG_HEADER_SIZE], 0);
}

/* addProto writes exactly PROTO_HEADER_SIZE bytes, padding missing args. */
TEST(MsgBuilder, AddProtoFullFourArgs) {
  auto mb = MsgBuilder().fieldProto().endDescription().addProto(1, 2, 3, 4);
  auto buf = mb.get();

  ASSERT_EQ(buf.size(), MSG_HEADER_SIZE + PROTO_HEADER_SIZE);
  EXPECT_EQ(buf[MSG_HEADER_SIZE + 0], 1);
  EXPECT_EQ(buf[MSG_HEADER_SIZE + 1], 2);
  EXPECT_EQ(buf[MSG_HEADER_SIZE + 2], 3);
  EXPECT_EQ(buf[MSG_HEADER_SIZE + 3], 4);
}

TEST(MsgBuilder, AddProtoPadsMissingArgsWithZero) {
  auto mb = MsgBuilder().fieldProto().endDescription().addProto(7);
  auto buf = mb.get();

  ASSERT_EQ(buf.size(), MSG_HEADER_SIZE + PROTO_HEADER_SIZE);
  EXPECT_EQ(buf[MSG_HEADER_SIZE + 0], 7);
  EXPECT_EQ(buf[MSG_HEADER_SIZE + 1], 0);
  EXPECT_EQ(buf[MSG_HEADER_SIZE + 2], 0);
  EXPECT_EQ(buf[MSG_HEADER_SIZE + 3], 0);
}

/* addBytes copies arbitrary binary data verbatim, including embedded nulls. */
TEST(MsgBuilder, AddBytesCopiesBinaryWithEmbeddedNulls) {
  const char raw[] = {0x10, 0x00, 0x20, 0x00, 0x30};
  const ssize_t len = sizeof(raw);

  auto mb = MsgBuilder().fieldBytes(len).endDescription().addBytes(raw, len);
  auto buf = mb.get();

  ASSERT_EQ(buf.size(), MSG_HEADER_SIZE + (size_t)len);
  for (ssize_t i = 0; i < len; ++i) {
    EXPECT_EQ(buf[MSG_HEADER_SIZE + i], raw[i]);
  }
}

/*
 * A realistic composite message: message type + uint64 + string, all appended
 * after endDescription. Verifies field offsets line up end-to-end.
 */
TEST(MsgBuilder, CompositeMessageLayout) {
  const std::string name = "seg0";
  const uint64_t offset = 0xDEADBEEFULL;

  auto mb = MsgBuilder()
                .fieldMessageType()
                .fieldUInt64()
                .fieldString(name.size())
                .endDescription()
                .addMessageType(MessageTypePut)
                .addUInt64(offset)
                .addString(name);
  auto buf = mb.get();

  size_t expected =
      MSG_HEADER_SIZE + UINT64_SZ + UINT64_SZ + name.size() + CHAR_SZ;
  ASSERT_EQ(buf.size(), expected);

  /* total length header */
  ASSERT_EQ(readBE64(buf, 0), (uint64_t)expected);

  /* message type occupies the low byte of an 8-byte slot */
  EXPECT_EQ(buf[MSG_HEADER_SIZE], MessageTypePut);

  /* uint64 field follows the message-type slot */
  EXPECT_EQ(readBE64(buf, MSG_HEADER_SIZE + UINT64_SZ), offset);

  /* string payload comes last */
  size_t str_off = MSG_HEADER_SIZE + UINT64_SZ + UINT64_SZ;
  for (size_t i = 0; i < name.size(); ++i) {
    EXPECT_EQ(buf[str_off + i], name[i]);
  }
}
