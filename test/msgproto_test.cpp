#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "msgproto.cpp"

TEST(Unit, example) {
    auto mb = MsgBuilder().fieldUInt64().fieldString(10).endDescription();
    ASSERT_EQ(mb.get().size(), 8 + 8 + 11);
}