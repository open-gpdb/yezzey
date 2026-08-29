#include "gtest/gtest.h"

#include "meta.h"

#include <memory>
#include <string>

TEST(MakeUnique, CreatesPrimitive) {
  auto p = make_unique<int>(42);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(*p, 42);
}

TEST(MakeUnique, CreatesString) {
  auto p = make_unique<std::string>("hello");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(*p, "hello");
  EXPECT_EQ(p->size(), 5u);
}

TEST(MakeUnique, CreatesWithMultipleArgs) {
  struct Pair {
    int a;
    int b;
    Pair(int a, int b) : a(a), b(b) {}
  };
  auto p = make_unique<Pair>(1, 2);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->a, 1);
  EXPECT_EQ(p->b, 2);
}

TEST(MakeUnique, ManagesOwnership) {
  auto p = make_unique<int>(7);
  auto raw = p.get();
  p.reset();
  EXPECT_EQ(p.get(), nullptr);
  (void)raw;
}
