#include "gtest/gtest.h"

#include "scope_guard.h"

#include <memory>

TEST(ScopeGuard, RunsOnScopeExit) {
  bool fired = false;
  {
    auto guard = makeScopeGuard([&] { fired = true; });
  }
  EXPECT_TRUE(fired);
}

TEST(ScopeGuard, DismissPreventsCall) {
  bool fired = false;
  {
    auto guard = makeScopeGuard([&] { fired = true; });
    guard.dismiss();
  }
  EXPECT_FALSE(fired);
}

TEST(ScopeGuard, MoveTransfersOwnership) {
  bool fired = false;
  {
    auto guard1 = makeScopeGuard([&] { fired = true; });
    ScopeGuard<decltype(guard1)> guard2(std::move(guard1));
  }
  EXPECT_TRUE(fired);
}

TEST(ScopeGuard, MovedFromDoesNotFire) {
  int count = 0;
  {
    auto guard1 = makeScopeGuard([&] { count++; });
    auto guard2 = std::move(guard1);
  }
  EXPECT_EQ(count, 1);
}
