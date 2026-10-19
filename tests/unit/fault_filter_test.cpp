#include "net/fault_filter.h"

#include <gtest/gtest.h>

using ledger::net::FaultFilter;

TEST(FaultFilter, BlockAndUnblock) {
  FaultFilter f;
  EXPECT_FALSE(f.is_blocked(2));
  f.block(2);
  f.block(3);
  EXPECT_TRUE(f.is_blocked(2));
  EXPECT_TRUE(f.is_blocked(3));
  EXPECT_FALSE(f.is_blocked(4));
  f.unblock(2);
  EXPECT_FALSE(f.is_blocked(2));
  EXPECT_TRUE(f.is_blocked(3));
  f.unblock_all();
  EXPECT_TRUE(f.blocked().empty());
}

TEST(FaultFilter, BlockIsIdempotent) {
  FaultFilter f;
  f.block(2);
  f.block(2);
  EXPECT_EQ(f.blocked().size(), 1u);
  f.unblock(2);
  EXPECT_FALSE(f.is_blocked(2));
}
