#include "net/framing.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

using namespace ledger::net;

TEST(Framing, RoundTrip) {
  std::string buf;
  append_frame(buf, "hello");
  auto r = decode_frame(buf);
  ASSERT_EQ(r.status, DecodeStatus::kFrame);
  EXPECT_EQ(r.payload, "hello");
  EXPECT_EQ(r.consumed, buf.size());
}

TEST(Framing, HeaderIsBigEndianLength) {
  std::string buf;
  append_frame(buf, std::string(0x0102, 'x'));
  ASSERT_GE(buf.size(), 4u);
  EXPECT_EQ(buf[0], 0);
  EXPECT_EQ(buf[1], 0);
  EXPECT_EQ(buf[2], 0x01);
  EXPECT_EQ(buf[3], 0x02);
}

TEST(Framing, EmptyPayload) {
  std::string buf;
  append_frame(buf, "");
  auto r = decode_frame(buf);
  ASSERT_EQ(r.status, DecodeStatus::kFrame);
  EXPECT_EQ(r.payload, "");
  EXPECT_EQ(r.consumed, 4u);
}

TEST(Framing, PayloadMayContainZeroBytes) {
  std::string payload("a\0b\0c", 5);
  std::string buf;
  append_frame(buf, payload);
  auto r = decode_frame(buf);
  ASSERT_EQ(r.status, DecodeStatus::kFrame);
  EXPECT_EQ(r.payload, payload);
}

TEST(Framing, PartialHeaderNeedsMore) {
  std::string buf;
  append_frame(buf, "hello");
  for (size_t n = 0; n < 4; ++n) {
    EXPECT_EQ(decode_frame(std::string_view(buf).substr(0, n)).status, DecodeStatus::kNeedMore);
  }
}

TEST(Framing, PartialBodyNeedsMore) {
  std::string buf;
  append_frame(buf, "hello");
  EXPECT_EQ(decode_frame(std::string_view(buf).substr(0, buf.size() - 1)).status,
            DecodeStatus::kNeedMore);
}

TEST(Framing, ByteByByteArrivalYieldsFrameExactlyAtTheEnd) {
  // Simulates the worst case of TCP delivering one byte per read().
  std::string full;
  append_frame(full, "consensus");
  std::string buf;
  for (size_t i = 0; i < full.size(); ++i) {
    buf.push_back(full[i]);
    auto r = decode_frame(buf);
    if (i + 1 < full.size()) {
      EXPECT_EQ(r.status, DecodeStatus::kNeedMore) << "at byte " << i;
    } else {
      ASSERT_EQ(r.status, DecodeStatus::kFrame);
      EXPECT_EQ(r.payload, "consensus");
    }
  }
}

TEST(Framing, MultipleFramesInOneBuffer) {
  std::string buf;
  append_frame(buf, "one");
  append_frame(buf, "two");
  append_frame(buf, "three");
  buf += std::string("\0\0", 2);  // start of a fourth, incomplete frame

  std::string_view view(buf);
  for (const char* expected : {"one", "two", "three"}) {
    auto r = decode_frame(view);
    ASSERT_EQ(r.status, DecodeStatus::kFrame);
    EXPECT_EQ(r.payload, expected);
    view.remove_prefix(r.consumed);
  }
  EXPECT_EQ(decode_frame(view).status, DecodeStatus::kNeedMore);
}

TEST(Framing, OversizeHeaderRejectedWithoutWaitingForBody) {
  std::string buf("\x7f\xff\xff\xff", 4);  // claims ~2GB
  EXPECT_EQ(decode_frame(buf).status, DecodeStatus::kTooLarge);
}

TEST(Framing, MaxSizeAcceptedOneMoreRejected) {
  std::string at_limit;
  uint32_t len = kMaxFrameSize;
  at_limit = {static_cast<char>(len >> 24), static_cast<char>(len >> 16),
              static_cast<char>(len >> 8), static_cast<char>(len)};
  EXPECT_EQ(decode_frame(at_limit).status, DecodeStatus::kNeedMore);

  len = kMaxFrameSize + 1;
  std::string over = {static_cast<char>(len >> 24), static_cast<char>(len >> 16),
                      static_cast<char>(len >> 8), static_cast<char>(len)};
  EXPECT_EQ(decode_frame(over).status, DecodeStatus::kTooLarge);
}

TEST(Framing, EncodeRejectsOversizePayload) {
  std::string buf;
  EXPECT_THROW(append_frame(buf, std::string(kMaxFrameSize + 1, 'x')), std::length_error);
}
