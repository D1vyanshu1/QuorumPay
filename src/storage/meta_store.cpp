#include "storage/meta_store.h"

#include <stdexcept>

#include "crypto/hash.h"
#include "storage/file_util.h"

namespace ledger::storage {

namespace {

constexpr std::string_view kMagic = "QPMETA01";
constexpr size_t kSize = 8 + 8 + 4 + 4;

}  // namespace

MetaStore::MetaStore(std::string dir) : path_(std::move(dir) + "/meta") {}

Meta MetaStore::load() const {
  auto data = read_file(path_);
  if (!data) return Meta{};
  if (data->size() != kSize || data->compare(0, kMagic.size(), kMagic) != 0) {
    throw std::runtime_error("storage: " + path_ + " is not a meta file");
  }
  const char* p = data->data();
  if (get_u32(p + 20) != crypto::crc32(std::string_view(p, 20))) {
    throw std::runtime_error("storage: " + path_ + " is corrupt (bad CRC)");
  }
  return Meta{get_u64(p + 8), get_u32(p + 16)};
}

void MetaStore::save(const Meta& m) {
  std::string out(kMagic);
  put_u64(out, m.term);
  put_u32(out, m.voted_for);
  put_u32(out, crypto::crc32(out));
  write_file_atomic(path_, out);
}

}  // namespace ledger::storage
