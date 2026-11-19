#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace ledger::storage {

// Small POSIX file helpers for crash-safe storage. They all throw
// std::runtime_error (with the errno text) on failure: a node that can't
// write its state must stop rather than carry on with promises it can't keep.

// Writes `data` to `path` so that after a crash the file holds either the
// old contents or the new ones, never a mix:
//   write path.tmp -> fsync(path.tmp) -> rename(path.tmp, path) -> fsync(dir)
// rename() is atomic, and fsyncing the directory makes the rename itself
// durable (the directory entry lives in the directory, not in the file).
void write_file_atomic(const std::string& path, std::string_view data);

// The whole file, or nullopt if it doesn't exist.
std::optional<std::string> read_file(const std::string& path);

// mkdir -p.
void make_dirs(const std::string& path);

// write() until all of `data` is written (retrying short writes and EINTR).
void write_fully(int fd, std::string_view data, const std::string& what);

void fsync_fd(int fd, const std::string& what);
void fsync_dir(const std::string& dir);

// Names (not paths) of the entries in `dir`, sorted; empty if it doesn't exist.
std::vector<std::string> list_dir(const std::string& dir);

std::string parent_dir(const std::string& path);

// Big-endian fixed-width integers for the on-disk formats. (ledger/encoding.h
// has the same for signing bytes, but storage/ may not depend on ledger/.)
void put_u32(std::string& out, uint32_t v);
void put_u64(std::string& out, uint64_t v);
uint32_t get_u32(const char* p);
uint64_t get_u64(const char* p);

}  // namespace ledger::storage
