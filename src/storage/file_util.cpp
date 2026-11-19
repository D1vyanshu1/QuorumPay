#include "storage/file_util.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace ledger::storage {

namespace {

[[noreturn]] void fail(const std::string& what) {
  throw std::runtime_error("storage: " + what + ": " + std::strerror(errno));
}

}  // namespace

void write_file_atomic(const std::string& path, std::string_view data) {
  const std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) fail("open " + tmp);
  try {
    write_fully(fd, data, tmp);
    fsync_fd(fd, tmp);
  } catch (...) {
    ::close(fd);
    throw;
  }
  ::close(fd);
  if (::rename(tmp.c_str(), path.c_str()) < 0) fail("rename " + tmp);
  fsync_dir(parent_dir(path));
}

std::optional<std::string> read_file(const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) return std::nullopt;
    fail("open " + path);
  }
  std::string out;
  char buf[65536];
  while (true) {
    ssize_t n = ::read(fd, buf, sizeof buf);
    if (n < 0) {
      if (errno == EINTR) continue;
      ::close(fd);
      fail("read " + path);
    }
    if (n == 0) break;
    out.append(buf, static_cast<size_t>(n));
  }
  ::close(fd);
  return out;
}

void make_dirs(const std::string& path) {
  if (path.empty()) return;
  for (size_t pos = 1; pos <= path.size(); ++pos) {
    if (pos != path.size() && path[pos] != '/') continue;
    const std::string prefix = path.substr(0, pos);
    if (::mkdir(prefix.c_str(), 0755) < 0 && errno != EEXIST) fail("mkdir " + prefix);
  }
}

void write_fully(int fd, std::string_view data, const std::string& what) {
  size_t off = 0;
  while (off < data.size()) {
    ssize_t n = ::write(fd, data.data() + off, data.size() - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      fail("write " + what);
    }
    off += static_cast<size_t>(n);
  }
}

void fsync_fd(int fd, const std::string& what) {
  if (::fsync(fd) < 0) fail("fsync " + what);
}

void fsync_dir(const std::string& dir) {
  int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) fail("open dir " + dir);
  int rc = ::fsync(fd);
  int saved = errno;
  ::close(fd);
  errno = saved;
  if (rc < 0) fail("fsync dir " + dir);
}

std::vector<std::string> list_dir(const std::string& dir) {
  std::vector<std::string> out;
  DIR* d = ::opendir(dir.c_str());
  if (d == nullptr) {
    if (errno == ENOENT) return out;
    fail("opendir " + dir);
  }
  while (dirent* e = ::readdir(d)) {
    std::string name = e->d_name;
    if (name != "." && name != "..") out.push_back(name);
  }
  ::closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}

std::string parent_dir(const std::string& path) {
  size_t slash = path.find_last_of('/');
  if (slash == std::string::npos) return ".";
  if (slash == 0) return "/";
  return path.substr(0, slash);
}

void put_u32(std::string& out, uint32_t v) {
  for (int shift = 24; shift >= 0; shift -= 8) out.push_back(static_cast<char>(v >> shift));
}

void put_u64(std::string& out, uint64_t v) {
  for (int shift = 56; shift >= 0; shift -= 8) out.push_back(static_cast<char>(v >> shift));
}

uint32_t get_u32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v = (v << 8) | static_cast<unsigned char>(p[i]);
  return v;
}

uint64_t get_u64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<unsigned char>(p[i]);
  return v;
}

}  // namespace ledger::storage
