extern "C" {
#include <base/assert.h>
#include <base/stddef.h>
}

#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <openssl/md5.h>

#include "nu/utils/md5.hpp"

namespace nu {

inline unsigned long get_size_by_fd(int fd) {
  struct stat statbuf;
  if (fstat(fd, &statbuf) < 0) {
    exit(-1);
  }
  return statbuf.st_size;
}

MD5Val get_md5(std::string file_name) {
  auto fd = open(file_name.c_str(), O_RDONLY);
  BUG_ON(fd < 0);
  auto file_size = get_size_by_fd(fd);
  auto file_buf = mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);
  BUG_ON(file_buf == MAP_FAILED);
  MD5Val md5_val;
  MD5(reinterpret_cast<unsigned char *>(file_buf), file_size, md5_val.data);
  BUG_ON(munmap(file_buf, file_size) == -1);
  return md5_val;
}

} // namespace nu
