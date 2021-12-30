#include <string>

namespace nu {

struct MD5Val {
  unsigned char data[MD5_DIGEST_LENGTH];
};

MD5Val get_md5(std::string file_name);
}
