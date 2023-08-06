extern "C" {
#include <runtime/report.h>
}
#include <sync.h>
#include <thread.h>

#include "nu/commons.hpp"

namespace nu {

class ResourceReporter {
 public:
  ResourceReporter();
  ~ResourceReporter();
  float get_free_cores() const;
  float get_free_mem_mbs() const;
  float get_usable_mem_mbs() const;
  std::vector<std::pair<NodeIP, Resource>> get_global_free_resources();

 private:
  bool done_;
  rt::Thread th_;
  std::vector<std::pair<NodeIP, Resource>> global_free_resources_;
  rt::Spin spin_;

  void report_resource();
};

}  // namespace nu

#include "nu/impl/resource_reporter.ipp"
