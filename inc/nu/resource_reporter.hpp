extern "C" {
#include <runtime/report.h>
}

#include <sync.h>
#include <thread.h>

namespace nu {

class ResourceReporter {
 public:
  constexpr static uint32_t kReportResourceIntervalMs = 1;

  ResourceReporter();
  ~ResourceReporter();
  std::vector<std::pair<NodeIP, Resource>> get_global_free_resources();

 private:
  bool done_;
  rt::Thread voluntary_reporter_;
  rt::Thread iokernel_forced_reporter_;
  std::vector<std::pair<NodeIP, Resource>> global_free_resources_;
  rt::Spin spin_;

  void report_resource(bool forced);
};

}  // namespace nu
