#include <thread.h>

namespace nu {

class ResourceReporter {
 public:
  constexpr static uint32_t kReportResourceIntervalMs = 1;

  ResourceReporter();
  ~ResourceReporter();

 private:
  bool done_;
  rt::Thread th_;

  void report_resource();
};
}  // namespace nu
