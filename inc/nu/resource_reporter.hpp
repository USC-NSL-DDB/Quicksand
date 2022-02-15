extern "C" {
#include <runtime/report.h>
}

#include <thread.h>

namespace nu {

class ResourceReporter {
public:
  constexpr static uint32_t kReportResourceIntervalMs = 1;

  ResourceReporter();
  ~ResourceReporter();

private:
  bool done_;
  rt::Thread voluntary_reporter_;
  rt::Thread iokernel_forced_reporter_;

  void report_resource(bool forced);
};
} // namespace nu
