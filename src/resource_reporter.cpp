extern "C" {
#include <runtime/timer.h>
}

#include <runtime.h>
#include <sync.h>

#include "nu/commons.hpp"
#include "nu/ctrl_client.hpp"
#include "nu/resource_reporter.hpp"
#include "nu/runtime.hpp"

namespace nu {

ResourceReporter::ResourceReporter() : done_(false) {
  th_ = rt::Thread([&] {
    while (!rt::access_once(done_)) {
      timer_sleep_hp(kReportResourceIntervalMs * kOneMilliSecond);
      report_resource();
    }
  });
}

ResourceReporter::~ResourceReporter() {
  done_ = true;
  barrier();
  th_.Join();
}

void ResourceReporter::report_resource() {
  Resource resource;
  resource.cores =
      std::min(rt::RuntimeGlobalIdleCores(),
               rt::RuntimeMaxCores() -
                   (rt::RuntimeActiveCores() - rt::RuntimeSpinningCores())) +
      rt::RuntimeSpinningCores();
  resource.mem_mbs = rt::RuntimeFreeMemMbs();
  Runtime::controller_client->report_free_resource(resource);
}

}  // namespace nu
