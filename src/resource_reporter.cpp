extern "C" {
#include <runtime/timer.h>
}

#include <runtime.h>
#include <sync.h>

#include "nu/runtime.hpp"
#include "nu/commons.hpp"
#include "nu/ctrl_client.hpp"
#include "nu/resource_reporter.hpp"

namespace nu {

ResourceReporter::ResourceReporter() : done_(false) {
  voluntary_reporter_ = rt::Thread([&] {
    while (!rt::access_once(done_)) {
      timer_sleep(kReportResourceIntervalMs * kOneMilliSecond);
      report_resource(/* forced = */ false);
    }
  });

  iokernel_forced_reporter_ = rt::Thread([&] {
    set_resource_reporting_handler(thread_self());
    while (!rt::access_once(done_)) {
      rt::Preempt p;
      rt::PreemptGuardAndPark gp(&p);

      report_resource(/* forced =  */ true);
    }
    set_resource_reporting_handler(nullptr);
  });
}

std::vector<std::pair<NodeIP, Resource>>
ResourceReporter::get_global_free_resources() {
  rt::ScopedLock lock(&spin_);

  return global_free_resources_;
}

ResourceReporter::~ResourceReporter() {
  done_ = true;
  barrier();
  voluntary_reporter_.Join();
  iokernel_forced_reporter_.Join();
}

void ResourceReporter::report_resource(bool forced) {
  Resource resource;
  resource.cores =
      std::min(rt::RuntimeGlobalIdleCores(),
               rt::RuntimeMaxCores() -
                   (rt::RuntimeActiveCores() - rt::RuntimeSpinningCores())) +
      rt::RuntimeSpinningCores();
  resource.mem_mbs = rt::RuntimeFreeMemMbs();
  auto global_free_resources =
      get_runtime()->controller_client()->report_free_resource(resource);
  {
    rt::ScopedLock lock(&spin_);

    global_free_resources_ = std::move(global_free_resources);
  }
  finish_resource_reporting(forced);
}

}  // namespace nu
