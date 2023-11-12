#include <cstdlib>

extern "C" {
#include <base/log.h>
}

namespace nu {
bool caladan_shutting_down;
}

// Hook Caladan's init_shutdown() to properly set the flag.
void init_shutdown(int status) {
  nu::caladan_shutting_down = true;
  log_info("init: shutting down -> %s",
           status == EXIT_SUCCESS ? "SUCCESS" : "FAILURE");
  exit(status);
}
