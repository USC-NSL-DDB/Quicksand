extern "C" {
#include <base/stddef.h>
}

#include "nu/runtime.hpp"
#include "nu/utils/blocked_syncer.hpp"
#include "nu/utils/caladan.hpp"

namespace nu {

void BlockedSyncer::add(void *syncer, Type type) {
  Caladan::PreemptGuard pg;
  ProcletSlabGuard sg(&get_runtime()->get_current_proclet_header()->slab);

  pg.enable_for([&] { sync_map_.put(syncer, type); });
}

}  // namespace nu
