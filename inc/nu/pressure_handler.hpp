#include <runtime/pressure.h>

namespace nu {

using ResourcePressureInfo = struct resource_pressure_info;

class PressureHandler {
public:
  static void handle();
  static void mock_set_pressure(ResourcePressureInfo pressure);
};

} // namespace nu
