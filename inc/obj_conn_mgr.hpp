#pragma once

#include <functional>

#include "conn_mgr.hpp"
#include "defs.hpp"

namespace nu {

class RemObjConnManager : public ConnectionManager<RemObjID> {
public:
  RemObjConnManager();

private:
  static std::function<tcpconn_t *(RemObjID id)> creator_;
};

} // namespace nu

