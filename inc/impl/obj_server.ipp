#include <type_traits>
#include <utility>

#include "ctrl.hpp"
#include "heap_mgr.hpp"
#include "runtime.hpp"

namespace nu {

template <typename Cls, typename... As>
void ObjServer::construct_obj(cereal::BinaryInputArchive &ia,
                              cereal::BinaryOutputArchive &oa) {
  void *base;
  ia >> base;
  Runtime::heap_manager->allocate(base);
  auto *slab = Runtime::heap_manager->get_slab(base);
  auto obj_space = slab->allocate(sizeof(Cls));

  std::tuple<std::decay_t<As>...> args;
  std::apply([&](auto &&... args) { ((ia >> args), ...); }, args);
  std::apply([&](const As &... args) { new (obj_space) Cls(args...); }, args);
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::closure_handler(cereal::BinaryInputArchive &ia,
                                cereal::BinaryOutputArchive &oa) {
  RemObjID id;
  ia >> id;
  auto *heap_base = reinterpret_cast<void *>(id);

  FnPtr fn;
  ia >> fn;

  std::tuple<std::decay_t<S1s>...> states;
  std::apply([&](auto &&... states) { ((ia >> states), ...); }, states);

  auto *obj = Runtime::setup_thread_env<Cls>(heap_base);
  if constexpr (std::is_same<RetT, void>::value) {
    std::apply([&](const S1s &... states) { return fn(*obj, states...); },
               states);
  } else {
    auto ret = std::apply(
        [&](const S1s &... states) { return fn(*obj, states...); }, states);
    Runtime::switch_to_runtime_heap();
    oa << ret;
    Runtime::switch_to_obj_heap<Cls>(heap_base);
  }
  Runtime::clear_thread_env(heap_base);
}

template <typename Cls, typename RetT, typename MdPtr, typename... A1s>
void ObjServer::method_handler(cereal::BinaryInputArchive &ia,
                               cereal::BinaryOutputArchive &oa) {
  RemObjID id;
  ia >> id;
  auto *heap_base = reinterpret_cast<void *>(id);

  MdPtr md;
  ia >> md.raw;

  std::tuple<std::decay_t<A1s>...> args;
  std::apply([&](auto &&... args) { ((ia >> args), ...); }, args);

  auto *obj = Runtime::setup_thread_env<Cls>(heap_base);
  if constexpr (std::is_same<RetT, void>::value) {
    std::apply([&](const A1s &... args) { return (obj->*(md.ptr))(args...); },
               args);
  } else {
    auto ret = std::apply(
        [&](const A1s &... args) { return (obj->*(md.ptr))(args...); }, args);
    Runtime::switch_to_runtime_heap();
    oa << ret;
    Runtime::switch_to_obj_heap<Cls>(heap_base);
  }
  Runtime::clear_thread_env(heap_base);
}
} // namespace nu
