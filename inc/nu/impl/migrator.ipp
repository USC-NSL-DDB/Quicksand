#include "nu/rpc_client_mgr.hpp"
#include "nu/utils/archive_pool.hpp"
#include "nu/utils/thread.hpp"

namespace nu {

template <typename RetT>
RPCReturnCode Migrator::load_thread_and_ret_val(ProcletHeader *dest_header,
                                                void *raw_dest_ret_val_ptr,
                                                uint64_t payload_len,
                                                uint8_t *payload) {
  auto optional_migration_guard =
      Runtime::attach_and_disable_migration(dest_header);
  if (unlikely(!optional_migration_guard)) {
    return kErrWrongClient;
  }
  Runtime::detach(*optional_migration_guard);

  size_t nu_state_size;
  thread_get_nu_state(thread_self(), &nu_state_size);
  auto *th = create_migrated_thread(payload);
  auto stack_range = Runtime::get_proclet_stack_range(th);
  auto stack_len = stack_range.end - stack_range.start;
  memcpy(reinterpret_cast<void *>(stack_range.start), payload + nu_state_size,
         stack_len);

  auto *dest_ret_val_ptr = reinterpret_cast<RetT *>(raw_dest_ret_val_ptr);
  auto *ia_sstream = Runtime::archive_pool->get_ia_sstream();
  auto &[ret_ss, ia] = *ia_sstream;
  ret_ss.span({reinterpret_cast<char *>(payload + nu_state_size + stack_len),
               payload_len - nu_state_size - stack_len});
  if constexpr (!std::is_same<RetT, void>::value) {
    ProcletSlabGuard g(&dest_header->slab);
    ia >> *dest_ret_val_ptr;
  }
  Runtime::archive_pool->put_ia_sstream(ia_sstream);

  thread_ready(th);
  return kOk;
}

template <typename RetT>
__attribute__((noinline)) void Migrator::snapshot_thread_and_ret_val(
    std::unique_ptr<std::byte[]> *req_buf, uint64_t *req_buf_len,
    RPCReturnBuffer &&ret_val_buf, ProcletID dest_id, RetT *dest_ret_val_ptr) {
  rt::Thread(
      [&, th = thread_self()]() mutable {
        thread_wait_until_parked(th);
        auto *dest_proclet_header = to_proclet_header(dest_id);
        thread_set_owner_proclet(th, dest_proclet_header, true);

        size_t nu_state_size;
        auto *nu_state = thread_get_nu_state(th, &nu_state_size);

        auto stack_range = Runtime::get_proclet_stack_range(th);
        auto stack_len = stack_range.end - stack_range.start;

        auto ret_val_span = ret_val_buf.get_buf();
        auto payload_len =
            nu_state_size + stack_len + ret_val_span.size_bytes();
        *req_buf_len = sizeof(RPCReqMigrateThreadAndRetVal) + payload_len;
        auto buf = std::make_unique_for_overwrite<std::byte[]>(*req_buf_len);
        auto *req = reinterpret_cast<RPCReqMigrateThreadAndRetVal *>(buf.get());
        std::construct_at(req);
        req->handler = load_thread_and_ret_val<RetT>;
        req->dest_proclet_header = dest_proclet_header;
        req->dest_ret_val_ptr = dest_ret_val_ptr;
        req->payload_len = payload_len;
        memcpy(req->payload, nu_state, nu_state_size);
        memcpy(req->payload + nu_state_size,
               reinterpret_cast<void *>(stack_range.start), stack_len);
        memcpy(req->payload + nu_state_size + stack_len, ret_val_span.data(),
               ret_val_span.size_bytes());
        // Only set req_buf after taking a snaphot of the stack so that only the
        // old thread will be able to observe its non-nullptr content.
        *req_buf = std::move(buf);
        thread_ready_head(th);
      },
      /* head = */ true)
      .Detach();

  rt::Preempt p;
  rt::PreemptGuardAndPark gp(&p);
}

template <typename RetT>
__attribute__((optimize("no-omit-frame-pointer"))) void
Migrator::migrate_thread_and_ret_val(
    RPCReturnBuffer &&ret_val_buf, ProcletID dest_id, RetT *dest_ret_val_ptr,
    std::move_only_function<void()> &&cleanup_fn) {
  assert(!thread_get_owner_proclet());

  std::unique_ptr<std::byte[]> req_buf;
  uint64_t req_buf_len;

  snapshot_thread_and_ret_val(&req_buf, &req_buf_len, std::move(ret_val_buf),
                              dest_id, dest_ret_val_ptr);

  if (req_buf) {
    if (cleanup_fn) {
      cleanup_fn();
    }
    auto *proclet_stack = reinterpret_cast<uint8_t *>(
        Runtime::get_proclet_stack_range(__self).end);
    Runtime::switch_to_runtime_stack();
    transmit_thread_and_ret_val(std::move(req_buf), req_buf_len, dest_id,
                                proclet_stack);
  }
}

}  // namespace nu
