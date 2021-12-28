#include "nu/rpc_client_mgr.hpp"
#include "nu/utils/archive_pool.hpp"
#include "nu/utils/thread.hpp"

namespace nu {

template <typename RetT>
RPCReturnCode Migrator::load_thread_and_ret_val(HeapHeader *dest_heap_header,
                                                void *raw_dest_ret_val_ptr,
                                                uint64_t payload_len,
                                                uint8_t *payload) {
retry:
  NonBlockingMigrationDisabledGuard guard(dest_heap_header);
  if (unlikely(!guard)) {
    if (unlikely(rt::access_once(dest_heap_header->status) >= kLoading)) {
      HeapManager::wait_until_present(dest_heap_header);
      goto retry;
    } else {
      return kErrWrongClient;
    }
  }

  size_t nu_state_size;
  thread_get_nu_state(thread_self(), &nu_state_size);
  auto *th = create_migrated_thread(payload);
  auto *nu_thread = reinterpret_cast<Thread *>(thread_get_nu_thread(th));

  auto stack_range = get_obj_stack_range(th);
  auto stack_len = stack_range.end - stack_range.start;

  // Only rewrite the pointer if the nu_thread locates at the dest heap.
  if (is_in_heap(nu_thread, dest_heap_header) ||
      is_in_stack(nu_thread, stack_range)) {
    BUG_ON(!nu_thread->th_);
    nu_thread->th_ = th;
  }
  memcpy(reinterpret_cast<void *>(stack_range.start), payload + nu_state_size,
         stack_len);

  auto *dest_ret_val_ptr = reinterpret_cast<RetT *>(raw_dest_ret_val_ptr);
  auto *ia_sstream = Runtime::archive_pool->get_ia_sstream();
  auto &[ret_ss, ia] = *ia_sstream;
  ret_ss.span({reinterpret_cast<char *>(payload + nu_state_size + stack_len),
               payload_len - nu_state_size - stack_len});
  if constexpr (!std::is_same<RetT, void>::value) {
    ObjSlabGuard g(&dest_heap_header->slab);
    ia >> *dest_ret_val_ptr;
  }
  Runtime::archive_pool->put_ia_sstream(ia_sstream);

  thread_ready(th);

  return kOk;
}

template <typename RetT>
void Migrator::migrate_thread_and_ret_val(RPCReturnBuffer &&ret_val_buf,
                                          RemObjID dest_id,
                                          RetT *dest_ret_val_ptr,
                                          folly::Function<void()> cleanup_fn) {
  rt::Thread(
      [&, th = thread_self(), ret_val_buf = std::move(ret_val_buf)] {
        thread_wait_until_parked(th);
        auto *dest_heap_header = to_heap_header(dest_id);
        thread_set_owner_heap(th, dest_heap_header);

        size_t nu_state_size;
        auto *nu_state = thread_get_nu_state(th, &nu_state_size);

        auto stack_range = get_obj_stack_range(th);
        auto stack_len = stack_range.end - stack_range.start;

        auto ret_val_span = ret_val_buf.get_buf();
        auto payload_len =
            nu_state_size + stack_len + ret_val_span.size_bytes();
        auto req_buf_len = sizeof(RPCReqMigrateThreadAndRetVal) + payload_len;
        auto req_buf = std::make_unique_for_overwrite<std::byte[]>(req_buf_len);
        auto *req =
            reinterpret_cast<RPCReqMigrateThreadAndRetVal *>(req_buf.get());
        std::construct_at(req);
        req->handler = load_thread_and_ret_val<RetT>;
        req->dest_heap_header = dest_heap_header;
        req->dest_ret_val_ptr = dest_ret_val_ptr;
        req->payload_len = payload_len;
        memcpy(req->payload, nu_state, nu_state_size);
        memcpy(req->payload + nu_state_size,
               reinterpret_cast<void *>(stack_range.start), stack_len);
        memcpy(req->payload + nu_state_size + stack_len, ret_val_span.data(),
               ret_val_span.size_bytes());

        if (cleanup_fn) {
          cleanup_fn();
        }

        auto req_span = std::span(req_buf.get(), req_buf_len);
        RPCReturnBuffer unused_buf;

      retry:
        auto *rpc_client = Runtime::rpc_client_mgr->get_by_rem_obj_id(dest_id);
        auto rc = rpc_client->CallPoll(req_span, &unused_buf);

        if (unlikely(rc == kErrWrongClient)) {
          Runtime::rpc_client_mgr->update_cache(dest_id, rpc_client);
          goto retry;
        }
      },
      /* head = */ true)
      .Detach();

  rt::Preempt p;
  rt::PreemptGuardAndPark gp(&p);
}

} // namespace nu
