#include "nu/rpc_client_mgr.hpp"
#include "nu/utils/archive_pool.hpp"
#include "nu/utils/thread.hpp"

namespace nu {

template <typename RetT>
void Migrator::load_callee_thread(void *raw_caller_ptr, uint64_t payload_len,
                                  uint8_t *payload) {
  size_t nu_state_size;
  thread_get_nu_state(thread_self(), &nu_state_size);
  auto *th = create_migrated_thread(payload, /* returned_callee = */ true);
  auto *nu_thread = reinterpret_cast<Thread *>(thread_get_nu_thread(th));
  if (nu_thread) {
    BUG_ON(!nu_thread->th_);
    nu_thread->th_ = th;
  }

  auto stack_range = get_obj_stack_range(th);
  auto stack_len = stack_range.end - stack_range.start;
  memcpy(reinterpret_cast<void *>(stack_range.start), payload + nu_state_size,
         stack_len);

  auto *caller_ptr = reinterpret_cast<RetT *>(raw_caller_ptr);
  auto *ia_sstream = Runtime::archive_pool->get_ia_sstream();
  auto &[ret_ss, ia] = *ia_sstream;
  ret_ss.span(
      {reinterpret_cast<char *>(payload + nu_state_size + stack_len),
       payload_len - nu_state_size - stack_len});
  if constexpr (!std::is_same<RetT, void>::value) {
    ia >> *caller_ptr;
  }
  Runtime::archive_pool->put_ia_sstream(ia_sstream);

  thread_ready(th);
}

template <typename RetT>
void Migrator::migrate_callee_thread_back_to_caller(RemObjID caller_id,
                                                    RemObjID callee_id,
                                                    RetT *caller_ptr,
                                                    RetT *callee_ptr) {
  assert_preempt_disabled();
  decltype(Runtime::archive_pool->get_oa_sstream()) oa_sstream;

  oa_sstream = Runtime::archive_pool->get_oa_sstream();
  if constexpr (!std::is_same<RetT, void>::value) {
    oa_sstream->oa << *callee_ptr;
  }

  rt::Thread(
      [oa_sstream, caller_ptr, caller_id, callee_id, th = thread_self()] {
        size_t nu_state_size;
        auto *nu_state = thread_get_nu_state(th, &nu_state_size);

        auto stack_range = get_obj_stack_range(th);
        auto stack_len = stack_range.end - stack_range.start;

        auto ss_view = oa_sstream->ss.view();
        auto ret_val = reinterpret_cast<const std::byte *>(ss_view.data());
        auto ret_val_len = oa_sstream->ss.tellp();
        assert((!ret_val_len == std::is_same<RetT, void>::value));

        auto payload_len = nu_state_size + stack_len + ret_val_len;
        auto req_buf_len = sizeof(RPCReqMigrateCalleeBack) + payload_len;
        auto req_buf = std::make_unique_for_overwrite<std::byte[]>(req_buf_len);
        auto *req = reinterpret_cast<RPCReqMigrateCalleeBack *>(req_buf.get());
        std::construct_at(req);
        req->handler = load_callee_thread<RetT>;
        req->caller_ptr = caller_ptr;
        req->payload_len = payload_len;
        memcpy(req->payload, nu_state, nu_state_size);
        memcpy(req->payload + nu_state_size,
               reinterpret_cast<void *>(stack_range.start), stack_len);
        memcpy(req->payload + nu_state_size + stack_len, ret_val, ret_val_len);

        Runtime::archive_pool->put_oa_sstream(oa_sstream);
        to_heap_header(callee_id)->migrated_wg.Done();

        auto req_span = std::span(req_buf.get(), req_buf_len);
        auto *rpc_client =
            Runtime::rpc_client_mgr->get_by_rem_obj_id(caller_id).second;
        RPCReturnBuffer return_buf;
        {
          RuntimeHeapGuard guard;
          BUG_ON(rpc_client->Call(req_span, &return_buf) != kOk);
          // TODO: handle the case that caller gets migrated between
          // rpc_client_mgr->get_by_rem_obj_id() and rpc_client->Call().
        }
      },
      /* head = */ true)
      .Detach();

  thread_park_and_preempt_enable();
}

} // namespace nu
