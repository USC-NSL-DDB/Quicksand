CALADAN_PATH=caladan

ROOT_PATH=$(CALADAN_PATH)
include $(CALADAN_PATH)/build/shared.mk

NCORES = $(shell nproc)

INC += -Iinc -I$(CALADAN_PATH)/bindings/cc -I$(CALADAN_PATH)/ksched -I/usr/include/libnl3/

override CXXFLAGS += -DNCORES=$(NCORES) -DSLAB_TRANSFER_CACHE -ftemplate-backtrace-limit=0
override LDFLAGS += -lcrypto -lpthread -lboost_program_options -lnuma -Wno-stringop-overread \
                    -Wno-alloc-size-larger-than

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a

lib_src = $(wildcard src/*.cpp) $(wildcard src/utils/*.cpp)
lib_src := $(filter-out $(wildcard src/*main.cpp),$(lib_src))
lib_obj = $(lib_src:.cpp=.o)

src = $(lib_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

test_proclet_src = test/test_proclet.cpp
test_proclet_obj = $(test_proclet_src:.cpp=.o)
test_slab_src = test/test_slab.cpp
test_slab_obj = $(test_slab_src:.cpp=.o)
test_pass_proclet_src = test/test_pass_proclet.cpp
test_pass_proclet_obj = $(test_pass_proclet_src:.cpp=.o)
test_migrate_src = test/test_migrate.cpp
test_migrate_obj = $(test_migrate_src:.cpp=.o)
test_continuous_migrate_src = test/test_continuous_migrate.cpp
test_continuous_migrate_obj = $(test_continuous_migrate_src:.cpp=.o)
test_lock_src = test/test_lock.cpp
test_lock_obj = $(test_lock_src:.cpp=.o)
test_condvar_src = test/test_condvar.cpp
test_condvar_obj = $(test_condvar_src:.cpp=.o)
test_time_src = test/test_time.cpp
test_time_obj = $(test_time_src:.cpp=.o)
test_sync_hash_map_src = test/test_sync_hash_map.cpp
test_sync_hash_map_obj = $(test_sync_hash_map_src:.cpp=.o)
test_sharded_set_src = test/test_sharded_set.cpp
test_sharded_set_obj = $(test_sharded_set_src:.cpp=.o)
test_sharded_unordered_set_src = test/test_sharded_unordered_set.cpp
test_sharded_unordered_set_obj = $(test_sharded_unordered_set_src:.cpp=.o)
test_sharded_map_src = test/test_sharded_map.cpp
test_sharded_map_obj = $(test_sharded_map_src:.cpp=.o)
test_sharded_unordered_map_src = test/test_sharded_unordered_map.cpp
test_sharded_unordered_map_obj = $(test_sharded_unordered_map_src:.cpp=.o)
test_sharded_vector_src = test/test_sharded_vector.cpp
test_sharded_vector_obj = $(test_sharded_vector_src:.cpp=.o)
test_dis_hash_table_src = test/test_dis_hash_table.cpp
test_dis_hash_table_obj = $(test_dis_hash_table_src:.cpp=.o)
test_dis_mem_pool_src = test/test_dis_mem_pool.cpp
test_dis_mem_pool_obj = $(test_dis_mem_pool_src:.cpp=.o)
test_nested_proclet_src = test/test_nested_proclet.cpp
test_nested_proclet_obj = $(test_nested_proclet_src:.cpp=.o)
test_rem_raw_ptr_src = test/test_rem_raw_ptr.cpp
test_rem_raw_ptr_obj = $(test_rem_raw_ptr_src:.cpp=.o)
test_rem_unique_ptr_src = test/test_rem_unique_ptr.cpp
test_rem_unique_ptr_obj = $(test_rem_unique_ptr_src:.cpp=.o)
test_rem_shared_ptr_src = test/test_rem_shared_ptr.cpp
test_rem_shared_ptr_obj = $(test_rem_shared_ptr_src:.cpp=.o)
test_fragmentation_src = test/test_fragmentation.cpp
test_fragmentation_obj = $(test_fragmentation_src:.cpp=.o)
test_perf_src = test/test_perf.cpp
test_perf_obj = $(test_perf_src:.cpp=.o)
test_cpu_load_src = test/test_cpu_load.cpp
test_cpu_load_obj = $(test_cpu_load_src:.cpp=.o)
test_tcp_poll_src = test/test_tcp_poll.cpp
test_tcp_poll_obj = $(test_tcp_poll_src:.cpp=.o)
test_thread_src = test/test_thread.cpp
test_thread_obj = $(test_thread_src:.cpp=.o)
test_fast_path_src = test/test_fast_path.cpp
test_fast_path_obj = $(test_fast_path_src:.cpp=.o)
test_slow_path_src = test/test_slow_path.cpp
test_slow_path_obj = $(test_slow_path_src:.cpp=.o)
test_max_num_proclets_src = test/test_max_num_proclets.cpp
test_max_num_proclets_obj = $(test_max_num_proclets_src:.cpp=.o)
test_sharded_partitioner_src = test/test_sharded_partitioner.cpp
test_sharded_partitioner_obj = $(test_sharded_partitioner_src:.cpp=.o)
test_cereal_src = test/test_cereal.cpp
test_cereal_obj = $(test_cereal_src:.cpp=.o)
test_iter_src = test/test_iter.cpp
test_iter_obj = $(test_iter_src:.cpp=.o)
test_sharded_sorter_src = test/test_sharded_sorter.cpp
test_sharded_sorter_obj = $(test_sharded_sorter_src:.cpp=.o)
test_sharded_stack_src = test/test_sharded_stack.cpp
test_sharded_stack_obj = $(test_sharded_stack_src:.cpp=.o)
test_sharded_queue_src = test/test_sharded_queue.cpp
test_sharded_queue_obj = $(test_sharded_queue_src:.cpp=.o)
test_dis_executor_src = test/test_dis_executor.cpp
test_dis_executor_obj = $(test_dis_executor_src:.cpp=.o)

bench_rpc_tput_src = bench/bench_rpc_tput.cpp
bench_rpc_tput_obj = $(bench_rpc_tput_src:.cpp=.o)
bench_proclet_call_tput_src = bench/bench_proclet_call_tput.cpp
bench_proclet_call_tput_obj = $(bench_proclet_call_tput_src:.cpp=.o)
bench_proclet_call_bw_src = bench/bench_proclet_call_bw.cpp
bench_proclet_call_bw_obj = $(bench_proclet_call_bw_src:.cpp=.o)
bench_proclet_call_lat_src = bench/bench_proclet_call_lat.cpp
bench_proclet_call_lat_obj = $(bench_proclet_call_lat_src:.cpp=.o)
bench_thread_src = bench/bench_thread.cpp
bench_thread_obj = $(bench_thread_src:.cpp=.o)
bench_migrate_src = bench/bench_migrate.cpp
bench_migrate_obj = $(bench_migrate_src:.cpp=.o)
bench_hashtable_timeseries_src = bench/bench_hashtable_timeseries.cpp
bench_hashtable_timeseries_obj = $(bench_hashtable_timeseries_src:.cpp=.o)
bench_dis_mem_pool_src = bench/bench_dis_mem_pool.cpp
bench_dis_mem_pool_obj = $(bench_dis_mem_pool_src:.cpp=.o)
bench_fake_migration_src = bench/bench_fake_migration.cpp
bench_fake_migration_obj = $(bench_fake_migration_src:.cpp=.o)
bench_fragmentation_src = bench/bench_fragmentation.cpp
bench_fragmentation_obj = $(bench_fragmentation_src:.cpp=.o)
bench_real_mem_pressure_src = bench/bench_real_mem_pressure.cpp
bench_real_mem_pressure_obj = $(bench_real_mem_pressure_src:.cpp=.o)
bench_real_cpu_pressure_src = bench/bench_real_cpu_pressure.cpp
bench_real_cpu_pressure_obj = $(bench_real_cpu_pressure_src:.cpp=.o)
bench_controller_src = bench/bench_controller.cpp
bench_controller_obj = $(bench_controller_src:.cpp=.o)
bench_sharded_partitioner_src = bench/bench_sharded_partitioner.cpp
bench_sharded_partitioner_obj = $(bench_sharded_partitioner_src:.cpp=.o)
bench_sharded_map_src = bench/bench_sharded_map.cpp
bench_sharded_map_obj = $(bench_sharded_map_src:.cpp=.o)
bench_sharded_unordered_map_src = bench/bench_sharded_unordered_map.cpp
bench_sharded_unordered_map_obj = $(bench_sharded_unordered_map_src:.cpp=.o)
bench_sharded_vector_src = bench/bench_sharded_vector.cpp
bench_sharded_vector_obj = $(bench_sharded_vector_src:.cpp=.o)
bench_cpu_overloaded_src = bench/bench_cpu_overloaded.cpp
bench_cpu_overloaded_obj = $(bench_cpu_overloaded_src:.cpp=.o)
bench_sharded_sorter_src = bench/bench_sharded_sorter.cpp
bench_sharded_sorter_obj = $(bench_sharded_sorter_src:.cpp=.o)
bench_sharded_queue_src = bench/bench_sharded_queue.cpp
bench_sharded_queue_obj = $(bench_sharded_queue_src:.cpp=.o)
bench_dis_executor_src = bench/bench_dis_executor.cpp
bench_dis_executor_obj = $(bench_dis_executor_src:.cpp=.o)
bench_sharded_set_src = bench/bench_sharded_set.cpp
bench_sharded_set_obj = $(bench_sharded_set_src:.cpp=.o)

ctrl_main_src = src/ctrl_main.cpp
ctrl_main_obj = $(ctrl_main_src:.cpp=.o)

all: libnu.a bin/test_slab bin/test_proclet bin/test_pass_proclet bin/test_migrate \
bin/test_lock bin/test_condvar bin/test_time bin/bench_rpc_tput \
bin/bench_proclet_call_tput bin/bench_proclet_call_lat bin/bench_thread \
bin/bench_migrate bin/test_sync_hash_map bin/test_dis_hash_table \
bin/bench_hashtable_timeseries bin/bench_fake_migration bin/test_nested_proclet \
bin/test_dis_mem_pool bin/test_rem_raw_ptr bin/test_rem_unique_ptr \
bin/test_rem_shared_ptr bin/bench_fragmentation bin/test_perf bin/bench_real_mem_pressure \
bin/bench_real_cpu_pressure bin/test_cpu_load bin/test_tcp_poll bin/test_thread \
bin/test_fast_path bin/test_slow_path bin/ctrl_main bin/test_max_num_proclets \
bin/test_sharded_partitioner bin/bench_controller bin/test_sharded_vector \
bin/bench_sharded_partitioner bin/test_cereal bin/bench_sharded_vector \
bin/bench_proclet_call_bw bin/test_sharded_set bin/test_sharded_map \
bin/test_sharded_unordered_map bin/bench_cpu_overloaded bin/test_iter \
bin/bench_sharded_unordered_map bin/bench_sharded_map bin/test_sharded_unordered_set \
bin/test_sharded_sorter bin/bench_sharded_sorter bin/test_sharded_stack \
bin/test_sharded_queue bin/test_dis_executor bin/test_continuous_migrate \
bin/bench_sharded_queue bin/bench_dis_executor bin/bench_sharded_set

%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

bin/test_proclet: $(test_proclet_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_proclet_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_slab: $(test_slab_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_slab_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_pass_proclet: $(test_pass_proclet_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_pass_proclet_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_migrate: $(test_migrate_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_migrate_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_continuous_migrate: $(test_continuous_migrate_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_continuous_migrate_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_lock: $(test_lock_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_lock_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_condvar: $(test_condvar_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_condvar_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_time: $(test_time_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_time_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sync_hash_map: $(test_sync_hash_map_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sync_hash_map_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sharded_set: $(test_sharded_set_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sharded_set_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sharded_unordered_set: $(test_sharded_unordered_set_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sharded_unordered_set_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sharded_map: $(test_sharded_map_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sharded_map_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sharded_unordered_map: $(test_sharded_unordered_map_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sharded_unordered_map_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sharded_vector: $(test_sharded_vector_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sharded_vector_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_dis_hash_table: $(test_dis_hash_table_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_dis_hash_table_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_dis_mem_pool: $(test_dis_mem_pool_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_dis_mem_pool_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_nested_proclet: $(test_nested_proclet_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_nested_proclet_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_rem_raw_ptr: $(test_rem_raw_ptr_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_rem_raw_ptr_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_rem_unique_ptr: $(test_rem_unique_ptr_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_rem_unique_ptr_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_rem_shared_ptr: $(test_rem_shared_ptr_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_rem_shared_ptr_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_perf: $(test_perf_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_perf_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_cpu_load: $(test_cpu_load_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_cpu_load_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_tcp_poll: $(test_tcp_poll_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_tcp_poll_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_thread: $(test_thread_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_thread_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_fast_path: $(test_fast_path_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_fast_path_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_slow_path: $(test_slow_path_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_slow_path_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_max_num_proclets: $(test_max_num_proclets_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_max_num_proclets_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sharded_partitioner: $(test_sharded_partitioner_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sharded_partitioner_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_cereal: $(test_cereal_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_cereal_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_iter: $(test_iter_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_iter_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sharded_sorter: $(test_sharded_sorter_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sharded_sorter_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sharded_stack: $(test_sharded_stack_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sharded_stack_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sharded_queue: $(test_sharded_queue_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sharded_queue_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_dis_executor: $(test_dis_executor_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_dis_executor_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)

bin/bench_rpc_tput: $(bench_rpc_tput_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_rpc_tput_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_proclet_call_tput: $(bench_proclet_call_tput_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_proclet_call_tput_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_proclet_call_bw: $(bench_proclet_call_bw_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_proclet_call_bw_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_proclet_call_lat: $(bench_proclet_call_lat_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_proclet_call_lat_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_thread: $(bench_thread_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_thread_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_migrate: $(bench_migrate_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_migrate_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_hashtable_timeseries: $(bench_hashtable_timeseries_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_hashtable_timeseries_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_fake_migration: $(bench_fake_migration_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_fake_migration_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_fragmentation: $(bench_fragmentation_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_fragmentation_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_real_mem_pressure: $(bench_real_mem_pressure_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_real_mem_pressure_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_real_cpu_pressure: $(bench_real_cpu_pressure_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_real_cpu_pressure_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_controller: $(bench_controller_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_controller_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_sharded_partitioner: $(bench_sharded_partitioner_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_sharded_partitioner_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_sharded_map: $(bench_sharded_map_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_sharded_map_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_sharded_unordered_map: $(bench_sharded_unordered_map_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_sharded_unordered_map_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_sharded_vector: $(bench_sharded_vector_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_sharded_vector_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_cpu_overloaded: $(bench_cpu_overloaded_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_cpu_overloaded_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_sharded_sorter: $(bench_sharded_sorter_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_sharded_sorter_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_sharded_queue: $(bench_sharded_queue_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_sharded_queue_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_dis_executor: $(bench_dis_executor_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_dis_executor_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_sharded_set: $(bench_sharded_set_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_sharded_set_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)

bin/ctrl_main: $(ctrl_main_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(ctrl_main_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
$(ctrl_main_obj): $(ctrl_main_src)
	$(CXX) $(CXXFLAGS) -c $< -o $@

libnu.a: $(lib_obj)
	$(AR) rcs $@ $^

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) src/*.o src/utils/*.o test/*.o bench/*.o bin/* lib*.a
