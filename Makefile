CALADAN_PATH=caladan

ROOT_PATH=$(CALADAN_PATH)
include $(CALADAN_PATH)/build/shared.mk

NCORES = $(shell nproc)

INC += -Iinc -I$(CALADAN_PATH)/bindings/cc -I$(CALADAN_PATH)/ksched -I/usr/include/libnl3/ \
-I$(CALADAN_PATH)/deps/folly/include/

override CXXFLAGS += -Wno-subobject-linkage -Wno-array-bounds -DNCORES=$(NCORES)
override LDFLAGS += -Lglibc/build/install/lib
override LDFLAGS += -static -static-libstdc++ -static-libgcc -lpthread

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a

lib_src = $(wildcard src/*.cpp) $(wildcard src/utils/*.cpp)
lib_obj = $(lib_src:.cpp=.o)

src = $(lib_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

test_rem_obj_src = test/test_rem_obj.cpp
test_rem_obj_obj = $(test_rem_obj_src:.cpp=.o)
test_multi_objs_src = test/test_multi_objs.cpp
test_multi_objs_obj = $(test_multi_objs_src:.cpp=.o)
test_slab_src = test/test_slab.cpp
test_slab_obj = $(test_slab_src:.cpp=.o)
test_pass_obj_src = test/test_pass_obj.cpp
test_pass_obj_obj = $(test_pass_obj_src:.cpp=.o)
test_migrate_src = test/test_migrate.cpp
test_migrate_obj = $(test_migrate_src:.cpp=.o)
test_lock_src = test/test_lock.cpp
test_lock_obj = $(test_lock_src:.cpp=.o)
test_condvar_src = test/test_condvar.cpp
test_condvar_obj = $(test_condvar_src:.cpp=.o)
test_time_src = test/test_time.cpp
test_time_obj = $(test_time_src:.cpp=.o)
test_sync_hash_map_src = test/test_sync_hash_map.cpp
test_sync_hash_map_obj = $(test_sync_hash_map_src:.cpp=.o)
test_dis_hash_table_src = test/test_dis_hash_table.cpp
test_dis_hash_table_obj = $(test_dis_hash_table_src:.cpp=.o)
test_dis_mem_pool_src = test/test_dis_mem_pool.cpp
test_dis_mem_pool_obj = $(test_dis_mem_pool_src:.cpp=.o)
test_nested_rem_obj_src = test/test_nested_rem_obj.cpp
test_nested_rem_obj_obj = $(test_nested_rem_obj_src:.cpp=.o)
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

bench_rpc_tput_src = bench/bench_rpc_tput.cpp
bench_rpc_tput_obj = $(bench_rpc_tput_src:.cpp=.o)
bench_rem_obj_call_tput_src = bench/bench_rem_obj_call_tput.cpp
bench_rem_obj_call_tput_obj = $(bench_rem_obj_call_tput_src:.cpp=.o)
bench_rem_obj_call_lat_src = bench/bench_rem_obj_call_lat.cpp
bench_rem_obj_call_lat_obj = $(bench_rem_obj_call_lat_src:.cpp=.o)
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

all: libnu.a bin/test_slab bin/test_rem_obj bin/test_multi_objs \
bin/test_pass_obj bin/test_migrate bin/test_lock bin/test_condvar bin/test_time \
bin/bench_rpc_tput bin/bench_rem_obj_call_tput bin/bench_rem_obj_call_lat bin/bench_thread \
bin/bench_migrate bin/test_sync_hash_map bin/test_dis_hash_table \
bin/bench_hashtable_timeseries bin/bench_fake_migration bin/test_nested_rem_obj \
bin/test_dis_mem_pool bin/test_rem_raw_ptr bin/test_rem_unique_ptr \
bin/test_rem_shared_ptr bin/bench_fragmentation bin/test_perf bin/bench_real_mem_pressure \
bin/bench_real_cpu_pressure bin/test_cpu_load

libnu.a: $(lib_obj)
	$(AR) rcs $@ $^

%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

bin/test_rem_obj: $(test_rem_obj_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_rem_obj_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_multi_objs: $(test_multi_objs_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_multi_objs_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_slab: $(test_slab_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_slab_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_pass_obj: $(test_pass_obj_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_pass_obj_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_migrate: $(test_migrate_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_migrate_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_lock: $(test_lock_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_lock_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_condvar: $(test_condvar_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_condvar_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_time: $(test_time_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_time_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_sync_hash_map: $(test_sync_hash_map_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_sync_hash_map_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_dis_hash_table: $(test_dis_hash_table_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_dis_hash_table_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_dis_mem_pool: $(test_dis_mem_pool_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_dis_mem_pool_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/test_nested_rem_obj: $(test_nested_rem_obj_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_nested_rem_obj_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
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

bin/bench_rpc_tput: $(bench_rpc_tput_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_rpc_tput_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_rem_obj_call_tput: $(bench_rem_obj_call_tput_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_rem_obj_call_tput_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/bench_rem_obj_call_lat: $(bench_rem_obj_call_lat_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_rem_obj_call_lat_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
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

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) src/*.o src/utils/*.o test/*.o bench/*.o bin/* lib*.a
