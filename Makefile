CALADAN_PATH=caladan

ROOT_PATH=caladan
include $(CALADAN_PATH)/build/shared.mk

NCORES = $(shell nproc)

INC += -Iinc -I$(CALADAN_PATH)/bindings/cc -I$(CALADAN_PATH)/ksched
override CXXFLAGS += -Wno-subobject-linkage -DNCORES=$(NCORES)
override LDFLAGS += -lboost_system

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a

lib_src = $(wildcard src/*.cpp) $(wildcard src/utils/*.cpp)
lib_obj = $(lib_src:.cpp=.o)

src = $(lib_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

test_closure_src = test/test_closure.cpp
test_closure_obj = $(test_closure_src:.cpp=.o)
test_method_src = test/test_method.cpp
test_method_obj = $(test_method_src:.cpp=.o)
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

bench_rpc_lat_src = bench/bench_rpc_lat.cpp
bench_rpc_lat_obj = $(bench_rpc_lat_src:.cpp=.o)
bench_rpc_tput_src = bench/bench_rpc_tput.cpp
bench_rpc_tput_obj = $(bench_rpc_tput_src:.cpp=.o)
bench_tcp_tput_src = bench/bench_tcp_tput.cpp
bench_tcp_tput_obj = $(bench_tcp_tput_src:.cpp=.o)
bench_tcp_lat_src = bench/bench_tcp_lat.cpp
bench_tcp_lat_obj = $(bench_tcp_lat_src:.cpp=.o)

all: libservless.a bin/test_slab bin/test_closure bin/test_method bin/test_multi_objs \
bin/test_pass_obj bin/test_migrate bin/test_lock bin/test_condvar bin/test_time \
bin/bench_rpc_lat bin/bench_rpc_tput bin/bench_tcp_tput bin/bench_tcp_lat

libservless.a: $(lib_obj)
	$(AR) rcs $@ $^

%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

bin/test_closure: $(test_closure_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_closure_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/test_method: $(test_method_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_method_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/test_multi_objs: $(test_multi_objs_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_multi_objs_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/test_slab: $(test_slab_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_slab_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/test_pass_obj: $(test_pass_obj_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_pass_obj_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/test_migrate: $(test_migrate_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_migrate_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/test_lock: $(test_lock_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_lock_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/test_condvar: $(test_condvar_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_condvar_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/test_time: $(test_time_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(test_time_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt

bin/bench_rpc_lat: $(bench_rpc_lat_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_rpc_lat_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/bench_rpc_tput: $(bench_rpc_tput_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_rpc_tput_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/bench_tcp_tput: $(bench_tcp_tput_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_tcp_tput_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt
bin/bench_tcp_lat: $(bench_tcp_lat_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(bench_tcp_lat_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS) -lrt

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) src/*.o src/utils/*.o test/*.o bench/*.o bin/* lib*.a
