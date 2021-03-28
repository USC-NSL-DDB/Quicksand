CALADAN_PATH=caladan

ROOT_PATH=caladan
include $(CALADAN_PATH)/build/shared.mk

INC += -Iinc -I$(CALADAN_PATH)/bindings/cc -I$(CALADAN_PATH)/ksched
override CXXFLAGS += -std=gnu++2a -fconcepts -Wno-subobject-linkage
override LDFLAGS += -lboost_system

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a

src = $(lib_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

lib_src = $(wildcard src/*.cpp) $(wildcard src/utils/*.cpp)
lib_obj = $(lib_src:.cpp=.o)

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

all: libservless.a bin/test_slab bin/test_closure bin/test_method bin/test_multi_objs \
bin/test_pass_obj bin/test_migrate

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

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) src/*.o src/utils/*.o test/*.o bin/* lib*.a
