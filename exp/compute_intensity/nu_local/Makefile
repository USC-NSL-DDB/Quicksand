CALADAN_PATH=../../../caladan

ROOT_PATH=$(CALADAN_PATH)
include $(CALADAN_PATH)/build/shared.mk

NCORES = $(shell nproc)

INC += -I../../../inc -I$(CALADAN_PATH)/bindings/cc -I$(CALADAN_PATH)/ksched -I/usr/include/libnl3/ \
-I$(CALADAN_PATH)/deps/folly/include/

override CXXFLAGS += -Wno-subobject-linkage -Wno-array-bounds -DNCORES=$(NCORES)
override LDFLAGS += -L../../../glibc/build/install/lib
override LDFLAGS += -static -static-libstdc++ -static-libgcc -lpthread

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a
lib_nu = ../../../libnu.a

src = $(lib_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

main_src = main.cpp
main_obj = $(main_src:.cpp=.o)

all: main

%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

main: $(main_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(main_obj) $(lib_nu) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) main main.o
