# Makefile for netbench
ROOT_PATH=..
include $(ROOT_PATH)/build/shared.mk

rpc_src = rpc.cc test.cc
rpc_obj = $(rpc_src:.cc=.o)

librt_libs = $(ROOT_PATH)/bindings/cc/librt++.a
INC += -I$(ROOT_PATH)/bindings/cc
INC += -I$(ROOT_PATH)/deps/folly/include
INC += -I$(ROOT_PATH)/deps/boost/include

RUNTIME_LIBS := $(RUNTIME_LIBS) -lnuma

# must be first
all: rpc

rpc: $(rpc_obj) $(librt_libs) $(RUNTIME_DEPS)
	$(LDXX) -o $@ $(LDFLAGS) $(rpc_obj) \
	$(librt_libs) $(RUNTIME_LIBS)

# general build rules for all targets
src = $(rpc_src)
obj = $(src:.cc=.o)
dep = $(obj:.o=.d)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)   # include all dep files in the makefile
endif

# rule to generate a dep file by using the C preprocessor
# (see man cpp for details on the -MM and -MT options)
%.d: %.cc
	@$(CXX) $(CXXFLAGS) $(INC) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

.PHONY: clean
clean:
	rm -f $(obj) $(dep) rpc
