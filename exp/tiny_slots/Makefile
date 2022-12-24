NU_PATH=../../
CALADAN_PATH=$(NU_PATH)/caladan

ROOT_PATH=$(CALADAN_PATH)
include $(CALADAN_PATH)/build/shared.mk

NCORES = $(shell nproc)

INC += -I$(NU_PATH)/inc -I$(CALADAN_PATH)/bindings/cc -I$(CALADAN_PATH)/ksched -I/usr/include/libnl3/

override CXXFLAGS += -Wno-subobject-linkage -Wno-array-bounds -DNCORES=$(NCORES) -DSLAB_TRANSFER_CACHE
override LDFLAGS += -L$(NU_PATH)/glibc/build/install/lib
override LDFLAGS += -lcrypto -lpthread -lboost_program_options -lnuma

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a
lib_nu = $(NU_PATH)/libnu.a

src = $(lib_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

caladan_src = caladan.cpp
caladan_obj = $(caladan_src:.cpp=.o)

nu_src = nu.cpp
nu_obj = $(nu_src:.cpp=.o)

all: caladan nu

%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

nu: $(nu_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(nu_obj) $(lib_obj) $(lib_nu) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
caladan: $(caladan_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(caladan_obj) $(lib_obj) $(lib_nu) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) nu caladan *.o
