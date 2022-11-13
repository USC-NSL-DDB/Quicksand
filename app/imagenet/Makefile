NU_PATH=../..
CALADAN_PATH=$(NU_PATH)/caladan

ROOT_PATH=$(CALADAN_PATH)
include $(CALADAN_PATH)/build/shared.mk

NCORES = $(shell nproc)

INC += -I$(NU_PATH)/inc -I$(CALADAN_PATH)/bindings/cc -I$(CALADAN_PATH)/ksched -I/usr/include/libnl3/

override CXXFLAGS += -Wno-subobject-linkage -Wno-array-bounds -Wno-invalid-offsetof \
                     -DNCORES=$(NCORES) $(shell pkg-config --cflags opencv4) \
										 -Wno-deprecated-enum-enum-conversion   # warnings from opencv library
override LDFLAGS += -lcrypto -lpthread -lboost_program_options $(shell pkg-config --libs opencv4) \
										 -lnuma

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a
lib_nu = $(NU_PATH)/libnu.a

src = $(lib_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

image_kernel_src = image_kernel.cpp
image_kernel_obj = $(image_kernel_src:.cpp=.o)
baseline_src = baseline.cpp
baseline_obj = $(baseline_src:.cpp=.o)

all: baseline

%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

baseline: $(baseline_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj) $(image_kernel_obj)
	$(LDXX) -o $@ $(baseline_obj) $(image_kernel_obj) $(lib_nu) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) baseline baseline.o image_kernel.o
