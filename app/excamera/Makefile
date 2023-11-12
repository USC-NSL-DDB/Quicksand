NU_PATH=../..
CALADAN_PATH=$(NU_PATH)/caladan

ROOT_PATH=$(CALADAN_PATH)
include $(CALADAN_PATH)/build/shared.mk

ALFALA_PATH=alfalfa

ALFALFA_INC = -I$(ALFALA_PATH)/src/encoder -I$(ALFALA_PATH)/src/decoder -I$(ALFALA_PATH)/src/input -I$(ALFALA_PATH)/src/util -I$(ALFALA_PATH)/src/display
INC += $(ALFALFA_INC) -I$(NU_PATH)/inc -I$(CALADAN_PATH)/bindings/cc -I$(CALADAN_PATH) -I/usr/include/libnl3/

override CXXFLAGS += -DNCORES=$(NCORES)
override LDFLAGS += -ljpeg -Wl,-Bstatic -lx264 -Wl,-Bdynamic -ldl -lboost_program_options -lcrypto -lpthread -lnuma

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a
lib_nu = $(NU_PATH)/libnu.a

lib_alfalfa = $(ALFALA_PATH)/src/encoder/libalfalfaencoder.a $(ALFALA_PATH)/src/decoder/libalfalfadecoder.a $(ALFALA_PATH)/src/input/libalfalfainput.a $(ALFALA_PATH)/src/util/libalfalfautil.a

src = $(wildcard src/*.cpp)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

qs_src       = src/xc_single_batch.cpp
qs_obj       = $(qs_src:.cpp=.o)
eval_src     = src/xc_multi_batch.cpp
eval_obj     = $(eval_src:.cpp=.o)

all: bin/xc_single_batch bin/xc_multi_batch

%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

bin/xc_single_batch: $(qs_obj)
	$(LDXX) -o $@ $(qs_obj) $(lib_alfalfa) $(lib_nu) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
bin/xc_multi_batch: $(eval_obj)
	$(LDXX) -o $@ $(eval_obj) $(lib_alfalfa) $(lib_nu) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) bin/xc_single_batch bin/xc_multi_batch src/*.o
