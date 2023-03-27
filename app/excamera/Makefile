# Build alfalfa first!!!

NU_PATH=../..
CALADAN_PATH=$(NU_PATH)/caladan

ROOT_PATH=$(CALADAN_PATH)
include $(CALADAN_PATH)/build/shared.mk

ALFALA_PATH=alfalfa

ALFALFA_INC = -I$(ALFALA_PATH)/src/encoder -I$(ALFALA_PATH)/src/decoder \
							-I$(ALFALA_PATH)/src/input -I$(ALFALA_PATH)/src/util \
							-I$(ALFALA_PATH)/src/display
INC += $(ALFALFA_INC)

override LDFLAGS += -ljpeg -lx264

lib_alfalfa = $(ALFALA_PATH)/src/encoder/libalfalfaencoder.a \
							$(ALFALA_PATH)/src/decoder/libalfalfadecoder.a \
							$(ALFALA_PATH)/src/input/libalfalfainput.a \
							$(ALFALA_PATH)/src/util/libalfalfautil.a

src = $(wildcard src/*.cpp)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

baseline_src = src/xc_baseline.cpp
baseline_obj = $(baseline_src:.cpp=.o)

all: bin/xc_baseline

%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

bin/xc_baseline: $(baseline_obj)
	$(LDXX) -o $@ $(baseline_obj) $(lib_alfalfa) $(LDFLAGS)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) bin/xc_baseline src/*.o
