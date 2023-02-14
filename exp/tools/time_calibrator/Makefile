NU_PATH=../../../
CALADAN_PATH=$(NU_PATH)/caladan

ROOT_PATH=$(CALADAN_PATH)
include $(CALADAN_PATH)/build/shared.mk

INC += -I$(CALADAN_PATH)/bindings/cc

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a

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
	$(LDXX) -o $@ $(main_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) main *.o
