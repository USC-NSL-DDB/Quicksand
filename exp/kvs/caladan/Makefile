NU_PATH=../../../
CALADAN_PATH=$(NU_PATH)/caladan

ROOT_PATH=$(CALADAN_PATH)
include $(CALADAN_PATH)/build/shared.mk


INC += -I$(NU_PATH)/inc -I$(CALADAN_PATH)/bindings/cc -I$(CALADAN_PATH) -I/usr/include/libnl3/

override CXXFLAGS += -Wno-subobject-linkage -Wno-array-bounds -DNCORES=$(NCORES)
override LDFLAGS += -ldl -lnuma -lpthread

librt_libs = $(CALADAN_PATH)/bindings/cc/librt++.a
lib_nu = $(NU_PATH)/libnu.a

src = $(lib_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

client_src = client.cpp
client_obj = $(client_src:.cpp=.o)

server_src = server.cpp
server_obj = $(server_src:.cpp=.o)

all: client server

%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

server: $(server_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(server_obj) $(lib_obj) $(lib_nu) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)
client: $(client_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(client_obj) $(lib_obj) $(lib_nu) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)
endif

.PHONY: clean
clean:
	rm -f $(dep) server client *.o
