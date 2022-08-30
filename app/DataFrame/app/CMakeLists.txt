# Build the app binary
add_executable(main main.cc)

# Include Nu
set(NU_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../../..)
target_include_directories(main PRIVATE ${NU_ROOT}/inc)

# Include Caladan
set(CALADAN_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../../../caladan)
target_include_directories(main PRIVATE ${CALADAN_ROOT}/inc)
target_include_directories(main PRIVATE ${CALADAN_ROOT}/bindings/cc)

# Include Folly
target_include_directories(main PRIVATE ${CALADAN_ROOT}/deps/folly/include)

# Define NCORES
include(ProcessorCount)
ProcessorCount(N)
add_definitions(-DNCORES=${N})

# Suppress Warning
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-subobject-linkage")

# Link Nu
target_link_directories(main PRIVATE ${NU_ROOT})
target_link_libraries(main nu)

# Link Caladan
target_link_directories(main PRIVATE ${CALADAN_ROOT}/bindings/cc)
target_link_directories(main PRIVATE ${CALADAN_ROOT})
target_link_libraries(main rt++ runtime net base)
set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -T${CALADAN_ROOT}/base/base.ld")

# Link OpenSSL
find_package(OpenSSL REQUIRED)
target_link_libraries(main OpenSSL::Crypto)

# Link Pthread
target_link_libraries(main pthread)

# Link RDMA-core
target_link_directories(main PRIVATE ${CALADAN_ROOT}/rdma-core/build/lib/statics)
target_link_directories(main PRIVATE ${CALADAN_ROOT}/rdma-core/build/util)
target_link_directories(main PRIVATE ${CALADAN_ROOT}/rdma-core/build/ccan)
target_link_libraries(main mlx5 ibverbs rdmacm rdma_util ccan)

# Link netlink
target_link_libraries(main nl-3 nl-route-3)

# Link libnuma
target_link_libraries(main numa)

# Link Boost
find_package(Boost QUIET REQUIRED COMPONENTS program_options)
target_link_libraries(main Boost::program_options)

# Link the DataFrame library to the app binary
target_link_libraries(main DataFrame)
