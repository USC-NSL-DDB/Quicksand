include("../cmake/Findthrift.cmake")

find_package(nlohmann_json 3.5.0 REQUIRED)
find_package(Threads)

set(THRIFT_GEN_CPP_DIR ../gen-cpp)
set(NU_DIR ../../../../)
set(CALADAN_DIR ${NU_DIR}/caladan/)
set(FOLLY_DIR ${CALADAN_DIR}/deps/folly/)
set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_EXE_LINKER_FLAGS "-T ${CALADAN_DIR}/base/base.ld \
                            -static -static-libstdc++ -static-libgcc \
                            -L${NU_DIR}/../glibc/build/install/lib")

execute_process(COMMAND nproc OUTPUT_VARIABLE NCORES)
add_compile_definitions(NCORES=${NCORES})

add_executable(
    main
    main.cpp
    utils.cpp
    ThriftBackEndServer.cpp
    BackEndService.cpp
    ${THRIFT_GEN_CPP_DIR}/BackEndService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
)

target_include_directories(
    main PRIVATE
    ../third_party
    ${THRIFT_INCLUDE_DIR}
    ${NU_DIR}/inc
    ${CALADAN_DIR}/inc
    ${CALADAN_DIR}/bindings/cc
    ${FOLLY_DIR}/include
)

target_link_directories(
    main PRIVATE
    ${CALADAN_DIR}
    ${CALADAN_DIR}/bindings/cc
    ${CALADAN_DIR}/rdma-core/build/lib/statics/
    ${NU_DIR}
)

target_link_libraries(
    main
    ${THRIFT_STATIC_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    nlohmann_json::nlohmann_json
    crypto
    # Nu
    nu
    # Caladan
    rt++
    runtime
    net
    base
    mlx5
    ibverbs
    nl-3
    nl-route-3
    pthread
    dl
)
