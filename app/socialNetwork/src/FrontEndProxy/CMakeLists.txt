set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_EXE_LINKER_FLAGS "-T ${CALADAN_DIR}/base/base.ld \
                            -static -static-libstdc++ -static-libgcc \
                            -L${NU_DIR}/../../glibc/build/install/lib")

add_executable(
    FrontEndProxy
    FrontEndProxy.cpp
    ${THRIFT_GEN_CPP_DIR}/FrontEndProxy.cpp    
    ${THRIFT_GEN_CPP_DIR}/HomeTimelineService.cpp
    ${THRIFT_GEN_CPP_DIR}/ComposePostService.cpp
    ${THRIFT_GEN_CPP_DIR}/SocialGraphService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserTimelineService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
)

target_include_directories(
    FrontEndProxy PRIVATE
    ${NU_DIR}/inc
    ${CALADAN_DIR}/inc
    ${CALADAN_DIR}/bindings/cc
    ${FOLLY_DIR}/include
)

target_link_libraries(
    FrontEndProxy
    nlohmann_json::nlohmann_json
    ${THRIFT_STATIC_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    ssl
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

install(TARGETS FrontEndProxy DESTINATION ./)
