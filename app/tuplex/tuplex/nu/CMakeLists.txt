CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

set(BIN_NAME nu_executor)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(ExternalProject)
ExternalProject_Get_Property(json source_dir)
set(json_INCLUDE_DIR ${source_dir}/include)
include_directories(${json_INCLUDE_DIR})

set(NU_DIR ../../../../)
set(CALADAN_DIR ${NU_DIR}/caladan/)
set(FOLLY_DIR ${CALADAN_DIR}/deps/folly/)
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_EXE_LINKER_FLAGS "-T ${CALADAN_DIR}/base/base.ld \
                            -static -static-libstdc++ -static-libgcc \
                            -L${NU_DIR}/glibc/build/install/lib")

execute_process(COMMAND nproc OUTPUT_VARIABLE NCORES)
add_compile_definitions(NCORES=${NCORES})
add_compile_options(-DSPDLOG_FMT_EXTERNAL -DPCRE2_CODE_UNIT_WIDTH=8)

add_executable(${BIN_NAME} "src/main.cc")

#set_target_properties(${BIN_NAME} PROPERTIES ENABLE_EXPORTS 1)

target_include_directories(
    ${BIN_NAME} PRIVATE
    ${NU_DIR}/inc
    ${CALADAN_DIR}/inc
    ${CALADAN_DIR}/bindings/cc
    ${FOLLY_DIR}/include
)

target_link_directories(
    ${BIN_NAME} PRIVATE
    ${CALADAN_DIR}
    ${CALADAN_DIR}/bindings/cc
    ${CALADAN_DIR}/rdma-core/build/lib/statics/
    ${NU_DIR}
    )

# Use a different name as tuplex also has a libruntime.
find_library(LibCaladanRuntime libruntime.a PATHS ${CALADAN_DIR})

target_link_libraries(
    ${BIN_NAME} PRIVATE
    libio
    libutils
    libcore
    ${Boost_LIBRARIES}
    ${LibMagic_STATIC_LIBRARIES}
    # Nu
    nu
    # Caladan
    rt++
    ${LibCaladanRuntime}
    net
    base
    mlx5
    ibverbs
    nl-3
    nl-route-3
    pthread
    dl
    /usr/lib/x86_64-linux-gnu/libbz2.a
    /usr/lib/x86_64-linux-gnu/libutil.a
    /usr/lib/x86_64-linux-gnu/libexpat.a
)

add_dependencies(${BIN_NAME} json libio libutils libcore runtime)
