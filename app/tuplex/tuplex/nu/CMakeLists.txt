CMAKE_MINIMUM_REQUIRED(VERSION 3.9 FATAL_ERROR)

set(BIN_NAME nu_executor)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(ExternalProject)
ExternalProject_Get_Property(json source_dir)
set(json_INCLUDE_DIR ${source_dir}/include)
include_directories(${json_INCLUDE_DIR})

add_executable(${BIN_NAME} "src/main.cpp")

set_target_properties(${BIN_NAME} PROPERTIES ENABLE_EXPORTS 1)

target_link_libraries(${BIN_NAME} PRIVATE
        libio libutils libcore ${Boost_LIBRARIES} ${LibMagic_LIBRARIES})

add_dependencies(${BIN_NAME} json libio libutils libcore)
