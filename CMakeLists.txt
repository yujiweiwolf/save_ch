cmake_minimum_required(VERSION 3.1)
project(simple CXX)

set(LIBX_ROOT "/home/yujw/Desktop/develop/vclibs/libx-1.0")
include_directories("${LIBX_ROOT}/include")
link_directories("${LIBX_ROOT}/lib")
set(LIBX_LIB x)

set(LIBZMQ_ROOT "/home/yujw/Desktop/develop/vclibs/libzmq-4.3.1")
include_directories("${LIBZMQ_ROOT}/include")
link_directories("${LIBZMQ_ROOT}/lib")
set(LIBZMQ_LIB zmq)

set(LIBICONV_ROOT "/home/yujw/Desktop/develop/vclibs/libiconv-1.15")
include_directories("${LIBICONV_ROOT}/include")
link_directories("${LIBICONV_ROOT}/lib")
set(LIBICONV_LIB charset iconv)

add_definitions(-DBOOST_SPIRIT_THREADSAFE)
set(BOOST_ROOT "/home/yujw/Desktop/develop/vclibs/boost-1.70.0")
find_package(Boost REQUIRED COMPONENTS program_options filesystem regex system thread serialization date_time log log_setup locale)
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
if(NOT Boost_FOUND)
    message("Can not find Boost")
endif()
message(STATUS "Found Boost IncPath ${Boost_INCLUDE_DIRS}")
message(STATUS "Found Boost LibPath ${Boost_LIBRARY_DIRS}")    
set(boost_libs dl pthread boost_program_options boost_system boost_filesystem boost_thread boost_regex boost_date_time boost_serialization boost_log_setup boost_log boost_locale)

add_definitions(-DBOOST_SPIRIT_THREADSAFE)
add_definitions(-DBOOST_ALL_DYN_LINK)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})


set(HOUSE_ROOT "/home/yujw/Desktop/clickhouse-cpp")
include_directories("${HOUSE_ROOT}")
include_directories("${HOUSE_ROOT}/types")

link_directories("${HOUSE_ROOT}/build/clickhouse")
set(LIBCK_LIB clickhouse-cpp-lib-static)

link_directories("${HOUSE_ROOT}/build/contrib/cityhash")
set(LIBCITY_LIB cityhash-lib)

link_directories("${HOUSE_ROOT}/build/contrib/lz4")
set(LIBLZ_LIB lz4-lib)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0  -ggdb -std=c++17 -fno-strict-aliasing")
#set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall  -ggdb -std=c++17 -fno-strict-aliasing")
#set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall -std=c++17 -fno-strict-aliasing")
set(CMAKE_BUILD_TYPE "Debug")


#include_directories("${PROJECT_SOURCE_DIR}")
#aux_source_directory(./ DIR_SRCS)
add_executable(simple main.cpp)

set(libs ${LIBCK_LIB}  ${LIBCITY_LIB} ${LIBLZ_LIB}  ${LIBX_LIB}  ${LIBZMQ_LIB} ${LIBICONV_LIB}  ${boost_libs})
target_link_libraries(simple ${libs})

message(${CMAKE_INSTALL_PREFIX})

