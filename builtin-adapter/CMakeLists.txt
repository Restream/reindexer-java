cmake_minimum_required(VERSION 3.0)

project(builtin-adapter)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE "Release")

set(TARGET builtin-adapter)

find_package(JNI REQUIRED)
find_package(reindexer REQUIRED)

add_library(${TARGET} SHARED BuiltinAdapter.cpp)

include_directories(${JNI_INCLUDE_DIRS} ${REINDEXER_INCLUDE_DIRS})

target_link_libraries(${TARGET} reindexer_server_library reindexer_server_resources ${REINDEXER_LIBRARIES})
