cmake_minimum_required(VERSION 3.5)

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

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(JNI_EXPORTS "${CMAKE_CURRENT_SOURCE_DIR}/jni.exports")
  set_property(TARGET ${TARGET} APPEND_STRING PROPERTY LINK_FLAGS
    " -Wl,--version-script=\"${JNI_EXPORTS}\" -Wl,--gc-sections")
  set_property(TARGET ${TARGET} APPEND PROPERTY LINK_DEPENDS "${JNI_EXPORTS}")

  # Keep the regular symbol table for native backtraces, but omit DWARF data.
  add_custom_command(TARGET ${TARGET} POST_BUILD
    COMMAND "${CMAKE_STRIP}" --strip-debug "$<TARGET_FILE:${TARGET}>"
    VERBATIM
  )
endif()

# Workaround for macOS to propagate a correct OpenSSL lib path to Reindexer.
if(APPLE)
  find_package(OpenSSL)
  if(OpenSSL_FOUND)
    if(NOT DEFINED OPENSSL_LIB_PATH)
      get_filename_component(OPENSSL_LIB_PATH ${OPENSSL_CRYPTO_LIBRARY} DIRECTORY)
    endif()
    set_target_properties(${TARGET} PROPERTIES
      BUILD_WITH_INSTALL_RPATH TRUE
      INSTALL_RPATH ${OPENSSL_LIB_PATH}
    )
  endif()
endif()
