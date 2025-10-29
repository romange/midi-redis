cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# Set targets in folders
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
project(DRAGONFLY C CXX)
set(CMAKE_CXX_STANDARD 17)

# We must define all the required variables from the root cmakefile, otherwise
# they just disappear.
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/helio/cmake" ${CMAKE_MODULE_PATH})
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)

include(third_party)
include(internal)

Message(STATUS "THIRD_PARTY_LIB_DIR ${THIRD_PARTY_LIB_DIR}")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(helio)


set(MIMALLOC_ROOT_DIR ${THIRD_PARTY_LIB_DIR}/mimalloc2)
set(MIMALLOC_INCLUDE_DIR ${MIMALLOC_ROOT_DIR}/include)
set(MIMALLOC_PATCH_DIR ${CMAKE_CURRENT_LIST_DIR}/../patches/mimalloc-v2.2.4)
set(MIMALLOC_C_FLAGS "-O3 -g -DMI_STAT=1 -DNDEBUG")
file(MAKE_DIRECTORY ${MIMALLOC_INCLUDE_DIR})

ExternalProject_Add(mimalloc2_project
  URL https://github.com/microsoft/mimalloc/archive/refs/tags/v2.2.4.tar.gz
  DOWNLOAD_DIR ${THIRD_PARTY_DIR}/mimalloc2
  SOURCE_DIR ${THIRD_PARTY_DIR}/mimalloc2
  # INSTALL_DIR ${MIMALLOC_ROOT_DIR}
  UPDATE_COMMAND ""

  BUILD_COMMAND make mimalloc-static

  INSTALL_COMMAND make install
  
  LOG_INSTALL ON
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON
  LOG_PATCH ON
  LOG_UPDATE ON
  DOWNLOAD_EXTRACT_TIMESTAMP YES

  CMAKE_GENERATOR "Unix Makefiles"

  # Add -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS=-O0 to debug, and set BUILD_BYPRODUCTS to
  # libmimalloc-debug.a

  BUILD_BYPRODUCTS ${MIMALLOC_ROOT_DIR}/lib/libmimalloc.a

  CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY:PATH=${MIMALLOC_ROOT_DIR}/lib
        -DCMAKE_LIBRARY_OUTPUT_DIRECTORY:PATH=${MIMALLOC_ROOT_DIR}/lib
        -DCMAKE_BUILD_TYPE:STRING=Release
        -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
        -DMI_INSTALL_TOPLEVEL=ON
        -DMI_OVERRIDE=OFF
        -DMI_NO_PADDING=ON
        -DMI_BUILD_TESTS=OFF
        -DMI_BUILD_SHARED=OFF
        -DMI_BUILD_OBJECT=OFF
        -DCMAKE_C_FLAGS=${MIMALLOC_C_FLAGS}
        -DCMAKE_INSTALL_PREFIX:PATH=${MIMALLOC_ROOT_DIR}
)

add_library(TRDP::mimalloc2 STATIC IMPORTED)
add_dependencies(TRDP::mimalloc2 mimalloc2_project)
set_target_properties(TRDP::mimalloc2 PROPERTIES IMPORTED_LOCATION ${MIMALLOC_ROOT_DIR}/lib/libmimalloc.a
                      INTERFACE_INCLUDE_DIRECTORIES ${MIMALLOC_ROOT_DIR}/include)

add_subdirectory(helio)
add_subdirectory(server)
add_subdirectory(string_set)