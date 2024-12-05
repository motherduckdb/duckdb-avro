cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME avro)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in ./vcpkg.json and then used in cmake with find_package. Feel free to
# remove or replace with other dependencies. Note that it should also be removed
# from vcpkg.json to prevent needlessly installing it..
find_path(AVRO_INCLUDE_DIR avro.h)
find_library(AVRO_LIBRARY avro)
find_library(JANSSON_LIBRARY jansson)
find_library(LZMA_LIBRARY lzma)
find_library(ZLIB_LIBRARY z)
find_library(SNAPPY_LIBRARY snappy)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/avro_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

include_directories(${AVRO_INCLUDE_DIR})

# Link OpenSSL in both the static library as the loadable extension
# target_link_libraries(${EXTENSION_NAME} Avro)
# target_link_libraries(${LOADABLE_EXTENSION_NAME} Avro)
target_link_libraries(
  ${EXTENSION_NAME}
  ${AVRO_LIBRARY}
  ${JEMALLOC_LIBRARY}
  ${JANSSON_LIBRARY}
  ${LZMA_LIBRARY}
  ${ZLIB_LIBRARY}
  ${SNAPPY_LIBRARY}
  ${GMP_LIBRARY}
  ${MATH_LIBRARY})

target_link_libraries(
  ${LOADABLE_EXTENSION_NAME}
  ${AVRO_LIBRARY}
  ${JEMALLOC_LIBRARY}
  ${JANSSON_LIBRARY}
  ${LZMA_LIBRARY}
  ${ZLIB_LIBRARY}
  ${SNAPPY_LIBRARY}
  ${GMP_LIBRARY}
  ${MATH_LIBRARY})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
