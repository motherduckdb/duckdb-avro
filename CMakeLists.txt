cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME avro)

find_path(AVRO_INCLUDE_DIR avro.h REQUIRED)
find_library(AVRO_LIBRARY avro REQUIRED)
find_library(JANSSON_LIBRARY jansson REQUIRED)
find_library(LZMA_LIBRARY lzma REQUIRED)
find_library(ZLIB_LIBRARY z REQUIRED)
find_library(SNAPPY_LIBRARY snappy REQUIRED)
set(ALL_AVRO_LIBRARIES
    ${AVRO_LIBRARY}
    ${JEMALLOC_LIBRARY}
    ${JANSSON_LIBRARY}
    ${LZMA_LIBRARY}
    ${ZLIB_LIBRARY}
    ${SNAPPY_LIBRARY}
    ${GMP_LIBRARY}
    ${MATH_LIBRARY})

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/avro_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

include_directories(${AVRO_INCLUDE_DIR})

target_link_libraries(${EXTENSION_NAME} ${ALL_AVRO_LIBRARIES})

target_link_libraries(${LOADABLE_EXTENSION_NAME} ${ALL_AVRO_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
