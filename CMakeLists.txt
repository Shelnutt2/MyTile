cmake_minimum_required(VERSION 3.3)

project(mytile)

if (CMAKE_VERSION VERSION_LESS "3.1")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14")
    endif ()
else ()
    set (CMAKE_CXX_STANDARD 14)
endif ()

add_subdirectory(external/TileDB)

# TileDB does not current support linking via cmake projects with their super build.
# We should not use link_directories but are forced to
link_directories("${CMAKE_CURRENT_BINARY_DIR}/external/TileDB/tiledb/tiledb/")

file(GLOB SOURCE_FILES mytile/*.cc)

# A the storage engine plugin
MYSQL_ADD_PLUGIN(mytile ${SOURCE_FILES} STORAGE_ENGINE MODULE_ONLY RECOMPILE_FOR_EMBEDDED)
# Set dependencies on superbuild
add_dependencies(mytile tiledb)

# Manually link shared library
# Static tiledb is throwing ha_mytile.so (errno: 2, undefined symbol: blosc_compress_ctx)
# Must use full library name else cmake things its the external target of `tiledb`
target_link_libraries(mytile "libtiledb${CMAKE_SHARED_LIBRARY_SUFFIX}")
target_include_directories(mytile PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/external/TileDB/tiledb/examples" "mytile")
