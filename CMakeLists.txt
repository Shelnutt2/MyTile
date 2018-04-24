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

file(GLOB SOURCE_FILES mytile/*.cc)

MYSQL_ADD_PLUGIN(mytile ${SOURCE_FILES} STORAGE_ENGINE MODULE_ONLY RECOMPILE_FOR_EMBEDDED)
#target_include_directories(mytile PUBLIC "external/TileDB/tiledb/sm/cpp_api" "mytile")
target_link_libraries(mytile ${tiledb_static})
target_include_directories(mytile PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/external/TileDB/tiledb/examples" "mytile")
#include_directories("${CMAKE_CURRENT_BINARY_DIR}/external/TileDB/tiledb/examples" "mytile")
#GET_DIRECTORY_PROPERTY(output INCLUDE_DIRECTORIES)
#message(WARN "${output}")
