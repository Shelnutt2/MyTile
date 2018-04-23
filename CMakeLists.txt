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

SET(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-fno-rtti")
SET(CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS} "-g")


file(GLOB SOURCE_FILES mytile/*.cc)

MYSQL_ADD_PLUGIN(mytile ${SOURCE_FILES} STORAGE_ENGINE MODULE_ONLY RECOMPILE_FOR_EMBEDDED)
include_directories("external/TileDB mytile")
target_link_libraries(mytile ${tiledb_static})