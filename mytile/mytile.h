/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#pragma once

//#include <table.h>
//#include <handler.h>
#include <field.h>
#include <tiledb/tiledb>
#include "buffer.h"

// Attribute for marking if row is deleted or not
#define MYTILE_DELETE_ATTRIBUTE "mytile_delete_attribute"
#define MYTILE_NULLS_ATTRIBUTE "mytile_nulls_attribute"

struct ha_table_option_struct
{
    char *array_uri;
    uint array_type;
    ulonglong capacity;
    uint cell_order;
    uint tile_order;
};

struct ha_field_option_struct
{
    bool dimension;
    char *lower_bound;
    char *upper_bound;
    char *tile_extent;
};

namespace tile {
    typedef struct ::ha_table_option_struct ha_table_option_struct;
    typedef struct ::ha_field_option_struct ha_field_option_struct;

    int create_array(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info, tiledb::Context ctx);

    tiledb::Attribute create_field_attribute(tiledb::Context &ctx, Field *field, tiledb::FilterList filterList);

    tiledb::Dimension create_field_dimension(tiledb::Context &ctx, Field *field);

    std::unique_ptr<tile::buffer> createBuffer(tiledb::Attribute attribute, size_t reserveSize);

    std::string getTileDBString(buffer *buffer, int64_t currentRowPosition);
}