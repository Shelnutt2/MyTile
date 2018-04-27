/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#pragma once

#include <table.h>
#include <handler.h>
#include <field.h>
#include <tiledb/tiledb>

// Attribute for marking if row is deleted or not
#define MYTILE_DELETE_ATTRIBUTE "mytile_delete_attribute"

namespace tile {
    int create_map(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info);

    tiledb::Attribute create_field_attribute(Field *field, tiledb::Context ctx);

    int cmpKeys(const uchar *key0, const uchar *key1, const KEY *key_info);
}