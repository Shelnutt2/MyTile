/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#pragma once

#include <table.h>
#include <handler.h>
#include <field.h>
#include <tiledb/tiledb>

namespace tile {
  int create_map(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info);
  tiledb::Attribute create_field_attribute(Field *field);
}