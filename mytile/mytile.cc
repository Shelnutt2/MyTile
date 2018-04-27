/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include <log.h>
#include "mytile.h"

int tile::create_map(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) {
  DBUG_ENTER("tile::create_map");
  int rc = 0;
  // Create TileDB context
  tiledb::Context ctx;


  // Create map schema
  tiledb::MapSchema schema(ctx);

  schema.add_attribute(tiledb::Attribute::create<bool>(ctx,  MYTILE_DELETE_ATTRIBUTE, {TILEDB_BLOSC_LZ, -1}));

  // Create attributes
  for (Field **field = table_arg->field; *field; field++) {
    schema.add_attribute(create_field_attribute(*field, ctx));
  }

  // Check array schema
  try {
    schema.check();
  } catch (tiledb::TileDBError &e) {
    sql_print_error("Error in building schema %s", e.what());
    DBUG_RETURN(-10);
  }

  // Create the map on storage
  tiledb::Map::create(name, schema);
  DBUG_RETURN(rc);
}

tiledb::Attribute tile::create_field_attribute(Field *field, tiledb::Context ctx) {
  switch (field->type()) {

    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      return tiledb::Attribute::create<double>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});

    case MYSQL_TYPE_FLOAT:
      return tiledb::Attribute::create<float>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});

    case MYSQL_TYPE_TINY:
      if (((Field_num *) field)->unsigned_flag)
        return tiledb::Attribute::create<uint8_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});
      else
        return tiledb::Attribute::create<int8_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});

    case MYSQL_TYPE_SHORT:
      if (((Field_num *) field)->unsigned_flag) {
        return tiledb::Attribute::create<uint16_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});
      } else {
        return tiledb::Attribute::create<int16_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});
      }
    case MYSQL_TYPE_YEAR:
      return tiledb::Attribute::create<uint16_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});

    case MYSQL_TYPE_INT24:
      if (((Field_num *) field)->unsigned_flag)
        return tiledb::Attribute::create<uint32_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});
      else
        return tiledb::Attribute::create<int32_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});

    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      if (((Field_num *) field)->unsigned_flag)
        return tiledb::Attribute::create<uint64_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});
      else
        return tiledb::Attribute::create<int64_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});

    case MYSQL_TYPE_NULL:
      return tiledb::Attribute::create<int64_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});

    case MYSQL_TYPE_BIT:
      return tiledb::Attribute::create<uint8_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});


    case MYSQL_TYPE_VARCHAR :
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_SET:
      return tiledb::Attribute::create<std::string>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});

    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_ENUM:
      return tiledb::Attribute::create<std::string>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});

    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_NEWDATE:
      return tiledb::Attribute::create<int64_t>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});
  }
  sql_print_error("Unknown mysql data type in creating tiledb field attribute");
  return tiledb::Attribute::create<std::string>(ctx, field->field_name, {TILEDB_BLOSC_LZ, -1});
}

/**
 *
 * @param key0
 * @param key1
 * @param key_info
 * @return 0 is keys are equal, -1 if key0 < key1 and 1 if key0 > key1
 */
int tile::cmpKeys(const uchar *key0, const uchar *key1, const KEY *key_info) {
  int res = 0;
  for (size_t i = 0; i < key_info->user_defined_key_parts && !res; i++) {
    const auto &keyPart = key_info->key_part[i];
    const int off = (keyPart.null_bit ? 1 : 0); // to step over null-byte

    if (keyPart.null_bit) // does the key part have a null-byte?
      if (*key0 != *key1) {
        return (int) ((*key1) - (*key0));
      }
    res = keyPart.field->key_cmp(key0 + off, key1 + off); // compare key parts
    key0 += keyPart.store_length; // go to next key part
    key1 += keyPart.store_length;
  }
  return res;
}