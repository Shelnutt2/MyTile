/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include "mytile.h"

int tile::create_map(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) {
// Create TileDB context
  tiledb::Context ctx;


// Create map schema
  tiledb::MapSchema schema(ctx);

// Create attributes
  for(Field **field = table_arg->field; *field; field++) {
    schema.add_attributes(create_field_attribute(*field, ctx));
  }

// Check array schema
  try {
    schema.check();
  } catch (tiledb::TileDBError &e) {
    std::cout << e.what() << "\n";
    return -1;
  }

// Print the map schema
  schema.dump(stdout);

// Create the map on storage
  tiledb::Map::create(name, schema);
}

tiledb::Attribute create_field_attribute(Field *field, tiledb::Context ctx) {
  tiledb::Attribute att;
  switch (field->type()) {

    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      att = tiledb::Attribute::create<double>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;

    case MYSQL_TYPE_FLOAT:
      att = tiledb::Attribute::create<float>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;

    case MYSQL_TYPE_TINY:
      if (((Field_num *) field)->unsigned_flag)
        att = tiledb::Attribute::create<uint8_t>(ctx, field->field_name);
      else
        att = tiledb::Attribute::create<int8_t>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_YEAR:
      if (((Field_num *) field)->unsigned_flag)
        att = tiledb::Attribute::create<uint16_t>(ctx, field->field_name);
      else
        att = tiledb::Attribute::create<int16_t>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;

    case MYSQL_TYPE_INT24:
      if (((Field_num *) field)->unsigned_flag)
        att = tiledb::Attribute::create<uint32_t>(ctx, field->field_name);
      else
        att = tiledb::Attribute::create<int32_t>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;

    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      if (((Field_num *) field)->unsigned_flag)
        att = tiledb::Attribute::create<uint64_t>(ctx, field->field_name);
      else
        att = tiledb::Attribute::create<int64_t>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;

    case MYSQL_TYPE_NULL:
      att = tiledb::Attribute::create<int64_t>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;

    case MYSQL_TYPE_BIT:
      att = tiledb::Attribute::create<uint16_t>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;


    case MYSQL_TYPE_VARCHAR :
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_SET:
      att = tiledb::Attribute::create<std::string>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;

    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_ENUM:
      att = tiledb::Attribute::create<std::string>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;

    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_NEWDATE:
      att = tiledb::Attribute::create<int64_t>(ctx, field->field_name);
      att.set_compresson(TILEDB_BLOSC_LZ);
      return att;
  }

  att = tiledb::Attribute::create<std::string>(ctx, field->field_name);
  return att;
}