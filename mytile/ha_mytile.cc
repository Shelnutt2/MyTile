/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include <sql_plugin.h>
#include <log.h>
#include <key.h>
#include <vector>
#include <array>
#include "ha_mytile.h"
#include "mytile.h"

// Handler for mytile engine
handlerton *mytile_hton;

/**
  @brief
  Function we use in the creation of our hash to get key.
*/

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_mytile_share_mutex;

static PSI_mutex_info all_mytile_mutexes[] =
    {
        {&ex_key_mutex_mytile_share_mutex, "mytile_share::mutex", 0}
    };

static void init_mytile_psi_keys() {
  const char *category = "mytile";
  int count;

  count = array_elements(all_mytile_mutexes);
  mysql_mutex_register(category, all_mytile_mutexes, count);
}

#endif

tile::mytile_share::mytile_share() {
  thr_lock_init(&lock);
  mysql_mutex_init(ex_key_mutex_mytile_share_mutex,
                   &mutex, MY_MUTEX_INIT_FAST);
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each mytile handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

tile::mytile_share *tile::mytile::get_share() {
  tile::mytile_share *tmp_share;

  DBUG_ENTER("tile::mytile::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<tile::mytile_share *>(get_ha_share_ptr()))) {
    tmp_share = new mytile_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
  err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}

// Create mytile object
static handler *mytile_create_handler(handlerton *hton,
                                      TABLE_SHARE *table,
                                      MEM_ROOT *mem_root) {
  return new(mem_root) tile::mytile(hton, table);
}

// mytile file extensions
static const char *mytile_exts[] = {
    NullS
};

// Initialization function
static int mytile_init_func(void *p) {
  DBUG_ENTER("mytile_init_func");

#ifdef HAVE_PSI_INTERFACE
  // Initialize performance schema keys
  init_mytile_psi_keys();
#endif

  mytile_hton = (handlerton *) p;
  mytile_hton->state = SHOW_OPTION_YES;
  mytile_hton->create = mytile_create_handler;
  mytile_hton->tablefile_extensions = mytile_exts;

  DBUG_RETURN(0);
}

// Storage engine interface
struct st_mysql_storage_engine mytile_storage_engine =
    {MYSQL_HANDLERTON_INTERFACE_VERSION};

/**
 * Create a table structure and TileDB map schema
 * @param name
 * @param table_arg
 * @param create_info
 * @return
 */
int tile::mytile::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) {
  DBUG_ENTER("tile::mytile::create");
  DBUG_RETURN(create_map(name, table_arg, create_info));
}

/**
 * Delete a table by rm'ing the tiledb directory
 * @param name
 * @return
 */
int tile::mytile::delete_table(const char *name) {
  DBUG_ENTER("tile::mytile::delete_table");
  //Delete dir
  //DBUG_RETURN(tile::removeDirectory(name));
  try {
    tiledb::VFS vfs(ctx);
    vfs.remove_dir(name);
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error("delete_table error for table %s : %s", this->name.c_str(), e.what());
    DBUG_RETURN(-20);
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error("delete_table error for table %s : %s", this->name.c_str(), e.what());
    DBUG_RETURN(-21);
  }
  DBUG_RETURN(0);

}

int tile::mytile::rename_table(const char *from, const char *to) {
  DBUG_ENTER("tile::mytile::rename_table");
  DBUG_RETURN(0);
}

int tile::mytile::open(const char *name, int mode, uint test_if_locked) {
  DBUG_ENTER("tile::mytile::open");
  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock, &lock, NULL);

  // Create TileDB map
  try {
    map = std::make_unique<tiledb::Map>(this->ctx, name);
    mapSchema = std::make_unique<tiledb::MapSchema>(ctx, name);
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error("open error for table %s : %s", this->name.c_str(), e.what());
    DBUG_RETURN(-10);
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error("open error for table %s : %s", this->name.c_str(), e.what());
    DBUG_RETURN(-11);
  }
  this->name = name;

  this->primaryIndexID = 255;
  for (uint i = 0; i < table->s->keys; i++) {
    if (table->s->key_info[i].flags & HA_NOSAME) {
      this->primaryIndexID = i;
      break;
    }
  }
  if (this->primaryIndexID == 255) {
    sql_print_error("Could not find primary key for %s", name);
    DBUG_RETURN(-12);
  }

  ref_length = table->s->key_info[primaryIndexID].key_length;
  DBUG_RETURN(0);
};

int tile::mytile::close(void) {
  DBUG_ENTER("tile::mytile::close");
  DBUG_RETURN(0);
};

/* Table Scanning */
int tile::mytile::rnd_init(bool scan) {
  DBUG_ENTER("tile::mytile::rnd_init");
  // lock basic mutex
  //mysql_mutex_lock(&share->mutex);
  this->mapIterator = std::make_unique<tiledb::Map::iterator>(map->begin());
  DBUG_RETURN(0);
};

/**
 * This is the main table scan function
 * @param buf
 * @return
 */
int tile::mytile::rnd_next(uchar *buf) {
  DBUG_ENTER("tile::mytile::rnd_next");
  int rc = 0;
  try {
    if (*this->mapIterator == this->map->end()) {
      rc = HA_ERR_END_OF_FILE;
    } else {
      if (!(*this->mapIterator)->get<bool>(MYTILE_DELETE_ATTRIBUTE))
        rc = tileToFields(*(*this->mapIterator));
      else { // If the row is marked as delete move to next one
        this->mapIterator->operator++();
        rc = rnd_next(buf);
      }
    }
    if (!rc)
      this->mapIterator->operator++();
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error("[rnd_next] error for table %s : %s", this->name.c_str(), e.what());
    rc = -101;
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error("[rnd_next] error for table %s : %s", this->name.c_str(), e.what());
    rc = -102;
  }
  DBUG_RETURN(rc);
}

/**
 * Converts a tiledb MapItem to mysql buffer using mysql fields
 * @param item
 * @return
 */
int tile::mytile::tileToFields(tiledb::MapItem item) {
  DBUG_ENTER("tile::mytile::tileToFields");
  int rc = 0;
  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *orig = dbug_tmp_use_all_columns(table, table->write_set);
  try {
    auto attributesMap = this->mapSchema->attributes();
    std::array<bool, MAX_FIELDS> nulls = item.get<std::array<bool, MAX_FIELDS>>(MYTILE_NULLS_ATTRIBUTE);
    //for (Field **field = table->field; *field; field++) {
    for (uint i = 0; i < table->s->fields; i++) {
      Field *field = table->field[i];
      //Check for Null
      if (nulls[i]) {
        field->set_null();
        continue;
      }

      field->set_notnull();
      auto attributePair = attributesMap.find(field->field_name);
      if (attributePair == attributesMap.end()) {
        sql_print_error("Field %s is not present in the schema map but is in field list. Table %s is broken.",
                        field->field_name, name.c_str());
        rc = -200;
        break;
      }
      switch (attributePair->second.type()) {
        /** 32-bit signed integer */
        case TILEDB_INT32:
          field->store(item.get<int32_t>(field->field_name), false);
          break;
          /** 64-bit signed integer */
        case TILEDB_INT64:
          field->store(item.get<int64_t>(field->field_name), false);
          break;
          /** 32-bit floating point value */
        case TILEDB_FLOAT32:
          field->store(item.get<float>(field->field_name));
          break;
          /** 64-bit floating point value */
        case TILEDB_FLOAT64:
          field->store(item.get<double>(field->field_name));
          break;
          /** Character */
        case TILEDB_CHAR: {
          std::string rowString = item.get<std::string>(field->field_name);
          switch (field->type()) {
            case MYSQL_TYPE_GEOMETRY:
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_ENUM: {
              field->store(rowString.c_str(), rowString.length(), &my_charset_bin);
              break;
            }
            default:
              field->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
              break;
          }
          break;
        }
          /** 8-bit signed integer */
        case TILEDB_INT8:
          field->store(item.get<int8_t>(field->field_name), true);
          break;
          /** 8-bit unsigned integer */
        case TILEDB_UINT8: {
          field->store(item.get<uint8_t>(field->field_name), false);
          break;
        }
          /** 16-bit signed integer */
        case TILEDB_INT16:
          field->store(item.get<int16_t>(field->field_name), false);
          break;
          /** 16-bit unsigned integer */
        case TILEDB_UINT16:
          field->store(item.get<uint16_t>(field->field_name), true);
          break;
          /** 32-bit unsigned integer */
        case TILEDB_UINT32:
          field->store(item.get<uint32_t>(field->field_name), true);
          break;
          /** 64-bit unsigned integer */
        case TILEDB_UINT64:
          field->store(item.get<uint64_t>(field->field_name), true);
          break;
          /** ASCII string */
        case TILEDB_STRING_ASCII: {
          std::string rowString = item.get<std::string>(field->field_name);
          field->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
          break;
        }
          /** UTF-8 string */
        case TILEDB_STRING_UTF8: {
          std::string rowString = item.get<std::string>(field->field_name);
          field->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
          break;
        }
          /** UTF-16 string */
        case TILEDB_STRING_UTF16: {
          std::string rowString = item.get<std::string>(field->field_name);
          field->store(rowString.c_str(), rowString.length(), &my_charset_utf16_general_ci);
          break;
        }
          /** UTF-32 string */
        case TILEDB_STRING_UTF32: {
          std::string rowString = item.get<std::string>(field->field_name);
          field->store(rowString.c_str(), rowString.length(), &my_charset_utf32_general_ci);
          break;
        }
          /** UCS2 string */
        case TILEDB_STRING_UCS2: {
          std::string rowString = item.get<std::string>(field->field_name);
          field->store(rowString.c_str(), rowString.length(), &my_charset_ucs2_general_ci);
          break;
        }
          /** UCS4 string */
        case TILEDB_STRING_UCS4: {
          std::string rowString = item.get<std::string>(field->field_name);
          field->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
          break;
        }
          /** This can be any datatype. Must store (type tag, value) pairs. */
        case TILEDB_ANY: {
          std::string rowString = item.get<std::string>(field->field_name);
          field->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
          break;
        }
      }
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error("[rnd_next] error for table %s : %s", this->name.c_str(), e.what());
    rc = -101;
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error("[rnd_next] error for table %s : %s", this->name.c_str(), e.what());
    rc = -102;
  }
  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->write_set, orig);
  DBUG_RETURN(rc);
};

/**
 * Read position based on vector key
 * @param buf
 * @param pos
 * @return
 */
int tile::mytile::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_ENTER("tile::mytile::rnd_pos");
  std::vector<uchar> keyVec(pos, pos + table->s->key_info[primaryIndexID].key_length);
  auto rowItem = this->map->get_item(keyVec);
  if (!rowItem.good())
    DBUG_RETURN(-300);

  DBUG_RETURN(tileToFields(rowItem));
};

/**
 * In the case of an order by rows will need to be sorted.
  ::position() is called after each call to ::rnd_next(),
  the data it stores is to a byte array. You can store this
  data via my_store_ptr(). ref_length is a variable defined to the
  class that is the sizeof() of position being stored.

 * @param record
 */
void tile::mytile::position(const uchar *record) {
  DBUG_ENTER("tile::mytile::position");
  /* Copy primary key as the row reference */
  KEY *key_info = table->key_info + this->primaryIndexID;
  key_copy(ref, (uchar *) record, key_info, key_info->key_length);
  DBUG_VOID_RETURN;
};

int tile::mytile::rnd_end() {
  DBUG_ENTER("tile::mytile::rnd_end");
  // Unlock basic mutex
  //mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}

int tile::mytile::write_row(uchar *buf) {
  DBUG_ENTER("tile::mytile::write_row");
  int error = 0;

  /*
   If we have an auto_increment column and we are writing a changed row
   or a new row, then update the auto_increment value in the record.
*/
  if (table->next_number_field && buf == table->record[0])
    error = update_auto_increment();

  if (!error) {
    //Check if primary_key exists
    if (check_primary_key_exists(buf)) {
      print_keydup_error(table, &table->key_info[this->primaryIndexID], MYF(0));
      error = HA_ERR_FOUND_DUPP_KEY;
    } else {
      error = tile_write_row(buf);
    }
  } else {
    sql_print_error("update auto increment failed (error code %d) for write_row on table %s", error,
                    this->name.c_str());
  }
  DBUG_RETURN(error);
}

/**
 * Checks if a primary key from a row exists
 * @param buf
 * @return
 */
bool tile::mytile::check_primary_key_exists(uchar *buf) {
  DBUG_ENTER("tile::mytile::check_primary_key");
  uchar *to_key = new uchar[table->key_info[this->primaryIndexID].key_length];
  key_copy(to_key, buf, &table->key_info[this->primaryIndexID], table->key_info[this->primaryIndexID].key_length);
  std::vector<uchar> keyVec(to_key, to_key + table->key_info[this->primaryIndexID].key_length);
  tiledb::MapItem existingRow = this->map->get_item(keyVec);
  // Check if primary key exists and the row is not deleted
  if (existingRow.good() && !existingRow.get<bool>(MYTILE_DELETE_ATTRIBUTE))
    DBUG_RETURN(true);
  DBUG_RETURN(false);
}

int tile::mytile::tile_write_row(uchar *buf) {
  DBUG_ENTER("tile::mytile::tile_write_row");
  int error = 0;

  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);

  // Key the key we are writting from buffer
  uchar *to_key = new uchar[table->key_info[this->primaryIndexID].key_length];
  key_copy(to_key, buf, &table->key_info[this->primaryIndexID], table->key_info[this->primaryIndexID].key_length);
  std::vector<uchar> keyVec(to_key, to_key + table->key_info[this->primaryIndexID].key_length);
  try {
    // Create a new item
    auto item = tiledb::Map::create_item(ctx, keyVec);
    // Set item delete to false
    item[MYTILE_DELETE_ATTRIBUTE] = false;
    auto attributesMap = this->mapSchema->attributes();
    std::array<bool, MAX_FIELDS> nulls;
    //for (Field **field = table->field; *field; field++) {
    for (uint i = 0; i < table->s->fields; i++) {
      Field *field = table->field[i];
      // Set null
      nulls[i] = field->is_null();

      auto attributePair = attributesMap.find(field->field_name);
      if (attributePair == attributesMap.end()) {
        sql_print_error("Field %s is not present in the schema map but is in field list. Table %s is broken.",
                        field->field_name, name.c_str());
        error = -100;
        break;
      }
      switch (attributePair->second.type()) {
        /** 32-bit signed integer */
        case TILEDB_INT32:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<int32_t>(0);
          else
            item[field->field_name] = static_cast<int32_t>(field->val_int());
          break;
          /** 64-bit signed integer */
        case TILEDB_INT64:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<int64_t>(0);
          else
            item[field->field_name] = static_cast<int64_t>(field->val_int());
          break;
          /** 32-bit floating point value */
        case TILEDB_FLOAT32:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<float>(0);
          else
            item[field->field_name] = static_cast<float>(field->val_real());
          break;
          /** 64-bit floating point value */
        case TILEDB_FLOAT64:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<double>(0);
          else
            item[field->field_name] = field->val_real();
          break;
          /** Character */
        case TILEDB_CHAR: {
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = std::string("null");
          else {
            char attribute_buffer[1024 * 8];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_utf8_general_ci);
            field->val_str(&attribute, &attribute);
            item[field->field_name] = std::string(attribute.c_ptr_safe());
          }
          break;
        }
          /** 8-bit signed integer */
        case TILEDB_INT8:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<int8_t>(0);
          else
            item[field->field_name] = static_cast<int8_t>(field->val_int());
          break;
          /** 8-bit unsigned integer */
        case TILEDB_UINT8:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<uint8_t>(0);
          else
            item[field->field_name] = static_cast<uint8_t>(field->val_int());
          break;

          /** 16-bit signed integer */
        case TILEDB_INT16:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<int16_t>(0);
          else
            item[field->field_name] = static_cast<int16_t>(field->val_int());
          break;
          /** 16-bit unsigned integer */
        case TILEDB_UINT16:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<uint16_t>(0);
          else
            item[field->field_name] = static_cast<uint16_t>(field->val_int());
          break;
          /** 32-bit unsigned integer */
        case TILEDB_UINT32:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<uint32_t>(0);
          else
            item[field->field_name] = static_cast<uint32_t>(field->val_int());
          break;
          /** 64-bit unsigned integer */
        case TILEDB_UINT64:
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = static_cast<uint64_t>(0);
          else
            item[field->field_name] = static_cast<uint64_t>(field->val_int());
          break;
          /** ASCII string */
        case TILEDB_STRING_ASCII: {
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = std::string("null");
          else {
            //Buffer used for conversion of string
            char attribute_buffer[1024 * 8];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_utf8_general_ci);
            field->val_str(&attribute, &attribute);
            item[field->field_name] = std::string(attribute.c_ptr_safe());
          }
          break;
        }
          /** UTF-8 string */
        case TILEDB_STRING_UTF8: {
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = std::string("null");
          else {
            char attribute_buffer[1024 * 8];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_utf8_general_ci);
            field->val_str(&attribute, &attribute);
            item[field->field_name] = std::string(attribute.c_ptr_safe());
          }
          break;
        }
          /** UTF-16 string */
        case TILEDB_STRING_UTF16: {
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = std::string("null");
          else {
            char attribute_buffer[1024 * 8];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_utf16_general_ci);
            field->val_str(&attribute, &attribute);
            item[field->field_name] = std::string(attribute.c_ptr_safe());
          }
          break;
        }
          /** UTF-32 string */
        case TILEDB_STRING_UTF32: {
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = std::string("null");
          else {
            char attribute_buffer[1024 * 8];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_utf32_general_ci);
            field->val_str(&attribute, &attribute);
            item[field->field_name] = std::string(attribute.c_ptr_safe());
          }
          break;
        }
          /** UCS2 string */
        case TILEDB_STRING_UCS2: {
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = std::string("null");
          else {
            char attribute_buffer[1024 * 8];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_ucs2_general_ci);
            field->val_str(&attribute, &attribute);
            item[field->field_name] = std::string(attribute.c_ptr_safe());
          }
          break;
        }
          /** UCS4 string */
        case TILEDB_STRING_UCS4: {
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = std::string("null");
          else {
            char attribute_buffer[1024 * 8];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_utf8_general_ci);
            field->val_str(&attribute, &attribute);
            item[field->field_name] = std::string(attribute.c_ptr_safe());
          }
          break;
        }
          /** This can be any datatype. Must store (type tag, value) pairs. */
        case TILEDB_ANY: {
          // If the column is null, forced to set default non-empty value for tiledb
          if (field->is_null())
            item[field->field_name] = std::string("null");
          else {
            char attribute_buffer[1024 * 8];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_utf8_general_ci);
            field->val_str(&attribute, &attribute);
            item[field->field_name] = std::string(attribute.c_ptr_safe());
          }
          break;
        }
      }
    }

    item[MYTILE_NULLS_ATTRIBUTE] = nulls;
    // If there is no error we will add the item and flush.
    // Flushing eventually will be done in a transaction, for now its on each write_row
    if (!error) {
      map->add_item(item);
      map->flush();
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error("write error for table %s : %s", this->name.c_str(), e.what());
    error = -101;
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error("write error for table %s : %s", this->name.c_str(), e.what());
    error = -102;
  }

  // Reset bitmap to original

  dbug_tmp_restore_column_map(table->read_set, old_map);
  DBUG_RETURN(error);
}

/**
 * Delete a row based on a primary key
 * Since primary keys are required this uses index_read_map to find the row to delete
 * @param buf
 * @return
 */
int tile::mytile::delete_row(const uchar *buf) {
  DBUG_ENTER("crunch::delete_row");
  int rc = 0;
  // Get key from buffer
  uchar *to_key = new uchar[table->key_info[this->primaryIndexID].key_length];
  key_copy(to_key, const_cast<uchar *>(buf), &table->key_info[this->primaryIndexID],
           table->key_info[this->primaryIndexID].key_length);
  std::vector<uchar> keyVec(to_key, to_key + table->key_info[this->primaryIndexID].key_length);
  try {
    // If the key is good, mark as deleted
    if (this->map->get_item(keyVec).good()) {
      (*this->map)[keyVec][MYTILE_DELETE_ATTRIBUTE] = true;
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error("write error for table %s : %s", this->name.c_str(), e.what());
    rc = -400;
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error("write error for table %s : %s", this->name.c_str(), e.what());
    rc = -401;
  }
  DBUG_RETURN(rc);
}

/**
 * Update a row with new values. In tiledb the latest fragment is always the one that is a read.
 * Each write produces a new fragment, so to update we should just call write_row.
 * @param old_data
 * @param new_data
 * @return
 */
int tile::mytile::update_row(const uchar *old_data, uchar *new_data) {
  DBUG_ENTER("tile::mytile::update_row");
  DBUG_RETURN(tile_write_row(new_data));
}

/**
 * This calls index_read_idx_map to find a row based on a key
 * @param buf
 * @param key
 * @param keypart_map
 * @param find_flag
 * @return
 */
int
tile::mytile::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map, enum ha_rkey_function find_flag) {
  DBUG_ENTER("tile::mytile::index_read_map");
  DBUG_RETURN(index_read_idx_map(buf, active_index, key, keypart_map, find_flag));
}

/**
 * Find a row based on a given key, currently only primary keys are supported
 * @param buf
 * @param idx
 * @param key
 * @param keypart_map
 * @param find_flag
 * @return
 */
int tile::mytile::index_read_idx_map(uchar *buf, uint idx, const uchar *key, key_part_map keypart_map,
                                     enum ha_rkey_function find_flag) {
  DBUG_ENTER("tile::mytile::index_read_idx_map");
  int rc = 0;

  // We iterate over the entire map, this is the worst possible way to implement key lookup.
  tiledb::Map::iterator mapItemIterator = this->map->begin();
  //for (auto mapItem : this->map) {
  std::vector<uchar> prevKey;
  std::vector<uchar> itemKey;

  // TODO: The keys are ordered, but for auto increment this happens to work by the nature of auto increment
  if(key == NULL) {
    sql_print_information("find_flag = %d", find_flag);
    if(find_flag == HA_READ_PREFIX_LAST) {
      while (mapItemIterator != this->map->end()) {
        itemKey = mapItemIterator->key<std::vector<uchar>>();
        sql_print_information("key found: %s", itemKey.data());
        prevKey = itemKey;
        mapItemIterator.operator++();
      }
      itemKey = prevKey;
      sql_print_information("key found final size of: %s", itemKey.size());
    }
  } else { //Key is not null
    while (mapItemIterator != this->map->end()) {
      // Only check the item if it is not deleted
      if (!mapItemIterator->get<bool>(MYTILE_DELETE_ATTRIBUTE)) {
        itemKey = mapItemIterator->key<std::vector<uchar>>();
        if (!tile::cmpKeys(key, itemKey.data(), table->s->key_info + idx)) {
          switch (find_flag) {
            // Currently only support AFTER, BEFORE and EXACT.
            case HA_READ_AFTER_KEY:
              mapItemIterator.operator++();
              itemKey = mapItemIterator->key<std::vector<uchar>>();
              break;
            case HA_READ_BEFORE_KEY:
              itemKey = prevKey;
              break;
            case HA_READ_KEY_EXACT:
              break;
            case HA_READ_KEY_OR_NEXT:
            case HA_READ_KEY_OR_PREV:
            case HA_READ_PREFIX:
            case HA_READ_PREFIX_LAST:
            case HA_READ_PREFIX_LAST_OR_PREV:
            case HA_READ_MBR_CONTAIN:
            case HA_READ_MBR_INTERSECT:
            case HA_READ_MBR_WITHIN:
            case HA_READ_MBR_DISJOINT:
            case HA_READ_MBR_EQUAL:
              /* This flag is not used by the SQL layer, so we don't support it yet. */
              rc = HA_ERR_UNSUPPORTED;
              break;
          }
          break;
        }
        prevKey = itemKey;
      }
      mapItemIterator.operator++();
    }
  }

  if(itemKey.size() == 0)
    rc = HA_ERR_KEY_NOT_FOUND;

  if (!rc) {
    tiledb::MapItem mapItem = this->map->get_item(itemKey);
    if (mapItem.good()) {
      rc = tileToFields(mapItem);
    } else {
      rc = HA_ERR_KEY_NOT_FOUND;
    }
  }

  DBUG_RETURN(rc);
}

int tile::mytile::index_last(uchar *buf) {
  DBUG_ENTER("tile::mytile::index_last");

  int error = index_read_map(buf, NULL, 0, HA_READ_BEFORE_KEY);

  /* MySQL does not seem to allow this to return HA_ERR_KEY_NOT_FOUND */

  if (error == HA_ERR_KEY_NOT_FOUND) {
    error = HA_ERR_END_OF_FILE;
  }
  DBUG_RETURN(error);
}
/**
 * Store a lock, we aren't using table or row locking at this point.
 * @param thd
 * @param to
 * @param lock_type
 * @return
 */
THR_LOCK_DATA **tile::mytile::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
  DBUG_ENTER("tile::mytile::store_lock");
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type = lock_type;
  *to++ = &lock;
  DBUG_RETURN(to);
};

/**
 * Not implemented until transaction support added
 * @param thd
 * @param lock_type
 * @return
 */
int tile::mytile::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER("tile::mytile::external_lock");
  DBUG_RETURN(0);
}

/**
 * This should return relevant stats of the underlying tiledb map,
 * currently just sets row count to 2, to avoid 0/1 row optimizations
 * @return
 */
int tile::mytile::info(uint) {
  DBUG_ENTER("tile::mytile::info");
  //Need records to be greater than 1 to avoid 0/1 row optimizations by query optimizer
  stats.records = 2;
  DBUG_RETURN(0);
};

/**
 * Unimplemented
 * @param inx
 * @param part
 * @param all_parts
 * @return
 */
ulong tile::mytile::index_flags(uint inx, uint part, bool all_parts) const {
  DBUG_ENTER("tile::mytile::index_flags");
  DBUG_RETURN(0);
}

/**
 * Unimplemented
 * @param inx
 * @param min_key
 * @param max_key
 * @return
 */
ha_rows tile::mytile::records_in_range(uint inx, key_range *min_key, key_range *max_key) {
  DBUG_ENTER("tile::mytile::records_in_range");
  DBUG_RETURN(HA_POS_ERROR);
}

/**
 * Flags for table features supported
 * @return
 */
ulonglong tile::mytile::table_flags(void) const {
  DBUG_ENTER("tile::mytile::table_flags");
  DBUG_RETURN(HA_REC_NOT_IN_SEQ | HA_CAN_SQL_HANDLER | HA_NULL_IN_KEY | HA_REQUIRE_PRIMARY_KEY
              | HA_AUTO_PART_KEY | HA_NO_TRANSACTIONS
              | HA_CAN_BIT_FIELD | HA_FILE_BASED | HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE);
};

mysql_declare_plugin(mytile)
        {
            MYSQL_STORAGE_ENGINE_PLUGIN,                  /* the plugin type (a MYSQL_XXX_PLUGIN value)   */
            &mytile_storage_engine,                       /* pointer to type-specific plugin descriptor   */
            "MyTile",                                     /* plugin name                                  */
            "Seth Shelnutt",                              /* plugin author (for I_S.PLUGINS)              */
            "MyTile storage engine",                      /* general descriptive text (for I_S.PLUGINS)   */
            PLUGIN_LICENSE_GPL,                           /* the plugin license (PLUGIN_LICENSE_XXX)      */
            mytile_init_func,                             /* Plugin Init */
            NULL,                                         /* Plugin Deinit */
            0x0001,                                       /* version number (0.1) */
            NULL,                                         /* status variables */
            NULL,                                         /* system variables */
            NULL,                                         /* config options */
            0,                                            /* flags */
        }mysql_declare_plugin_end;
maria_declare_plugin(mytile)
        {
            MYSQL_STORAGE_ENGINE_PLUGIN,                  /* the plugin type (a MYSQL_XXX_PLUGIN value)   */
            &mytile_storage_engine,                       /* pointer to type-specific plugin descriptor   */
            "MyTile",                                     /* plugin name                                  */
            "Seth Shelnutt",                              /* plugin author (for I_S.PLUGINS)              */
            "MyTile storage engine",                      /* general descriptive text (for I_S.PLUGINS)   */
            PLUGIN_LICENSE_GPL,                           /* the plugin license (PLUGIN_LICENSE_XXX)      */
            mytile_init_func,                             /* Plugin Init */
            NULL,                                         /* Plugin Deinit */
            0x0001,                                       /* version number (0.1) */
            NULL,                                         /* status variables */
            NULL,                                         /* system variables */
            "0.1",                                        /* string version */
            MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
        }maria_declare_plugin_end;