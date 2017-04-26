/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include <sql_plugin.h>
#include <log.h>
#include <key.h>
#include <vector>
#include "ha_mytile.h"
#include "mytile.h"
#include "utils.h"

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
  DBUG_RETURN(tile::removeDirectory(name));
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

  DBUG_RETURN(0);
};

int tile::mytile::close(void) {
  DBUG_ENTER("tile::mytile::close");
  DBUG_RETURN(0);
};

/* Table Scaning */
int tile::mytile::rnd_init(bool scan) {
  DBUG_ENTER("tile::mytile::rnd_init");
  // lock basic mutex
  //mysql_mutex_lock(&share->mutex);
  this->mapIterator = std::make_unique<tiledb::Map::iterator>(map->begin());
  DBUG_RETURN(0);
};

int tile::mytile::rnd_next(uchar *buf) {
  DBUG_ENTER("tile::mytile::rnd_next");
  int rc = 0;
  try {
    if (*this->mapIterator == this->map->end()) {
      rc = HA_ERR_END_OF_FILE;
    } else {
      rc = tileToFields(*(*this->mapIterator));
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

int tile::mytile::tileToFields(tiledb::MapItem item) {
  DBUG_ENTER("tile::mytile::tileToFields");
  int rc = 0;
  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *orig = dbug_tmp_use_all_columns(table, table->write_set);
  try {
    auto attributesMap = this->mapSchema->attributes();
    for (Field **field = table->field; *field; field++) {
      (*field)->set_notnull();
      auto attributePair = attributesMap.find((*field)->field_name);
      if (attributePair == attributesMap.end()) {
        sql_print_error("Field %s is not present in the schema map but is in field list. Table %s is broken.",
                        (*field)->field_name, name);
        rc = -200;
        break;
      }
      switch (attributePair->second.type()) {
        /** 32-bit signed integer */
        case TILEDB_INT32:
          (*field)->store(item.get<int32_t>((*field)->field_name), false);
          break;
          /** 64-bit signed integer */
        case TILEDB_INT64:
          (*field)->store(item.get<int64_t>((*field)->field_name), false);
          break;
          /** 32-bit floating point value */
        case TILEDB_FLOAT32:
          (*field)->store(item.get<float>((*field)->field_name));
          break;
          /** 64-bit floating point value */
        case TILEDB_FLOAT64:
          (*field)->store(item.get<double>((*field)->field_name));
          break;
          /** Character */
        case TILEDB_CHAR: {
          std::string rowString = item.get<std::string>((*field)->field_name);
          switch ((*field)->type()) {
            case MYSQL_TYPE_GEOMETRY:
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_ENUM: {
              (*field)->store(rowString.c_str(), rowString.length(), &my_charset_bin);
              break;
            }
            default:
              (*field)->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
              break;
          }
          break;
        }
          /** 8-bit signed integer */
        case TILEDB_INT8:
          (*field)->store(item.get<int8_t>((*field)->field_name), true);
          break;
          /** 8-bit unsigned integer */
        case TILEDB_UINT8: {
          (*field)->store(item.get<uint8_t>((*field)->field_name), false);
          break;
        }
          /** 16-bit signed integer */
        case TILEDB_INT16:
          (*field)->store(item.get<int16_t>((*field)->field_name), false);
          break;
          /** 16-bit unsigned integer */
        case TILEDB_UINT16:
          (*field)->store(item.get<uint16_t>((*field)->field_name), true);
          break;
          /** 32-bit unsigned integer */
        case TILEDB_UINT32:
          (*field)->store(item.get<uint32_t>((*field)->field_name), true);
          break;
          /** 64-bit unsigned integer */
        case TILEDB_UINT64:
          (*field)->store(item.get<uint64_t>((*field)->field_name), true);
          break;
          /** ASCII string */
        case TILEDB_STRING_ASCII: {
          std::string rowString = item.get<std::string>((*field)->field_name);
          (*field)->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
          break;
        }
          /** UTF-8 string */
        case TILEDB_STRING_UTF8: {
          std::string rowString = item.get<std::string>((*field)->field_name);
          (*field)->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
          break;
        }
          /** UTF-16 string */
        case TILEDB_STRING_UTF16: {
          std::string rowString = item.get<std::string>((*field)->field_name);
          (*field)->store(rowString.c_str(), rowString.length(), &my_charset_utf16_general_ci);
          break;
        }
          /** UTF-32 string */
        case TILEDB_STRING_UTF32: {
          std::string rowString = item.get<std::string>((*field)->field_name);
          (*field)->store(rowString.c_str(), rowString.length(), &my_charset_utf32_general_ci);
          break;
        }
          /** UCS2 string */
        case TILEDB_STRING_UCS2: {
          std::string rowString = item.get<std::string>((*field)->field_name);
          (*field)->store(rowString.c_str(), rowString.length(), &my_charset_ucs2_general_ci);
          break;
        }
          /** UCS4 string */
        case TILEDB_STRING_UCS4: {
          std::string rowString = item.get<std::string>((*field)->field_name);
          (*field)->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
          break;
        }
          /** This can be any datatype. Must store (type tag, value) pairs. */
        case TILEDB_ANY: {
          std::string rowString = item.get<std::string>((*field)->field_name);
          (*field)->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
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

int tile::mytile::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_ENTER("tile::mytile::rnd_pos");
  uint64_t len;
  memcpy(&len, pos, sizeof(uint64_t));
  std::vector<uchar> keyVec(pos + sizeof(uint64_t), pos + sizeof(uint64_t) + len);
  auto rowItem = this->map->get_item(keyVec);
  if (!rowItem.good())
    DBUG_RETURN(-300);

  DBUG_RETURN(tileToFields(rowItem));
};

void tile::mytile::position(const uchar *record) {
  DBUG_ENTER("tile::mytile::position");
  uchar *to_key = new uchar[table->key_info[this->primaryIndexID].key_length];
  key_copy(to_key, const_cast<uchar *>(record), &table->key_info[this->primaryIndexID],
           table->key_info[this->primaryIndexID].key_length);
  uint64_t len = table->key_info[this->primaryIndexID].key_length;
  memcpy(ref, &len, sizeof(uint64_t));
  memcpy(ref + sizeof(uint64_t), to_key, len);
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
  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);

  /*
   If we have an auto_increment column and we are writing a changed row
   or a new row, then update the auto_increment value in the record.
*/
  if (table->next_number_field && buf == table->record[0])
    error = update_auto_increment();

  if (!error) {

    uchar *to_key = new uchar[table->key_info[this->primaryIndexID].key_length];
    key_copy(to_key, buf, &table->key_info[this->primaryIndexID], table->key_info[this->primaryIndexID].key_length);
    std::vector<uchar> keyVec(to_key, to_key + table->key_info[this->primaryIndexID].key_length);
    try {
      auto item = tiledb::Map::create_item(ctx, keyVec);
      auto attributesMap = this->mapSchema->attributes();
      for (Field **field = table->field; *field; field++) {
        if (!(*field)->is_null()) {
          auto attributePair = attributesMap.find((*field)->field_name);
          if (attributePair == attributesMap.end()) {
            sql_print_error("Field %s is not present in the schema map but is in field list. Table %s is broken.",
                            (*field)->field_name, name);
            error = -100;
            break;
          }
          switch (attributePair->second.type()) {
            /** 32-bit signed integer */
            case TILEDB_INT32:
              item[(*field)->field_name] = static_cast<int32_t>((*field)->val_int());
              break;
              /** 64-bit signed integer */
            case TILEDB_INT64:
              item[(*field)->field_name] = static_cast<int64_t>((*field)->val_int());
              break;
              /** 32-bit floating point value */
            case TILEDB_FLOAT32:
              item[(*field)->field_name] = static_cast<float>((*field)->val_real());
              break;
              /** 64-bit floating point value */
            case TILEDB_FLOAT64:
              item[(*field)->field_name] = (*field)->val_real();
              break;
              /** Character */
            case TILEDB_CHAR: {
              char attribute_buffer[1024 * 8];
              String attribute(attribute_buffer, sizeof(attribute_buffer),
                               &my_charset_utf8_general_ci);
              (*field)->val_str(&attribute, &attribute);
              item[(*field)->field_name] = std::string(attribute.c_ptr_safe());
              break;
            }
              /** 8-bit signed integer */
            case TILEDB_INT8:
              item[(*field)->field_name] = static_cast<int8_t>((*field)->val_int());
              break;
              /** 8-bit unsigned integer */
            case TILEDB_UINT8:
              item[(*field)->field_name] = static_cast<uint8_t>((*field)->val_int());
              break;

              /** 16-bit signed integer */
            case TILEDB_INT16:
              item[(*field)->field_name] = static_cast<int16_t>((*field)->val_int());
              break;
              /** 16-bit unsigned integer */
            case TILEDB_UINT16:
              item[(*field)->field_name] = static_cast<uint16_t>((*field)->val_int());
              break;
              /** 32-bit unsigned integer */
            case TILEDB_UINT32:
              item[(*field)->field_name] = static_cast<uint32_t>((*field)->val_int());
              break;
              /** 64-bit unsigned integer */
            case TILEDB_UINT64:
              item[(*field)->field_name] = static_cast<uint64_t>((*field)->val_int());
              break;
              /** ASCII string */
            case TILEDB_STRING_ASCII: {
              char attribute_buffer[1024 * 8];
              String attribute(attribute_buffer, sizeof(attribute_buffer),
                               &my_charset_utf8_general_ci);
              (*field)->val_str(&attribute, &attribute);
              item[(*field)->field_name] = std::string(attribute.c_ptr_safe());
              break;
            }
              /** UTF-8 string */
            case TILEDB_STRING_UTF8: {
              char attribute_buffer[1024 * 8];
              String attribute(attribute_buffer, sizeof(attribute_buffer),
                               &my_charset_utf8_general_ci);
              (*field)->val_str(&attribute, &attribute);
              item[(*field)->field_name] = std::string(attribute.c_ptr_safe());
              break;
            }
              /** UTF-16 string */
            case TILEDB_STRING_UTF16: {
              char attribute_buffer[1024 * 8];
              String attribute(attribute_buffer, sizeof(attribute_buffer),
                               &my_charset_utf16_general_ci);
              (*field)->val_str(&attribute, &attribute);
              item[(*field)->field_name] = std::string(attribute.c_ptr_safe());
              break;
            }
              /** UTF-32 string */
            case TILEDB_STRING_UTF32: {
              char attribute_buffer[1024 * 8];
              String attribute(attribute_buffer, sizeof(attribute_buffer),
                               &my_charset_utf32_general_ci);
              (*field)->val_str(&attribute, &attribute);
              item[(*field)->field_name] = std::string(attribute.c_ptr_safe());
              break;
            }
              /** UCS2 string */
            case TILEDB_STRING_UCS2: {
              char attribute_buffer[1024 * 8];
              String attribute(attribute_buffer, sizeof(attribute_buffer),
                               &my_charset_ucs2_general_ci);
              (*field)->val_str(&attribute, &attribute);
              item[(*field)->field_name] = std::string(attribute.c_ptr_safe());
              break;
            }
              /** UCS4 string */
            case TILEDB_STRING_UCS4: {
              char attribute_buffer[1024 * 8];
              String attribute(attribute_buffer, sizeof(attribute_buffer),
                               &my_charset_utf8_general_ci);
              (*field)->val_str(&attribute, &attribute);
              item[(*field)->field_name] = std::string(attribute.c_ptr_safe());
              break;
            }
              /** This can be any datatype. Must store (type tag, value) pairs. */
            case TILEDB_ANY: {
              char attribute_buffer[1024 * 8];
              String attribute(attribute_buffer, sizeof(attribute_buffer),
                               &my_charset_utf8_general_ci);
              (*field)->val_str(&attribute, &attribute);
              item[(*field)->field_name] = std::string(attribute.c_ptr_safe());
              break;
            }
          }
        } else {
          //sql_print_error("Field %s is null!!", (*field)->field_name);
        }
      }
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
  } else {
    sql_print_error("update auto increment failed (error code %d) for write_row on table %s", error,
                    this->name.c_str());
  }

  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->read_set, old_map);
  DBUG_RETURN(error);
}

THR_LOCK_DATA **tile::mytile::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
  DBUG_ENTER("tile::mytile::store_lock");
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type = lock_type;
  *to++ = &lock;
  DBUG_RETURN(to);
};

int tile::mytile::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER("tile::mytile::external_lock");
  DBUG_RETURN(0);
}

int tile::mytile::info(uint) {
  DBUG_ENTER("tile::mytile::info");
  //Need records to be greater than 1 to avoid 0/1 row optimizations by query optimizer
  stats.records = 2;
  DBUG_RETURN(0);
};

ulong tile::mytile::index_flags(uint inx, uint part, bool all_parts) const {
  DBUG_ENTER("tile::mytile::index_flags");
  DBUG_RETURN(0);
}

ha_rows tile::mytile::records_in_range(uint inx, key_range *min_key, key_range *max_key) {
  DBUG_ENTER("tile::mytile::records_in_range");
  DBUG_RETURN(HA_POS_ERROR);
}

ulonglong tile::mytile::table_flags(void) const {
  DBUG_ENTER("tile::mytile::table_flags");
  DBUG_RETURN(HA_REC_NOT_IN_SEQ | HA_CAN_SQL_HANDLER | HA_NULL_IN_KEY | HA_REQUIRE_PRIMARY_KEY
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