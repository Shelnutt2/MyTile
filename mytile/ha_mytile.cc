/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation                          // gcc: Class implementation
#endif

#include <my_global.h>

#define MYSQL_SERVER 1

#include <my_config.h>
#include <mysql/plugin.h>
#include "ha_mytile.h"
#include "mytile.h"
#include <cstring>
#include "sql_class.h"
#include <vector>

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

ha_create_table_option mytile_table_option_list[] = {
        HA_TOPTION_STRING("uri", array_uri),
        HA_TOPTION_ENUM("array_type", array_type, "DENSE,SPARSE", TILEDB_SPARSE),
        HA_TOPTION_NUMBER("capacity", capacity, 10000, 0, UINT64_MAX, 1),
        HA_TOPTION_ENUM("cell_order", cell_order, "ROW_MAJOR,COLUMN_MAJOR", TILEDB_ROW_MAJOR),
        HA_TOPTION_ENUM("tile_order", tile_order, "ROW_MAJOR,COLUMN_MAJOR", TILEDB_ROW_MAJOR),
        HA_TOPTION_END
};

ha_create_table_option mytile_field_option_list[] =
        {
                /*
                  If the engine wants something more complex than a string, number, enum,
                  or boolean - for example a list - it needs to specify the option
                  as a string and parse it internally.
                */
                HA_FOPTION_BOOL("dimension", dimension, 0),
                HA_FOPTION_STRING("lower_bound", lower_bound),
                HA_FOPTION_STRING("upper_bound", upper_bound),
                HA_FOPTION_STRING("tile_extent", tile_extent),
                HA_FOPTION_END
        };

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
    mytile_hton->table_options = mytile_table_option_list;
    mytile_hton->field_options = mytile_field_option_list;

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
    DBUG_RETURN(create_array(name, table_arg, create_info, ctx));
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

/*int tile::mytile::rename_table(const char *from, const char *to) {
    DBUG_ENTER("tile::mytile::rename_table");
    DBUG_RETURN(0);
}*/

int tile::mytile::open(const char *name, int mode, uint test_if_locked) {
    DBUG_ENTER("tile::mytile::open");
    if (!(share = get_share()))
        DBUG_RETURN(1);
    thr_lock_data_init(&share->lock, &lock, nullptr);

    // Open TileDB Array
    try {
        std::string uri = name;
        if (this->table->s->option_struct->array_uri != nullptr)
            uri = this->table->s->option_struct->array_uri;

        tiledb_query_type_t openType = tiledb_query_type_t::TILEDB_READ;
        if (mode == O_RDWR)
            openType = tiledb_query_type_t::TILEDB_WRITE;

        array = std::make_unique<tiledb::Array>(this->ctx, uri, openType);

        tiledb::ArraySchema schema = array->schema();

        std::set<std::string> dimensions;

        for (const tiledb::Dimension &dimension : schema.domain().dimensions()) {
            dimensions.emplace(dimension.name());
        }

        for (size_t fieldIndex = 0; fieldIndex < this->table->s->fields; fieldIndex++) {
            Field *field = this->table->field[fieldIndex];

            if (dimensions.find(field->field_name.str) != dimensions.end()) {
                dimensionIndexes.emplace(field->field_name.str, fieldIndex);
            }
        }

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

    buffers.reserve(this->table->s->fields);

    DBUG_RETURN(0);
};

int tile::mytile::close(void) {
    DBUG_ENTER("tile::mytile::close");
    array->close();
    DBUG_RETURN(0);
};

/* Table Scanning */
int tile::mytile::rnd_init(bool scan) {
    DBUG_ENTER("tile::mytile::rnd_init");
    // lock basic mutex
    //mysql_mutex_lock(&share->mutex);

    currentRowPosition = 0;
    query = std::make_unique<tiledb::Query>(this->ctx, *this->array, tiledb_query_type_t::TILEDB_READ);
    query->set_layout(tiledb_layout_t::TILEDB_GLOBAL_ORDER);
    uint64_t readBufferSize = THDVAR(this->ha_thd(), read_buffer_size);
    allocBuffers(readBufferSize);

    // Build subarray for query based on split
    void *subarray = buildSubArray(scan);


    totalNumRecordsUB = computeRecordsUB(subarray);

    setSubarray(subarray);

    //query->submit();
    DBUG_RETURN(0);
};

void tile::mytile::setSubarray(void *subarray) {
    tiledb::ArraySchema schema = array->schema();
    tiledb::Domain domain = schema.domain();
    size_t elements = domain.ndim() * 2;
    switch (domain.type()) {
        case TILEDB_INT8: {
            query->set_subarray(static_cast<int8 *>(subarray), elements);
            return;
        }
        case TILEDB_UINT8: {
            query->set_subarray(static_cast<uint8 *>(subarray), elements);
            return;
        }
        case TILEDB_INT16: {
            query->set_subarray(static_cast<int16 *>(subarray), elements);
            return;
        }
        case TILEDB_UINT16: {
            query->set_subarray(static_cast<uint16 *>(subarray), elements);
            return;
        }
        case TILEDB_INT32: {
            query->set_subarray(static_cast<int32 *>(subarray), elements);
            return;
        }
        case TILEDB_UINT32: {
            query->set_subarray(static_cast<uint32 *>(subarray), elements);
            return;
        }
        case TILEDB_INT64: {
            query->set_subarray(static_cast<int64 *>(subarray), elements);
            return;
        }
        case TILEDB_UINT64: {
            query->set_subarray(static_cast<uint64 *>(subarray), elements);
            return;
        }
        case TILEDB_FLOAT32: {
            query->set_subarray(static_cast<float *>(subarray), elements);
            return;
        }
        case TILEDB_FLOAT64: {
            query->set_subarray(static_cast<double *>(subarray), elements);
            return;
        }
        case TILEDB_CHAR:
        case TILEDB_STRING_ASCII:
        case TILEDB_STRING_UTF8:
        case TILEDB_STRING_UTF16:
        case TILEDB_STRING_UTF32:
        case TILEDB_STRING_UCS2:
        case TILEDB_STRING_UCS4:
        case TILEDB_ANY:
            sql_print_error("Unsupported dimension type");
            break;
    }
}

uint64_t tile::mytile::computeRecordsUB(void *subarray) {
    tiledb::ArraySchema schema = array->schema();
    switch (schema.domain().type()) {
        case TILEDB_INT8: {
            return computeRecordsUB<int8>(subarray);
        }
        case TILEDB_UINT8: {
            return computeRecordsUB<uint8>(subarray);
        }
        case TILEDB_INT16: {
            return computeRecordsUB<int16>(subarray);
        }
        case TILEDB_UINT16: {
            return computeRecordsUB<uint16>(subarray);
        }
        case TILEDB_INT32: {
            return computeRecordsUB<int32>(subarray);
        }
        case TILEDB_UINT32: {
            return computeRecordsUB<uint32>(subarray);
        }
        case TILEDB_INT64: {
            return computeRecordsUB<int64>(subarray);
        }
        case TILEDB_UINT64: {
            return computeRecordsUB<uint64>(subarray);
        }
        case TILEDB_FLOAT32: {
            return computeRecordsUB<float>(subarray);
        }
        case TILEDB_FLOAT64: {
            return computeRecordsUB<double>(subarray);
        }
        case TILEDB_CHAR:
        case TILEDB_STRING_ASCII:
        case TILEDB_STRING_UTF8:
        case TILEDB_STRING_UTF16:
        case TILEDB_STRING_UTF32:
        case TILEDB_STRING_UCS2:
        case TILEDB_STRING_UCS4:
        case TILEDB_ANY:
            sql_print_error("Unsupported dimension type");
            break;
    }
    return 0;
}


void *tile::mytile::buildSubArray(bool tableScan) {
    DBUG_ENTER("mytile::buildSubArray");

    tiledb::ArraySchema schema = array->schema();
    tiledb::Domain domain = schema.domain();
    auto dimensions = domain.dimensions();

    void *subarray;
    auto type = domain.type();
    switch (type) {
        case TILEDB_INT8: {
            subarray = buildSubArray<int8>(tableScan);
            break;
        }
        case TILEDB_UINT8: {
            subarray = buildSubArray<uint8>(tableScan);
            break;
        }
        case TILEDB_INT16: {
            subarray = buildSubArray<int16>(tableScan);
            break;
        }
        case TILEDB_UINT16: {
            subarray = buildSubArray<uint16>(tableScan);
            break;
        }
        case TILEDB_INT32: {
            subarray = buildSubArray<int32>(tableScan);
            break;
        }
        case TILEDB_UINT32: {
            subarray = buildSubArray<uint32>(tableScan);
            break;
        }
        case TILEDB_INT64: {
            subarray = buildSubArray<int64>(tableScan);
            break;
        }
        case TILEDB_UINT64: {
            subarray = buildSubArray<uint64>(tableScan);
            break;
        }
        case TILEDB_FLOAT32: {
            subarray = buildSubArray<float>(tableScan);
            break;
        }
        case TILEDB_FLOAT64: {
            subarray = buildSubArray<double>(tableScan);
            break;
        }
        case TILEDB_CHAR:
        case TILEDB_STRING_ASCII:
        case TILEDB_STRING_UTF8:
        case TILEDB_STRING_UTF16:
        case TILEDB_STRING_UTF32:
        case TILEDB_STRING_UCS2:
        case TILEDB_STRING_UCS4:
        case TILEDB_ANY:
            sql_print_error("Unsupported dimension type");
            break;
    }

    DBUG_RETURN(subarray);
}


bool tile::mytile::isDimension(std::string name) {
    return dimensionIndexes.find(name) != dimensionIndexes.end();
}

bool tile::mytile::isDimension(LEX_CSTRING name) {
    return isDimension(name.str);
}


/**
  Push condition down to the table handler.

  @param  cond   Condition to be pushed. The condition tree must not be
                 modified by the caller.

  @return
    The 'remainder' condition that caller must use to filter out records.
    NULL means the handler will not return rows that do not match the
    passed condition.

  @note
    CONNECT handles the filtering only for table types that construct
    an SQL or WQL query, but still leaves it to MySQL because only some
    parts of the filter may be relevant.
    The first suballocate finds the position where the string will be
    constructed in the sarea. The second one does make the suballocation
    with the proper length.
*/
/*
const COND *tile::mytile::cond_push(const COND *cond) {
    DBUG_ENTER("mytile::cond_push");

    // NOTE: This is called one or more times by handle interface (I think).
    // It *should* be called before rnd_init, but not positive, need validation
    switch (cond->type()) {
        case Item::COND_ITEM: {
            Item_cond *cond_item = (Item_cond *) cond;
            switch (cond_item->functype()) {
                case Item_func::COND_AND_FUNC:
                    vop = OP_AND;
                    break;
                case Item_func::COND_OR_FUNC:
                    vop = OP_OR;
                    break;
                default:
                    return NULL;
            } // endswitch functype

            List<Item> *arglist = cond_item->argument_list();
            List_iterator<Item> li(*arglist);
            const Item *subitem;

            for (uint32_t i = 0; i < arglist->elements; i++) {
                if ((subitem = li++)) {
                    // COND_ITEMs
                    cond_push((COND*)&subitem);
                }
            }
            break;
        }
        case Item::FUNC_ITEM: {
            Item_func *func_item = (Item_func *) cond;
            Item **args = func_item->arguments();
            const Item *subitem;
            bool isColumn;

            for (uint32_t i = 0; i < func_item->argument_count(); i++) {
                if ((isColumn= args[i]->type() == COND::FIELD_ITEM)) {
                    Item_field *pField = (Item_field *) args[i];
                    LEX_CSTRING fieldName = pField->field->field_name;
                    // Check if field is a dimension, if not we can't push it down, so let mariadb filter it
                    if (!isDimension(fieldName)) {
                        continue;
                    }

                    //TODO: Switch on field type

                    //TODO: Set condition to subarray list
                }
            }
        }
    }
    DBUG_RETURN(cond);
}*/

/**
 * This is the main table scan function
 * @param buf
 * @return
 */
int tile::mytile::rnd_next(uchar *buf) {
    DBUG_ENTER("tile::mytile::rnd_next");
    int rc = 0;
    try {

        // If we have run out of records report EOF
        // note the upper bound of records might be *more* than actual results, thus this check is not guaranteed
        // see the next check were we look for complete query and row position
        if (numRecordsRead >= totalNumRecordsUB) {
            return HA_ERR_END_OF_FILE;
        }

        // If we are complete and there is no more records we report EOF
        if (status == tiledb::Query::Status::COMPLETE && currentRowPosition >= currentNumRecords) {
            return HA_ERR_END_OF_FILE;
        }

        // If the cursor has passed the number of records from the previous query (or if this is the first time),
        // (re)submit the query->
        if (currentRowPosition >= currentNumRecords - 1) {
            do {
                status = query->submit();

                // Compute the number of cells (records) that were returned by the query->
                const std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> &queryResultBufferElements = query->result_buffer_elements();
                currentNumRecords = queryResultBufferElements.find(TILEDB_COORDS)->second.second /
                                    dimensionIndexes.size();

                for (size_t i = 0; i < buffers.size(); i++) {
                    buffers[i]->resultBufferElement = queryResultBufferElements.find(buffers[i]->name)->second;
                }

                // Increase the buffer allocation and resubmit if necessary.
                if (status == tiledb::Query::Status::INCOMPLETE && currentNumRecords == 0) {  // VERY IMPORTANT!!
                    allocBuffers(bufferSizeBytes * 2);
                } else if (currentNumRecords > 0) {
                    currentRowPosition = -1;
                    // Break out of resubmit loop as we have some results.
                    break;
                }
            } while (status == tiledb::Query::Status::INCOMPLETE);
        }

        tileToFields(currentRowPosition, false);

        currentRowPosition++;
        numRecordsRead++;

    } catch (const tiledb::TileDBError &e) {
        // Log errors
        sql_print_error("[rnd_next] error for table %s : %s", this->name.c_str(), e.what());
        rc = -101;
    } catch (const std::exception &e) {
        // Log errors
        sql_print_error("[rnd_next] error for table %s : %s", this->name.c_str(), e.what());
        rc = -102;
    }
    currentRowPosition++;
    DBUG_RETURN(rc);
}

/**
 * Converts a tiledb record to mysql buffer using mysql fields
 * @param item
 * @return
 */
int tile::mytile::tileToFields(uint64_t item, bool dimensions_only) {
    DBUG_ENTER("tile::mytile::tileToFields");
    int rc = 0;
    // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
    my_bitmap_map *orig = dbug_tmp_use_all_columns(table, table->write_set);
    try {
        //for (Field **field = table->field; *field; field++) {
        for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
            Field *field = table->field[fieldIndex];
            field->set_notnull();

            int64_t index = currentRowPosition;

            tile::buffer *buffer = buffers[fieldIndex].get();

            if (buffer->isDimension) {
                index = currentRowPosition * dimensionIndexes.size() + buffer->bufferPositionOffset;
            } else if (dimensions_only) {
                continue;
            }

            switch (buffer->datatype) {
                /** 8-bit signed integer */
                case TILEDB_INT8:
                    field->store(((int8_t *) buffer->values)[index], false);
                    break;
                    /** 8-bit unsigned integer */
                case TILEDB_UINT8:
                    field->store(((uint8_t *) buffer->values)[index], true);
                    break;
                    /** 16-bit signed integer */
                case TILEDB_INT16:
                    field->store(((int16_t *) buffer->values)[index], false);
                    break;
                    /** 16-bit unsigned integer */
                case TILEDB_UINT16:
                    field->store(((uint16_t *) buffer->values)[index], true);
                    break;
                    /** 32-bit signed integer */
                case TILEDB_INT32:
                    field->store(((int32_t *) buffer->values)[index], false);
                    break;
                    /** 32-bit unsigned integer */
                case TILEDB_UINT32:
                    field->store(((uint32_t *) buffer->values)[index], true);
                    break;
                    /** 64-bit signed integer */
                case TILEDB_INT64:
                    field->store(((int64_t *) buffer->values)[index], false);
                    break;
                    /** 64-bit unsigned integer */
                case TILEDB_UINT64:
                    field->store(((uint64_t *) buffer->values)[index], true);
                    break;
                    /** 32-bit floating point value */
                case TILEDB_FLOAT32:
                    field->store(((float *) buffer->values)[currentRowPosition]);
                    break;
                    /** 64-bit floating point value */
                case TILEDB_FLOAT64:
                    field->store(((double *) buffer->values)[currentRowPosition]);
                    break;
                    /** Character */
                case TILEDB_CHAR: {
                    std::string rowString = tile::getTileDBString(buffer, currentRowPosition);
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
                    /** ASCII string */
                case TILEDB_STRING_ASCII: {
                    std::string rowString = tile::getTileDBString(buffer, currentRowPosition);
                    field->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
                    break;
                }
                    /** UTF-8 string */
                case TILEDB_STRING_UTF8: {
                    std::string rowString = tile::getTileDBString(buffer, currentRowPosition);
                    field->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
                    break;
                }
                    /** UTF-16 string */
                case TILEDB_STRING_UTF16: {
                    std::string rowString = tile::getTileDBString(buffer, currentRowPosition);
                    field->store(rowString.c_str(), rowString.length(), &my_charset_utf16_general_ci);
                    break;
                }
                    /** UTF-32 string */
                case TILEDB_STRING_UTF32: {
                    std::string rowString = tile::getTileDBString(buffer, currentRowPosition);
                    field->store(rowString.c_str(), rowString.length(), &my_charset_utf32_general_ci);
                    break;
                }
                    /** UCS2 string */
                case TILEDB_STRING_UCS2: {
                    std::string rowString = tile::getTileDBString(buffer, currentRowPosition);
                    field->store(rowString.c_str(), rowString.length(), &my_charset_ucs2_general_ci);
                    break;
                }
                    /** UCS4 string */
                case TILEDB_STRING_UCS4: {
                    std::string rowString = tile::getTileDBString(buffer, currentRowPosition);
                    field->store(rowString.c_str(), rowString.length(), &my_charset_utf8_general_ci);
                    break;
                }
                    /** This can be any datatype. Must store (type tag, value) pairs. */
                case TILEDB_ANY: {
                    std::string rowString = tile::getTileDBString(buffer, currentRowPosition);
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

    DBUG_RETURN(tileToFields(currentRowPosition, true));
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
    //TODO: copy coordinates
    DBUG_VOID_RETURN;
};

int tile::mytile::rnd_end() {
    DBUG_ENTER("tile::mytile::rnd_end");
    // Unlock basic mutex
    query->finalize();
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
        error = tile_write_row(buf);
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
/*bool tile::mytile::check_primary_key_exists(uchar *buf) {
    DBUG_ENTER("tile::mytile::check_primary_key");
    uchar *to_key = new uchar[table->key_info[this->primaryIndexID].key_length];
    key_copy(to_key, buf, &table->key_info[this->primaryIndexID], table->key_info[this->primaryIndexID].key_length);
    std::vector<uchar> keyVec(to_key, to_key + table->key_info[this->primaryIndexID].key_length);
    tiledb::MapItem existingRow = this->map->get_item(keyVec);
    // Check if primary key exists and the row is not deleted
    if (existingRow.good() && !existingRow.get<bool>(MYTILE_DELETE_ATTRIBUTE))
        DBUG_RETURN(true);
    DBUG_RETURN(false);
}*/

int tile::mytile::tile_write_row(uchar *buf) {
    DBUG_ENTER("tile::mytile::tile_write_row");
    int error = 0;

    if (array->is_open() && array->query_type() != TILEDB_WRITE) {
        array->close();
        array->open(TILEDB_WRITE);
    }
    if (query == nullptr || query->query_type() != TILEDB_WRITE)
        query = std::make_unique<tiledb::Query>(ctx, *array, TILEDB_WRITE);

    // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
    my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);

    uint64_t writeBufferSize = THDVAR(this->ha_thd(), write_buffer_size);
    allocBuffers(1);

    // Key the key we are writting from buffer
    try {
        //for (Field **field = table->field; *field; field++) {
        for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
            Field *field = table->field[fieldIndex];

            if (field->is_null()) {
                error = HA_ERR_UNSUPPORTED;
            } else {

                tile::buffer *buffer = buffers[fieldIndex].get();

                switch (buffer->datatype) {
                    /** 8-bit signed integer */
                    case TILEDB_INT8:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((int8_t *) buffer->values)[0] = static_cast<int8_t>(field->val_int());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<int8>(buffer->name, buffer->offsets, buffer->offset_length,
                                                        static_cast<int8 *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<int8>(buffer->name, static_cast<int8 *>(buffer->values),
                                                        buffer->values_length);
                            }
                        }
                        break;
                        /** 8-bit unsigned integer */
                    case TILEDB_UINT8:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((uint8_t *) buffer->values)[0] = static_cast<uint8_t>(field->val_int());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<uint8>(buffer->name, buffer->offsets, buffer->offset_length,
                                                         static_cast<uint8 *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<uint8>(buffer->name, static_cast<uint8 *>(buffer->values),
                                                         buffer->values_length);
                            }
                        }
                        break;

                        /** 16-bit signed integer */
                    case TILEDB_INT16:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((int16_t *) buffer->values)[0] = static_cast<int16_t>(field->val_int());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<int16>(buffer->name, buffer->offsets, buffer->offset_length,
                                                         static_cast<int16 *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<int16>(buffer->name, static_cast<int16 *>(buffer->values),
                                                         buffer->values_length);
                            }
                        }
                        break;
                        /** 16-bit unsigned integer */
                    case TILEDB_UINT16:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((uint16_t *) buffer->values)[0] = static_cast<uint16_t>(field->val_int());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<uint16>(buffer->name, buffer->offsets, buffer->offset_length,
                                                          static_cast<uint16 *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<uint16>(buffer->name, static_cast<uint16 *>(buffer->values),
                                                          buffer->values_length);
                            }
                        }
                        break;
                        /** 32-bit signed integer */
                    case TILEDB_INT32:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((int32_t *) buffer->values)[0] = static_cast<int32_t>(field->val_int());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<int32>(buffer->name, buffer->offsets, buffer->offset_length,
                                                         static_cast<int32 *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<int32>(buffer->name, static_cast<int32 *>(buffer->values),
                                                         buffer->values_length);
                            }
                        }
                        break;
                        /** 32-bit unsigned integer */
                    case TILEDB_UINT32:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((uint32_t *) buffer->values)[0] = static_cast<uint32_t>(field->val_int());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<uint32>(buffer->name, buffer->offsets, buffer->offset_length,
                                                          static_cast<uint32 *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<uint32>(buffer->name, static_cast<uint32 *>(buffer->values),
                                                          buffer->values_length);
                            }
                        }
                        break;
                        /** 64-bit signed integer */
                    case TILEDB_INT64:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((int64_t *) buffer->values)[0] = static_cast<int64_t>(field->val_int());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<int64>(buffer->name, buffer->offsets, buffer->offset_length,
                                                         static_cast<int64 *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<int64>(buffer->name, static_cast<int64 *>(buffer->values),
                                                         buffer->values_length);
                            }
                        }
                        break;
                        /** 64-bit unsigned integer */
                    case TILEDB_UINT64:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((uint64_t *) buffer->values)[0] = static_cast<uint64_t>(field->val_int());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<uint64>(buffer->name, buffer->offsets, buffer->offset_length,
                                                          static_cast<uint64 *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<uint64>(buffer->name, static_cast<uint64 *>(buffer->values),
                                                          buffer->values_length);
                            }
                        }
                        break;
                        /** 32-bit floating point value */
                    case TILEDB_FLOAT32:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((float *) buffer->values)[0] = static_cast<float>(field->val_real());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<float>(buffer->name, buffer->offsets, buffer->offset_length,
                                                         static_cast<float *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<float>(buffer->name, static_cast<float *>(buffer->values),
                                                         buffer->values_length);
                            }
                        }
                        break;
                        /** 64-bit floating point value */
                    case TILEDB_FLOAT64:
                        // If the column is null, forced to set default non-empty value for tiledb
                        ((double *) buffer->values)[0] = field->val_real();
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<double>(buffer->name, buffer->offsets, buffer->offset_length,
                                                          static_cast<double *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<double>(buffer->name, static_cast<double *>(buffer->values),
                                                          buffer->values_length);
                            }
                        }
                        break;
                        /** Character */
                    case TILEDB_CHAR:
                        /** ASCII string */
                    case TILEDB_STRING_ASCII:
                        /** UTF-8 string */
                    case TILEDB_STRING_UTF8: {
                        // If the column is null, forced to set default non-empty value for tiledb
                        char attribute_buffer[1024 * 8];
                        String attribute(attribute_buffer, sizeof(attribute_buffer),
                                         &my_charset_utf8_general_ci);
                        field->val_str(&attribute, &attribute);
                        if (attribute.length() > 1) {
                            buffer->values_length = attribute.length();
                            buffer->values = new char[attribute.length()];
                        }
                        std::strcpy(static_cast<char *>(buffer->values), attribute.c_ptr_safe());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<char>(buffer->name, buffer->offsets, buffer->offset_length,
                                                        static_cast<char *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<char>(buffer->name, static_cast<char *>(buffer->values),
                                                        buffer->values_length);
                            }
                        }
                        break;
                    }
                        /** UTF-16 string */
                    case TILEDB_STRING_UTF16: {
                        // If the column is null, forced to set default non-empty value for tiledb
                        char attribute_buffer[1024 * 8];
                        String attribute(attribute_buffer, sizeof(attribute_buffer),
                                         &my_charset_utf16_general_ci);
                        field->val_str(&attribute, &attribute);
                        if (attribute.length() > 1) {
                            buffer->values_length = attribute.length();
                            buffer->values = new char[attribute.length()];
                        }
                        std::strcpy(static_cast<char *>(buffer->values), attribute.c_ptr_safe());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<char>(buffer->name, buffer->offsets, buffer->offset_length,
                                                        static_cast<char *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<char>(buffer->name, static_cast<char *>(buffer->values),
                                                        buffer->values_length);
                            }
                        }
                        break;
                    }
                        /** UTF-32 string */
                    case TILEDB_STRING_UTF32: {
                        // If the column is null, forced to set default non-empty value for tiledb
                        char attribute_buffer[1024 * 8];
                        String attribute(attribute_buffer, sizeof(attribute_buffer),
                                         &my_charset_utf32_general_ci);
                        field->val_str(&attribute, &attribute);
                        if (attribute.length() > 1) {
                            buffer->values_length = attribute.length();
                            buffer->values = new char[attribute.length()];
                        }
                        std::strcpy(static_cast<char *>(buffer->values), attribute.c_ptr_safe());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<char>(buffer->name, buffer->offsets, buffer->offset_length,
                                                        static_cast<char *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<char>(buffer->name, static_cast<char *>(buffer->values),
                                                        buffer->values_length);
                            }
                        }
                        break;
                    }
                        /** UCS2 string */
                    case TILEDB_STRING_UCS2: {
                        // If the column is null, forced to set default non-empty value for tiledb
                        char attribute_buffer[1024 * 8];
                        String attribute(attribute_buffer, sizeof(attribute_buffer),
                                         &my_charset_ucs2_general_ci);
                        field->val_str(&attribute, &attribute);
                        if (attribute.length() > 1) {
                            buffer->values_length = attribute.length();
                            buffer->values = new char[attribute.length()];
                        }
                        std::strcpy(static_cast<char *>(buffer->values), attribute.c_ptr_safe());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<char>(buffer->name, buffer->offsets, buffer->offset_length,
                                                        static_cast<char *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<char>(buffer->name, static_cast<char *>(buffer->values),
                                                        buffer->values_length);
                            }
                        }
                        break;
                    }
                        /** UCS4 string */
                    case TILEDB_STRING_UCS4: {
                        // If the column is null, forced to set default non-empty value for tiledb
                        char attribute_buffer[1024 * 8];
                        String attribute(attribute_buffer, sizeof(attribute_buffer),
                                         &my_charset_utf8_general_ci);
                        field->val_str(&attribute, &attribute);
                        if (attribute.length() > 1) {
                            buffer->values_length = attribute.length();
                            buffer->values = new char[attribute.length()];
                        }
                        std::strcpy(static_cast<char *>(buffer->values), attribute.c_ptr_safe());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<char>(buffer->name, buffer->offsets, buffer->offset_length,
                                                        static_cast<char *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<char>(buffer->name, static_cast<char *>(buffer->values),
                                                        buffer->values_length);
                            }
                        }
                        break;
                    }
                        /** This can be any datatype. Must store (type tag, value) pairs. */
                    case TILEDB_ANY: {
                        // If the column is null, forced to set default non-empty value for tiledb
                        char attribute_buffer[1024 * 8];
                        String attribute(attribute_buffer, sizeof(attribute_buffer),
                                         &my_charset_utf8_general_ci);
                        field->val_str(&attribute, &attribute);
                        if (attribute.length() > 1) {
                            buffer->values_length = attribute.length();
                            buffer->values = new char[attribute.length()];
                        }
                        std::strcpy(static_cast<char *>(buffer->values), attribute.c_ptr_safe());
                        if (!buffer->isDimension) {
                            if (buffer->offsets != nullptr) {
                                query->set_buffer<char>(buffer->name, buffer->offsets, buffer->offset_length,
                                                        static_cast<char *>(buffer->values), buffer->values_length);
                            } else {
                                query->set_buffer<char>(buffer->name, static_cast<char *>(buffer->values),
                                                        buffer->values_length);
                            }
                        }
                        break;
                    }
                }
            }
        }

        query->submit();
        query->finalize();

        /*if (!error) {
            for (size_t fieldIndex = 0; fieldIndex < buffers.size(); fieldIndex++) {
                tile::buffer* buffer = buffers[fieldIndex];
                if ()
                query->set_buffer()
            }
        }*/
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
/*int tile::mytile::delete_row(const uchar *buf) {
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
}*/

/** @brief
  This is a bitmap of flags that indicates how the storage engine
  implements indexes. The current index flags are documented in
  handler.h. If you do not implement indexes, just return zero here.
    @details
  part is the key part to check. First key part is 0.
  If all_parts is set, MySQL wants to know the flags for the combined
  index, up to and including 'part'.
*/
ulong tile::mytile::index_flags(uint idx, uint part, bool all_parts) const {
    return 0;
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
 * Flags for table features supported
 * @return
 */
ulonglong tile::mytile::table_flags(void) const {
    DBUG_ENTER("tile::mytile::table_flags");
    DBUG_RETURN(HA_REC_NOT_IN_SEQ | HA_CAN_SQL_HANDLER //| HA_NULL_IN_KEY | //HA_REQUIRE_PRIMARY_KEY
                | HA_CAN_BIT_FIELD | HA_FILE_BASED | HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE);
}

void tile::mytile::allocBuffers(uint64_t readBufferSize) {
    this->bufferSizeBytes = readBufferSize;
    tiledb::ArraySchema arraySchema = array->schema();
    for (uint fieldIndex = 0; fieldIndex < this->table->s->fields; fieldIndex++) {
        //Field *field = this->table->s->field[fieldIndex];
        if (bitmap_is_set(this->table->read_set, fieldIndex)) {
            tiledb::Attribute attribute = arraySchema.attribute(this->table->s->field[fieldIndex]->field_name.str);
            std::unique_ptr<buffer> buffer = tile::createBuffer(attribute, readBufferSize);

            switch (attribute.type()) {
                case TILEDB_INT8: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<int8>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                static_cast<int8 *>(buffer->values),
                                                buffer->values_length);
                    } else {
                        query->set_buffer<int8>(attribute.name(), static_cast<int8 *>(buffer->values),
                                                buffer->values_length);
                    }
                    break;
                }
                case TILEDB_UINT8: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<uint8>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                 static_cast<uint8 *>(buffer->values),
                                                 buffer->values_length);
                    } else {
                        query->set_buffer<uint8>(attribute.name(), static_cast<uint8 *>(buffer->values),
                                                 buffer->values_length);
                    }
                    break;
                }
                case TILEDB_INT16: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<int16>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                 static_cast<int16 *>(buffer->values),
                                                 buffer->values_length);
                    } else {
                        query->set_buffer<int16>(attribute.name(), static_cast<int16 *>(buffer->values),
                                                 buffer->values_length);
                    }
                    break;
                }
                case TILEDB_UINT16: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<uint16>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                  static_cast<uint16 *>(buffer->values),
                                                  buffer->values_length);
                    } else {
                        query->set_buffer<uint16>(attribute.name(), static_cast<uint16 *>(buffer->values),
                                                  buffer->values_length);
                    }
                    break;
                }
                case TILEDB_INT32: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<int32>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                 static_cast<int32 *>(buffer->values),
                                                 buffer->values_length);
                    } else {
                        query->set_buffer<int32>(attribute.name(), static_cast<int32 *>(buffer->values),
                                                 buffer->values_length);
                    }
                    break;
                }
                case TILEDB_UINT32: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<uint32>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                  static_cast<uint32 *>(buffer->values),
                                                  buffer->values_length);
                    } else {
                        query->set_buffer<uint32>(attribute.name(), static_cast<uint32 *>(buffer->values),
                                                  buffer->values_length);
                    }
                    break;
                }
                case TILEDB_INT64: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<int64>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                 static_cast<int64 *>(buffer->values),
                                                 buffer->values_length);
                    } else {
                        query->set_buffer<int64>(attribute.name(), static_cast<int64 *>(buffer->values),
                                                 buffer->values_length);
                    }
                    break;
                }
                case TILEDB_UINT64: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<uint64>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                  static_cast<uint64 *>(buffer->values),
                                                  buffer->values_length);
                    } else {
                        query->set_buffer<uint64>(attribute.name(), static_cast<uint64 *>(buffer->values),
                                                  buffer->values_length);
                    }
                    break;
                }
                case TILEDB_FLOAT32: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<float>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                 static_cast<float *>(buffer->values),
                                                 buffer->values_length);
                    } else {
                        query->set_buffer<float>(attribute.name(), static_cast<float *>(buffer->values),
                                                 buffer->values_length);
                    }
                    break;
                }
                case TILEDB_FLOAT64: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<double>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                  static_cast<double *>(buffer->values),
                                                  buffer->values_length);
                    } else {
                        query->set_buffer<double>(attribute.name(), static_cast<double *>(buffer->values),
                                                  buffer->values_length);
                    }
                    break;
                }
                case TILEDB_CHAR:
                case TILEDB_STRING_ASCII:
                case TILEDB_STRING_UCS2:
                case TILEDB_STRING_UCS4:
                case TILEDB_STRING_UTF8:
                case TILEDB_STRING_UTF16:
                case TILEDB_STRING_UTF32:
                case TILEDB_ANY: {
                    if (attribute.variable_sized()) {
                        query->set_buffer<char>(attribute.name(), buffer->offsets, buffer->offset_length,
                                                static_cast<char *>(buffer->values),
                                                buffer->values_length);
                    } else {
                        query->set_buffer<char>(attribute.name(), static_cast<char *>(buffer->values),
                                                buffer->values_length);
                    }
                    break;
                }
            }
            buffers[fieldIndex] = std::move(buffer);
        }
    }
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
                        mytile_system_variables,                      /* system variables */
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
                        mytile_system_variables,                      /* system variables */
                        "0.1",                                        /* string version */
                        MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
                }maria_declare_plugin_end;