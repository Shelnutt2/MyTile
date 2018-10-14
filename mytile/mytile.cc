/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include <my_global.h>
#include "mytile.h"
#include <log.h>

int tile::create_array(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info, tiledb::Context ctx) {
    DBUG_ENTER("tile::create_array");
    int rc = 0;

    tiledb_array_type_t arrayType = TILEDB_SPARSE;
    if (create_info->option_struct->array_type == 0) {
        arrayType = TILEDB_SPARSE;
    } else {
        arrayType = TILEDB_DENSE;
    }

    // Create array schema
    tiledb::ArraySchema schema(ctx, arrayType);

    //schema.add_attribute(tiledb::Attribute::create<bool>(ctx, MYTILE_DELETE_ATTRIBUTE, {TILEDB_ZSTD, -1}));
    //schema.add_attribute(tiledb::Attribute::create<std::array<bool, MAX_FIELDS>>(ctx, MYTILE_NULLS_ATTRIBUTE));

    tiledb::Domain domain(ctx);

    // Create attributes
    for (Field **field = table_arg->field; *field; field++) {
        if (!(*field)->option_struct->dimension) {
            tiledb::FilterList filterList(ctx);
            tiledb::Filter filter(ctx, TILEDB_FILTER_ZSTD);
            filterList.add_filter(filter);
            schema.add_attribute(create_field_attribute(ctx, *field, filterList));
        } else {
            if ((*field)->option_struct->lower_bound == nullptr || strcmp((*field)->option_struct->lower_bound, "") == 0) {
                sql_print_error("Dimension field %s lower bound was not set, can not create table", (*field)->field_name);
                DBUG_RETURN(-11);
            }
            if ((*field)->option_struct->upper_bound == nullptr || strcmp((*field)->option_struct->lower_bound, "") == 0) {
                sql_print_error("Dimension field %s upper bound was not set, can not create table", (*field)->field_name);
                DBUG_RETURN(-12);
            }
            if ((*field)->option_struct->tile_extent == nullptr || strcmp((*field)->option_struct->lower_bound, "") == 0) {
                sql_print_error("Dimension field %s tile extent was not set, can not create table", (*field)->field_name);
                DBUG_RETURN(-13);
            }
            domain.add_dimension(create_field_dimension(ctx, *field));
        };
    }

    schema.set_domain(domain);

    // Set capacity
    schema.set_capacity(create_info->option_struct->capacity);

    // Set cell ordering if configured
    if (create_info->option_struct->cell_order == 0) {
        schema.set_cell_order(TILEDB_ROW_MAJOR);
    } else if (create_info->option_struct->cell_order == 1) {
        schema.set_cell_order(TILEDB_COL_MAJOR);
    }

    // Set tile ordering if configured
    if (create_info->option_struct->tile_order == 0) {
        schema.set_tile_order(TILEDB_ROW_MAJOR);
    } else if (create_info->option_struct->tile_order == 1) {
        schema.set_tile_order(TILEDB_COL_MAJOR);
    }

    std::string uri = name;
    if (create_info->option_struct->array_uri != nullptr)
        uri = create_info->option_struct->array_uri;
    else
        create_info->option_struct->array_uri = const_cast<char*>(name);

    // Check array schema
    try {
        schema.check();
    } catch (tiledb::TileDBError &e) {
        sql_print_error("Error in building schema %s", e.what());
        DBUG_RETURN(-10);
    }

    // Create the array on storage
    tiledb::Array::create(uri, schema);
    DBUG_RETURN(rc);
}

tiledb::Attribute tile::create_field_attribute(tiledb::Context &ctx, Field *field, tiledb::FilterList filterList) {
    switch (field->type()) {

        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            return tiledb::Attribute::create<double>(ctx, field->field_name.str, filterList);

        case MYSQL_TYPE_FLOAT:
            return tiledb::Attribute::create<float>(ctx, field->field_name.str, filterList);

        case MYSQL_TYPE_TINY:
            if (((Field_num *) field)->unsigned_flag)
                return tiledb::Attribute::create<uint8_t>(ctx, field->field_name.str, filterList);
            else
                return tiledb::Attribute::create<int8_t>(ctx, field->field_name.str, filterList);

        case MYSQL_TYPE_SHORT:
            if (((Field_num *) field)->unsigned_flag) {
                return tiledb::Attribute::create<uint16_t>(ctx, field->field_name.str, filterList);
            } else {
                return tiledb::Attribute::create<int16_t>(ctx, field->field_name.str, filterList);
            }
        case MYSQL_TYPE_YEAR:
            return tiledb::Attribute::create<uint16_t>(ctx, field->field_name.str, filterList);

        case MYSQL_TYPE_INT24:
            if (((Field_num *) field)->unsigned_flag)
                return tiledb::Attribute::create<uint32_t>(ctx, field->field_name.str, filterList);
            else
                return tiledb::Attribute::create<int32_t>(ctx, field->field_name.str, filterList);

        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
            if (((Field_num *) field)->unsigned_flag)
                return tiledb::Attribute::create<uint64_t>(ctx, field->field_name.str, filterList);
            else
                return tiledb::Attribute::create<int64_t>(ctx, field->field_name.str, filterList);

        case MYSQL_TYPE_NULL:
            return tiledb::Attribute::create<uint8_t>(ctx, field->field_name.str, filterList);

        case MYSQL_TYPE_BIT:
            return tiledb::Attribute::create<uint8_t>(ctx, field->field_name.str, filterList);


        case MYSQL_TYPE_VARCHAR :
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_SET:
            return tiledb::Attribute::create<std::string>(ctx, field->field_name.str, filterList);

        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_ENUM:
            return tiledb::Attribute::create<std::string>(ctx, field->field_name.str, filterList);

        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_NEWDATE:
            return tiledb::Attribute::create<uint64_t>(ctx, field->field_name.str, filterList);
    }
    sql_print_error("Unknown mysql data type in creating tiledb field attribute");
    return tiledb::Attribute::create<std::string>(ctx, field->field_name.str, filterList);
}

tiledb::Dimension tile::create_field_dimension(tiledb::Context &ctx, Field *field) {
    switch (field->type()) {

        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
            std::array<double, 2> domain = {DBL_MIN, DBL_MAX - 1};
            if (field->option_struct->lower_bound != nullptr) {
                domain[0] = std::strtod(field->option_struct->lower_bound, nullptr);
            }
            if (field->option_struct->upper_bound != nullptr) {
                domain[1] = std::strtod(field->option_struct->upper_bound, nullptr);
            }

            // If a user passed a tile extent use it
            if (field->option_struct->tile_extent != nullptr) {
                double tileExtent = std::strtod(field->option_struct->tile_extent, nullptr);
                return tiledb::Dimension::create<double>(ctx, field->field_name.str, domain, tileExtent);
            }
            return tiledb::Dimension::create<double>(ctx, field->field_name.str, domain);
        }

        case MYSQL_TYPE_FLOAT: {
            std::array<float, 2> domain = {FLT_MIN, FLT_MAX - 1};
            if (field->option_struct->lower_bound != nullptr) {
                domain[0] = std::strtof(field->option_struct->lower_bound, nullptr);
            }
            if (field->option_struct->upper_bound != nullptr) {
                domain[1] = std::strtof(field->option_struct->upper_bound, nullptr);
            }

            // If a user passed a tile extent use it
            if (field->option_struct->tile_extent != nullptr) {
                float tileExtent = std::strtof(field->option_struct->tile_extent, nullptr);
                return tiledb::Dimension::create<float>(ctx, field->field_name.str, domain, tileExtent);
            }
            return tiledb::Dimension::create<float>(ctx, field->field_name.str, domain);
        }

        case MYSQL_TYPE_TINY: {
            if (((Field_num *) field)->unsigned_flag) {
                std::array<uint8_t, 2> domain = {0, UINT8_MAX - 1};
                if (field->option_struct->lower_bound != nullptr) {
                    domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
                }
                if (field->option_struct->upper_bound != nullptr) {
                    domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
                }

                // If a user passed a tile extent use it
                if (field->option_struct->tile_extent != nullptr) {
                    uint8_t tileExtent = std::strtoul(field->option_struct->tile_extent, nullptr, 10);
                    return tiledb::Dimension::create<uint8_t>(ctx, field->field_name.str, domain, tileExtent);
                }
                return tiledb::Dimension::create<uint8_t>(ctx, field->field_name.str, domain);
            } else {
                std::array<int8_t, 2> domain = {INT8_MIN, INT8_MAX - 1};
                if (field->option_struct->lower_bound != nullptr) {
                    domain[0] = std::strtol(field->option_struct->lower_bound, nullptr, 10);
                }
                if (field->option_struct->upper_bound != nullptr) {
                    domain[1] = std::strtol(field->option_struct->upper_bound, nullptr, 10);
                }

                // If a user passed a tile extent use it
                if (field->option_struct->tile_extent != nullptr) {
                    int8_t tileExtent = std::strtol(field->option_struct->tile_extent, nullptr, 10);
                    return tiledb::Dimension::create<int8_t>(ctx, field->field_name.str, domain, tileExtent);
                }
                return tiledb::Dimension::create<int8_t>(ctx, field->field_name.str, domain);
            }
        }

        case MYSQL_TYPE_SHORT: {
            if (((Field_num *) field)->unsigned_flag) {
                std::array<uint16_t, 2> domain = {0, UINT16_MAX - 1};
                if (field->option_struct->lower_bound != nullptr) {
                    domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
                }
                if (field->option_struct->upper_bound != nullptr) {
                    domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
                }

                // If a user passed a tile extent use it
                if (field->option_struct->tile_extent != nullptr) {
                    uint16_t tileExtent = std::strtoul(field->option_struct->tile_extent, nullptr, 10);
                    return tiledb::Dimension::create<uint16_t>(ctx, field->field_name.str, domain, tileExtent);
                }
                return tiledb::Dimension::create<uint16_t>(ctx, field->field_name.str, domain);
            } else {
                std::array<int16_t, 2> domain = {INT16_MIN, INT16_MAX - 1};
                if (field->option_struct->lower_bound != nullptr) {
                    domain[0] = std::strtol(field->option_struct->lower_bound, nullptr, 10);
                }
                if (field->option_struct->upper_bound != nullptr) {
                    domain[1] = std::strtol(field->option_struct->upper_bound, nullptr, 10);
                }

                // If a user passed a tile extent use it
                if (field->option_struct->tile_extent != nullptr) {
                    int16_t tileExtent = std::strtol(field->option_struct->tile_extent, nullptr, 10);
                    return tiledb::Dimension::create<int16_t>(ctx, field->field_name.str, domain, tileExtent);
                }
                return tiledb::Dimension::create<int16_t>(ctx, field->field_name.str, domain);
            }
        }
        case MYSQL_TYPE_YEAR: {
            std::array<uint16_t, 2> domain = {0, UINT16_MAX - 1};
            if (field->option_struct->lower_bound != nullptr) {
                domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
            }
            if (field->option_struct->upper_bound != nullptr) {
                domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
            }

            // If a user passed a tile extent use it
            if (field->option_struct->tile_extent != nullptr) {
                uint64_t tileExtent = std::strtoul(field->option_struct->tile_extent, nullptr, 10);
                return tiledb::Dimension::create<uint16_t>(ctx, field->field_name.str, domain, tileExtent);
            }
            return tiledb::Dimension::create<uint16_t>(ctx, field->field_name.str, domain);
        }

        case MYSQL_TYPE_INT24: {
            if (((Field_num *) field)->unsigned_flag) {
                std::array<uint32_t, 2> domain = {0, UINT32_MAX - 1};
                if (field->option_struct->lower_bound != nullptr) {
                    domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
                }
                if (field->option_struct->upper_bound != nullptr) {
                    domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
                }

                // If a user passed a tile extent use it
                if (field->option_struct->tile_extent != nullptr) {
                    uint32_t tileExtent = std::strtoul(field->option_struct->tile_extent, nullptr, 10);
                    return tiledb::Dimension::create<uint32_t>(ctx, field->field_name.str, domain, tileExtent);
                }
                return tiledb::Dimension::create<uint32_t>(ctx, field->field_name.str, domain);
            } else {
                std::array<int32_t, 2> domain = {INT32_MIN, INT32_MAX - 1};
                if (field->option_struct->lower_bound != nullptr) {
                    domain[0] = std::strtol(field->option_struct->lower_bound, nullptr, 10);
                }
                if (field->option_struct->upper_bound != nullptr) {
                    domain[1] = std::strtol(field->option_struct->upper_bound, nullptr, 10);
                }

                // If a user passed a tile extent use it
                if (field->option_struct->tile_extent != nullptr) {
                    int32_t tileExtent = std::strtol(field->option_struct->tile_extent, nullptr, 10);
                    return tiledb::Dimension::create<int32_t>(ctx, field->field_name.str, domain, tileExtent);
                }
                return tiledb::Dimension::create<int32_t>(ctx, field->field_name.str, domain);
            }
        }

        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG: {
            if (((Field_num *) field)->unsigned_flag) {
                std::array<uint64_t, 2> domain = {0, UINT64_MAX - 1};
                if (field->option_struct->lower_bound != nullptr) {
                    domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
                }
                if (field->option_struct->upper_bound != nullptr) {
                    domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
                }

                // If a user passed a tile extent use it
                if (field->option_struct->tile_extent != nullptr) {
                    uint64_t tileExtent = std::strtoul(field->option_struct->tile_extent, nullptr, 10);
                    return tiledb::Dimension::create<uint64_t>(ctx, field->field_name.str, domain, tileExtent);
                }
                return tiledb::Dimension::create<uint64_t>(ctx, field->field_name.str, domain);
            } else {
                std::array<int64_t, 2> domain = {INT64_MIN, INT64_MAX - 1};
                if (field->option_struct->lower_bound != nullptr) {
                    domain[0] = std::strtol(field->option_struct->lower_bound, nullptr, 10);
                }
                if (field->option_struct->upper_bound != nullptr) {
                    domain[1] = std::strtol(field->option_struct->upper_bound, nullptr, 10);
                }

                // If a user passed a tile extent use it
                if (field->option_struct->tile_extent != nullptr) {
                    int64_t tileExtent = std::strtol(field->option_struct->tile_extent, nullptr, 10);
                    return tiledb::Dimension::create<int64_t>(ctx, field->field_name.str, domain, tileExtent);
                }
                return tiledb::Dimension::create<int64_t>(ctx, field->field_name.str, domain);
            }
        }

        case MYSQL_TYPE_NULL:
        case MYSQL_TYPE_BIT: {
            std::array<uint8_t, 2> domain = {0, UINT8_MAX - 1};
            if (field->option_struct->lower_bound != nullptr) {
                domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
            }
            if (field->option_struct->upper_bound != nullptr) {
                domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
            }

            // If a user passed a tile extent use it
            if (field->option_struct->tile_extent != nullptr) {
                uint64_t tileExtent = std::strtoul(field->option_struct->tile_extent, nullptr, 10);
                return tiledb::Dimension::create<uint8_t>(ctx, field->field_name.str, domain, tileExtent);
            }
            return tiledb::Dimension::create<uint8_t>(ctx, field->field_name.str, domain);
        }

        case MYSQL_TYPE_VARCHAR :
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_SET: {
            sql_print_error("Varchar fields not supported for tiledb dimension fields");
            return tiledb::Dimension::create<uint8>(ctx, field->field_name.str, {0, 0});
        }

        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_ENUM: {
            sql_print_error("Blob or enum fields not supported for tiledb dimension fields");
            return tiledb::Dimension::create<uint8>(ctx, field->field_name.str, {0, 0});
        }
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_NEWDATE: {
            std::array<uint64_t, 2> domain = {0, UINT64_MAX - 1};
            if (field->option_struct->lower_bound != nullptr) {
                domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
            }
            if (field->option_struct->upper_bound != nullptr) {
                domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
            }

            // If a user passed a tile extent use it
            if (field->option_struct->tile_extent != nullptr) {
                uint64_t tileExtent = std::strtoul(field->option_struct->tile_extent, nullptr, 10);
                return tiledb::Dimension::create<uint64_t>(ctx, field->field_name.str, domain, tileExtent);
            }
            return tiledb::Dimension::create<uint64_t>(ctx, field->field_name.str, domain);
        }
        default: {
            sql_print_error("Unknown mysql data type in creating tiledb field attribute");
            return tiledb::Dimension::create<uint8>(ctx, field->field_name.str, {0,0});
        }
    }
}

std::unique_ptr<tile::buffer> tile::createBuffer(tiledb::Attribute attribute, size_t reserveSizeMB) {

    std::unique_ptr<tile::buffer> buffer = std::make_unique<tile::buffer>();
    buffer->datatype = attribute.type();
    if (attribute.variable_sized()) {
        buffer->offset_length = reserveSizeMB /  sizeof(uint64_t);
        buffer->offsets = new uint64_t[buffer->offset_length];
    }
    switch (attribute.type()) {
        case tiledb_datatype_t::TILEDB_INT8:
            buffer->values_length = reserveSizeMB / sizeof(int8_t);
            buffer->values = new int8_t[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_STRING_ASCII:
        case tiledb_datatype_t::TILEDB_STRING_UTF8:
        case tiledb_datatype_t::TILEDB_UINT8    :
            buffer->values_length = reserveSizeMB / sizeof(uint8_t);
            buffer->values = new uint8_t[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_INT16:
            buffer->values_length = reserveSizeMB / sizeof(int16_t);
            buffer->values = new int16_t[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_STRING_UTF16:
        case tiledb_datatype_t::TILEDB_STRING_UCS2:
        case tiledb_datatype_t::TILEDB_UINT16:
            buffer->values_length = reserveSizeMB / sizeof(uint16_t);
            buffer->values = new uint16_t[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_INT32:
            buffer->values_length = reserveSizeMB / sizeof(int32_t);
            buffer->values = new int32_t[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_STRING_UTF32:
        case tiledb_datatype_t::TILEDB_STRING_UCS4:
        case tiledb_datatype_t::TILEDB_UINT32:
            buffer->values_length = reserveSizeMB / sizeof(uint32_t);
            buffer->values = new uint32_t[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_INT64:
            buffer->values_length = reserveSizeMB / sizeof(int64_t);
            buffer->values = new int64_t[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_UINT64:
            buffer->values_length = reserveSizeMB / sizeof(uint64_t);
            buffer->values = new uint64_t[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_FLOAT32:
            buffer->values_length = reserveSizeMB / sizeof(float);
            buffer->values = new float[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_FLOAT64:
            buffer->values_length = reserveSizeMB / sizeof(double);
            buffer->values = new double[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_CHAR:
            buffer->values_length = reserveSizeMB / sizeof(char);
            buffer->values = new char[buffer->values_length];
            break;
        case tiledb_datatype_t::TILEDB_ANY:
            buffer->values_length = reserveSizeMB / sizeof(int8_t);
            buffer->values = new int8_t[buffer->values_length];
            break;
    }
    return buffer;
}

std::string tile::getTileDBString(tile::buffer *buffer, int64_t currentRowPosition) {
    std::string rowString;
    if (buffer->offsets != nullptr) {
        uint64_t endPosition = currentRowPosition + 1;
        // If its not the first value, we need to see where the previous position ended to know where to start.
        if (currentRowPosition > 0) {
            currentRowPosition = (uint64_t) buffer->offsets[currentRowPosition];
        }
        // If the current position is equal to the number of results - 1 then we are at the last varchar value
        if (currentRowPosition >= buffer->offset_length - 1) {
            endPosition = buffer->offset_length;
        } else { // Else read the end from the next offset.
            endPosition = (uint64_t) buffer->offsets[currentRowPosition + 1];
        }
        rowString = std::string(((char *) buffer->values) + currentRowPosition,
                                endPosition - currentRowPosition);
    } else {
        rowString = ((char *) buffer->values)[currentRowPosition];
    }
    return rowString;
}
