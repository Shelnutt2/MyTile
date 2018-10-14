/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#pragma once

#include <tiledb/tiledb>

namespace tile {
    struct buffer {
        uint64_t* offsets;
        uint64_t offset_length;
        void* values;
        uint64_t values_length;
        tiledb_datatype_t datatype;
        bool isDimension;
        uint64_t bufferPositionOffset;
        std::string name;
        std::pair<uint64_t, uint64_t> resultBufferElement;
    };
}
