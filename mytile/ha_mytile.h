/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#pragma once

#include <handler.h>

#define MYSQL_SERVER 1 // required for THD class

// Handler for mytile engine
extern handlerton *mytile_hton;

namespace tile {
    class mytile : public handler {
    public:
        mytile(handlerton *hton, TABLE_SHARE *table_arg ): handler(hton, table_arg ){};

        ~mytile() noexcept(true) {};
        int create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info);
        int delete_table(const char *name);
        int rename_table(const char *from, const char *to);
        ulonglong table_flags(void) const;
    };
}
