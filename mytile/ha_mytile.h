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
        ulonglong table_flags(void) const override;

        int create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) override;
        int delete_table(const char *name) override;
        int rename_table(const char *from, const char *to) override;
        int open(const char *name, int mode, uint test_if_locked) override;
        int close(void) override;

        /* Table Scaning */
        int rnd_init(bool scan) override;
        int rnd_next(uchar *buf) override;
        int rnd_pos(uchar * buf, uchar *pos) override;
        void position(const uchar *record) override;

        THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) override;


        int info(uint) override;
        ulong index_flags(uint inx, uint part, bool all_parts) const override;

    };
}
