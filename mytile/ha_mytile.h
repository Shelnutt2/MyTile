/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#pragma once

#include <handler.h>

#define MYSQL_SERVER 1 // required for THD class

// Handler for mytile engine
extern handlerton *mytile_hton;
namespace tile {
/** @brief
  mytile_share is a class that will be shared among all open handlers.
  This mytile implements the minimum of what you will probably need.
*/
    class mytile_share : public Handler_share {
    public:
        mysql_mutex_t mutex;
        THR_LOCK lock;

        mytile_share();

        ~mytile_share() {
          thr_lock_delete(&lock);
          mysql_mutex_destroy(&mutex);
        }
    };


    class mytile : public handler {

        THR_LOCK_DATA lock;      ///< MySQL lock
        mytile_share *share;    ///< Shared lock info
        mytile_share *get_share(); ///< Get the share
    public:
        mytile(handlerton *hton, TABLE_SHARE *table_arg) : handler(hton, table_arg) {};

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

        int rnd_pos(uchar *buf, uchar *pos) override;

        int rnd_end() override;

        void position(const uchar *record) override;

        THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) override;


        int info(uint) override;

        ulong index_flags(uint inx, uint part, bool all_parts) const override;

    };
}
