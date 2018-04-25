/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include <sql_plugin.h>
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

static PSI_mutex_info all_mytile_mutexes[]=
    {
        { &ex_key_mutex_mytile_share_mutex, "mytile_share::mutex", 0}
    };

static void init_mytile_psi_keys()
{
  const char* category= "mytile";
  int count;

  count= array_elements(all_mytile_mutexes);
  mysql_mutex_register(category, all_mytile_mutexes, count);
}
#endif

tile::mytile_share::mytile_share()
{
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

tile::mytile_share *tile::mytile::get_share()
{
  tile::mytile_share *tmp_share;

  DBUG_ENTER("tile::mytile::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= static_cast<tile::mytile_share*>(get_ha_share_ptr())))
  {
    tmp_share= new mytile_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
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

int tile::mytile::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) {
  DBUG_ENTER("tile::mytile::create");
  DBUG_RETURN(create_map(name, table_arg, create_info));
}
int tile::mytile::delete_table(const char *name){
  DBUG_ENTER("tile::mytile::delete_table");
  DBUG_RETURN(0);
}

int tile::mytile::rename_table(const char *from, const char *to){
  DBUG_ENTER("tile::mytile::rename_table");
  DBUG_RETURN(0);
}

int tile::mytile::open(const char *name, int mode, uint test_if_locked) {
  DBUG_ENTER("tile::mytile::open");
  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);
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
  mysql_mutex_lock(&share->mutex);
  DBUG_RETURN(0);
};
int tile::mytile::rnd_next(uchar *buf) {
  DBUG_ENTER("tile::mytile::rnd_next");
  DBUG_RETURN(0);
};
int tile::mytile::rnd_pos(uchar * buf, uchar *pos) {
  DBUG_ENTER("tile::mytile::rnd_pos");
  DBUG_RETURN(0);
};
void tile::mytile::position(const uchar *record) {
  DBUG_ENTER("tile::mytile::position");
  DBUG_VOID_RETURN;
};

int tile::mytile::rnd_end() {
  DBUG_ENTER("tile::mytile::rnd_end");
  // Unlock basic mutex
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}

THR_LOCK_DATA **tile::mytile::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
  DBUG_ENTER("tile::mytile::store_lock");
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  DBUG_RETURN(to);
};


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

ulonglong tile::mytile::table_flags(void) const {
  DBUG_ENTER("tile::mytile::table_flags");
  DBUG_RETURN(HA_REC_NOT_IN_SEQ | HA_CAN_GEOMETRY | HA_CAN_SQL_HANDLER | HA_NULL_IN_KEY
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