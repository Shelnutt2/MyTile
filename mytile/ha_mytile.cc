/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include <sql_plugin.h>
#include "ha_mytile.h"
#include "mytile.h"

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