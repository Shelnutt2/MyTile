/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include <my_global.h>
#include "mytile-sysvars.h"

struct st_mysql_sys_var* mytile_system_variables[]= {
    MYSQL_SYSVAR(read_buffer_size),
    MYSQL_SYSVAR(write_buffer_size),
    NULL
};