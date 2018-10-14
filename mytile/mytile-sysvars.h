/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#pragma once

#include <handler.h>

extern struct st_mysql_sys_var* mytile_system_variables[];

static MYSQL_THDVAR_ULONGLONG(read_buffer_size, PLUGIN_VAR_OPCMDARG,
                              "", NULL, NULL, 10485760, 0, ~0UL, 0);

static MYSQL_THDVAR_ULONGLONG(write_buffer_size, PLUGIN_VAR_OPCMDARG,
                              "", NULL, NULL, 10485760, 0, ~0UL, 0);
