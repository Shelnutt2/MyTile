/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "utils.h"

#include <dirent.h>
#include <cstring>
#include <sys/stat.h>
#include <unistd.h>

/**
 * Delete a directory recursively
 * @param pathString
 * @return
 */
int tile::removeDirectory(std::string pathString) {
  const char *path = pathString.c_str();
  DIR *d = opendir(path);
  size_t path_len = strlen(path);
  int r = -1;

  if (d) {
    struct dirent *p;

    r = 0;

    while (!r && (p = readdir(d))) {
      int r2 = -2;
      char *buf;
      size_t len;

      /* Skip the names "." and ".." as we don't want to recurse on them. */
      if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
        continue;
      }

      len = path_len + strlen(p->d_name) + 2;
      buf = (char *) malloc(len);

      if (buf) {
        struct stat statbuf;

        snprintf(buf, len, "%s/%s", path, p->d_name);

        if (!lstat(buf, &statbuf)) {
          if (S_ISDIR(statbuf.st_mode)) {
            r2 = removeDirectory(buf);
          } else {
            r2 = unlink(buf);
          }
        }
        free(buf);
      }
      r = r2;
    }
    closedir(d);
  }

  if (!r) {
    r = rmdir(path);
  }
  return r;
}