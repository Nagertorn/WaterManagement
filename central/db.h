#ifndef DB_H
#define DB_H

#include <sqlite3.h>

typedef enum {
    DB_SUCCESS = 0,
    DB_ERR_CANNOT_OPEN,
    DB_ERR_CREATE_TABLE,
    DB_ERR_QUERY
} db_status_t;

db_status_t init_db(const char *db_name, sqlite3 **db);

db_status_t load_registered_stations(sqlite3 *db);

void close_db(sqlite3 **db);

#endif
