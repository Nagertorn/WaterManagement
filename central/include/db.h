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

db_status_t read_watering_stations(sqlite3 *db);

db_status_t create_watering_station(sqlite3 *db, const char *id, const char *location, const char *status);

db_status_t update_watering_station_status(sqlite3 *db, const char *id, const char *status);

db_status_t read_operators(sqlite3 *db);

db_status_t create_operator(sqlite3 *db, const char *id, const char *name);

db_status_t find_watering_station_by_id(sqlite3 *db, const char *id, char *location_out, char *status_out);

db_status_t find_operator_by_id(sqlite3 *db, const char *id);

void close_db(sqlite3 **db);

#endif
