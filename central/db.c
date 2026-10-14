#include <stdio.h>
#include <stdlib.h>
#include "db.h"

db_status_t init_db(const char *db_name, sqlite3 **db) {
	int result_code = sqlite3_open(db_name, db);

	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Cannot open databese:\n%s\n", sqlite3_errmsg(*db));
		return DB_ERR_CANNOT_OPEN;
	}

	const char* sql_ws =
		"CREATE TABLE IF NOT EXISTS watering_station (	"
		"	id TEXT PRIMARY KEY,			"
		"	location TEXT NOT NULL,			"
		"	status TEXT DEFAULT  'DESCONECTADA'	"
		");						";

	printf("[DB] Database initialized successfully (%s)\n", db_name);
	return DB_SUCCESS;
}


void close_db(sqlite3 **db) {
	if(db) {
		if(sqlite3_close(db)) {
			fprintf(stdout, "[DB] Databese closed successfully");
		} else {
			fprintf(stderr, "[DB ERROR] Databese closed unsuccessfully");
		}
	}
}	
