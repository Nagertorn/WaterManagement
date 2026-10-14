#include <stdio.h>
#include <stdlib.h>
#include "db.h"

db_status_t init_db(const char *db_name, sqlite3 **db) {
	int result_code = sqlite3_open(db_name, db);

	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Cannot open databese:\n%s\n", sqlite3_errmsg(*db));
		return DB_ERR_CANNOT_OPEN;
	}

	const char* sql_create_ws_table =
		"CREATE TABLE IF NOT EXISTS watering_station (	"
		"	id TEXT PRIMARY KEY,			"
		"	location TEXT NOT NULL,			"
		"	status TEXT DEFAULT  'DESCONECTADA'	"
		");						";

	printf("[DB] Database initialized successfully (%s)\n", db_name);

	char* error_message = NULL;
	result_code = sqlite3_exec(*db, sql_create_ws_table, NULL, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Failed to create watering station table: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}
	fprintf(stdout, "[DB] Databese create watering station table successfully\n");

	return DB_SUCCESS;
}


void close_db(sqlite3 **db) {
	if(db) {
		if(sqlite3_close(*db)) {
			fprintf(stdout, "[DB] Databese closed successfully\n");
		} else {
			fprintf(stderr, "[DB ERROR] Databese closed unsuccessfully\n");
		}
	}
}	
