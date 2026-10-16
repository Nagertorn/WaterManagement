#include <stdio.h>
#include <stdlib.h>
#include "db.h"

db_status_t init_db(const char *db_name, sqlite3 **db) {
	int result_code = sqlite3_open(db_name, db);

	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Cannot open databese:\n%s\n", sqlite3_errmsg(*db));
		return DB_ERR_CANNOT_OPEN;
	}


	// CREATE WS TABLE
	const char* sql_create_ws_table =
		"CREATE TABLE IF NOT EXISTS watering_station ("
		"	id TEXT PRIMARY KEY,"
		"	location TEXT NOT NULL,"
		"	status TEXT DEFAULT 'DESCONECTADA'"
		");";

	printf("[DB] Database initialized successfully (%s)\n", db_name);

	char* error_message = NULL;
	result_code = sqlite3_exec(*db, sql_create_ws_table, NULL, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Table creation unsuccesfully: watering station: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	fprintf(stdout, "[DB] Table creation succesfully: watering_station\n");

	// CREATE OPERATORS TABLE
	const char* sql_create_operators_table =
		"CREATE TABLE IF NOT EXISTS operators ("
		"	id TEXT PRIMARY KEY,"
		"	name TEXT"
		");";

	error_message = NULL;
	result_code = sqlite3_exec(*db, sql_create_operators_table, NULL, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Table creation unsuccesfully: operators: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	fprintf(stdout, "[DB] Table creation succesfully: operators\n");

	return DB_SUCCESS;
}

int callbackF(void *NotUsed, int argc, char **argv, char **azColName) {
	NotUsed = 0;
	for (int i = 0; i < argc; i++) {
		printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
	}
	printf("\n");
	return 0;
}

db_status_t read_watering_stations(sqlite3 *db) {
	const char* sql_query = "SELECT * FROM watering_station;";

	char* error_message = NULL;
	int result_code = sqlite3_exec(db, sql_query, callbackF, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Select: watering_stations: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	return DB_SUCCESS;
}

db_status_t create_watering_station(sqlite3 *db, const char *id, const char *location, const char *status) {
	char sql_query[256];
	snprintf(sql_query, sizeof(sql_query), "INSERT INTO watering_station (id, location, status) VALUES ('%s', '%s', '%s')", id, location, status);

	char* error_message = NULL;
	int result_code = sqlite3_exec(db, sql_query, NULL, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Select: watering_stations: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	return DB_SUCCESS;
}

void close_db(sqlite3 **db) {
	if(db) {
		int result_code = sqlite3_close(*db);
		if(result_code == SQLITE_OK) {
			fprintf(stdout, "[DB] Databese closed successfully\n");
		} else {
			fprintf(stderr, "[DB ERROR] Databese closed unsuccessfully\n");
		}
	}
}	
