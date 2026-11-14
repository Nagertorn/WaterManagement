#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
		"CREATE TABLE IF NOT EXISTS operator ("
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

	// INSERT DEFAULT OPERATORS
	error_message = NULL;
	const char* sql_seed_operators =
		"INSERT OR IGNORE INTO operator (id, name) VALUES "
		"('FO-01', 'Operario 01'), "
		"('FO-02', 'Operario 02');";

	result_code = sqlite3_exec(*db, sql_seed_operators, NULL, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Table insertion unsuccesfully: operators: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_QUERY;
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
		fprintf(stderr, "[DB ERROR] Select: watering_station: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	return DB_SUCCESS;
}

db_status_t create_watering_station(sqlite3 *db, const char *id, const char *location, const char *status) {
	char sql_query[256];
	snprintf(sql_query, sizeof(sql_query),
			"INSERT INTO watering_station (id, location, status) VALUES ('%s', '%s', '%s') "
			"ON CONFLICT(id) DO UPDATE SET status = '%s', location = '%s';",
			id, location, status, status, location);

	char* error_message = NULL;
	int result_code = sqlite3_exec(db, sql_query, NULL, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Insert: watering_station: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	return DB_SUCCESS;
}

db_status_t update_watering_station_status(sqlite3 *db, const char *id, const char *status) {
	char sql_query[256];
	snprintf(sql_query, sizeof(sql_query), "UPDATE watering_station SET status = '%s' WHERE id = '%s'", status, id);

	char* error_message = NULL;
	int result_code = sqlite3_exec(db, sql_query, NULL, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Update: watering_station: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	return DB_SUCCESS;
}

db_status_t read_operators(sqlite3 *db) {
	const char* sql_query = "SELECT * FROM operator;";

	char* error_message = NULL;
	int result_code = sqlite3_exec(db, sql_query, callbackF, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Select: operator: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	return DB_SUCCESS;
}

db_status_t create_operator(sqlite3 *db, const char *id, const char *name) {
	char sql_query[256];
	snprintf(sql_query, sizeof(sql_query), "INSERT INTO operator (id, name) VALUES ('%s', '%s')", id, name);

	char* error_message = NULL;
	int result_code = sqlite3_exec(db, sql_query, NULL, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Insert: operator: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	return DB_SUCCESS;
}

db_status_t find_watering_station_by_id(sqlite3 *db, const char *id, char *location_out, char *status_out) {
	char sql_query[256];
	snprintf(sql_query, sizeof(sql_query), "SELECT location, status FROM watering_station WHERE id = '%s'", id);

	sqlite3_stmt *stmt;
	int result_code = sqlite3_prepare_v2(db, sql_query, -1, &stmt, NULL);

	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Select by ID: watering_station: \n%s\n", sqlite3_errmsg(db));
		return DB_ERR_CANNOT_OPEN;
	}

	db_status_t status = DB_ERR_CANNOT_OPEN;

	if (sqlite3_step(stmt) == SQLITE_ROW) {
		const unsigned char *location_val = sqlite3_column_text(stmt, 0);
		const unsigned char *status_val = sqlite3_column_text(stmt, 1);

		if (location_out && location_val) {
			strcpy(location_out, (const char *)location_val);
		}
		if (status_out && status_val) {
			strcpy(status_out, (const char *)status_val);
		}

		status = DB_SUCCESS;
	} else {
		fprintf(stderr, "[DB] Station ID '%s' not found\n", id);
	}

	sqlite3_finalize(stmt);
	return status;
}

db_status_t find_operator_by_id(sqlite3 *db, const char *id) {
	if (!db || !id) return DB_ERR_CANNOT_OPEN;

	char sql_query[256];
	snprintf(sql_query, sizeof(sql_query), "SELECT id FROM operator WHERE id = '%s'", id);

	sqlite3_stmt *stmt;
	int result_code = sqlite3_prepare_v2(db, sql_query, -1, &stmt, NULL);

	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Select by ID: operator: \n%s\n", sqlite3_errmsg(db));
		return DB_ERR_CANNOT_OPEN;
	}

	db_status_t status = DB_ERR_CANNOT_OPEN; // O tu enum para 'NO ENCONTRADO'

	if (sqlite3_step(stmt) == SQLITE_ROW) {
		status = DB_SUCCESS; // El operario existe en la BD
	} else {
		fprintf(stderr, "[DB] Operator ID '%s' not found\n", id);
	}

	sqlite3_finalize(stmt);
	return status;
}

db_status_t mark_all_stations_disconnected(sqlite3 *db) {
	if (!db) return DB_ERR_CANNOT_OPEN;

	const char *sql_query = "UPDATE watering_station SET status = 'DESCONECTADA';";
	char *error_message = NULL;

	int result_code = sqlite3_exec(db, sql_query, NULL, NULL, &error_message);
	if (result_code != SQLITE_OK) {
		fprintf(stderr, "[DB ERROR] Update all DESCONECTADA: \n%s\n", error_message);
		sqlite3_free(error_message);
		return DB_ERR_CREATE_TABLE;
	}

	printf("[DB] Todas las estaciones marcadas como DESCONECTADA al arrancar\n");
	return DB_SUCCESS;
}

db_status_t get_available_stations_string(sqlite3 *db, char *buffer_out, size_t max_len) {
	if (!db || !buffer_out) return DB_ERR_CANNOT_OPEN;

	sqlite3_stmt *stmt;
	const char *sql_query = "SELECT id FROM watering_station WHERE status = 'DISPONIBLE';";

	if (sqlite3_prepare_v2(db, sql_query, -1, &stmt, NULL) != SQLITE_OK) {
		return DB_ERR_CANNOT_OPEN;
	}

	buffer_out[0] = '\0';
	int first = 1;

	while (sqlite3_step(stmt) == SQLITE_ROW) {
		const char *id = (const char*)sqlite3_column_text(stmt, 0);

		char temp[128];
		if (first) {
			snprintf(temp, sizeof(temp), "%s", id);
			first = 0;
		} else {
			snprintf(temp, sizeof(temp), ", %s", id);
		}
		strncat(buffer_out, temp, max_len - strlen(buffer_out) - 1);
	}

	sqlite3_finalize(stmt);
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
