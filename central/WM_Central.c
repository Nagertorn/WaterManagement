#include <stdio.h>
#include <stdlib.h>
#include "db.h"

int main(int argc, char* argv[]) {

	if (argc != 4) {
		fprintf(stderr, "Use: %s <port_monitors> <ip_kafka> <port_kafka>\n", argv[0]);
		exit(1);
	}

	int port_monitors = atoi(argv[1]);
	char* ip_kafka = argv[2];
	int port_kafka = atoi(argv[3]);


	printf("port monitros: %d\n", port_monitors);
	printf("ip kafka: %s\n", ip_kafka);
	printf("port kafka: %d\n", port_kafka);


	sqlite3* db = NULL;

	db_status_t db_response = init_db("water_managment.db", &db);
	if (db_response != DB_SUCCESS) {
		fprintf(stderr, "[MAIN ERROR] Database initialization failed with status code: %d\n", db_response);
		return EXIT_FAILURE;
	}

	close_db(&db);

	return EXIT_SUCCESS;

}
