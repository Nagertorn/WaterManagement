#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "db.h"
#include "kafka_client.h"

void on_kafka_event_received(const char *topic, const char *payload) {
    printf("[CENTRAL CORE] Received from topic '%s': %s\n", topic, payload);

    //pthread_mutex_lock(&core_mutex);
    //pthread_mutex_unlock(&core_mutex);
}

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

	char kafka_server[128];
    snprintf(kafka_server, sizeof(kafka_server), "%s:%s", argv[2], argv[3]);

	sqlite3* db = NULL;
	db_status_t db_response = init_db("data/water_managment.db", &db);

	switch (db_response) {
		case DB_SUCCESS:
			break;

		case DB_ERR_CREATE_TABLE:
			fprintf(stderr, "[MAIN ERROR] Database creation failed with status code: %d\n", db_response);
			return EXIT_FAILURE;

		default:
			fprintf(stderr, "[MAIN ERROR] Database failed with status code: %d\n", db_response);
			return EXIT_FAILURE;
	}

    kafka_context_t kafka_ctx;
    if (init_kafka(&kafka_ctx, kafka_server, "wm_central_group") == KAFKA_SUCCESS) {
        kafka_send_message(&kafka_ctx, "central_events", "WM_Central is ONLINE");
	start_kafka_consumer(&kafka_ctx, "central_events", on_kafka_event_received);
    }

	db_response = create_watering_station(db, "1", "A", "D");
	db_response = create_watering_station(db, "2", "B", "D");
	db_response = create_watering_station(db, "3", "C", "A");
	db_response = read_watering_stations(db);
	db_response = update_watering_station_status(db, "1", "Actualizado");
	db_response = read_watering_stations(db);

	db_response = create_operator(db, "1", "Javier");
	db_response = create_operator(db, "2", "Paco");
	db_response = create_operator(db, "3", "Amadeus");
	db_response = read_operators(db);

	while (1) {
		sleep(1);
	}

	close_db(&db);

	return EXIT_SUCCESS;

}
