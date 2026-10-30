#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "db.h"
#include "kafka_client.h"
#include "socket_server.h"

static pthread_mutex_t core_mutex = PTHREAD_MUTEX_INITIALIZER;
static sqlite3* db = NULL;

void on_kafka_event_received(const char *topic, const char *payload) {
	printf("[CENTRAL CORE] Received from topic '%s': %s\n", topic, payload);

	pthread_mutex_lock(&core_mutex);

	printf("Payload: %s", payload);

	pthread_mutex_unlock(&core_mutex);
}

void on_socket_event_received(int client_fd, const char *message) {
    printf("[CENTRAL SOCKET] Received from fd %d: %s\n", client_fd, message);

    char command[32] = {0};
    char ws_id[32] = {0};
    char payload[128] = {0};

    // Parsea comandos formateados como "COMMAND#WS_ID#PAYLOAD" (ej: REGISTER#1#Parque_Central)
    if (sscanf(message, "%31[^#]#%31[^#]#%127s", command, ws_id, payload) >= 2) {
        
        pthread_mutex_lock(&core_mutex);

        if (strcmp(command, "REGISTER") == 0) {
            create_watering_station(db, ws_id, payload, "DISPONIBLE");
            send_network_message(client_fd, "ACK#REGISTERED\n");

        } else if (strcmp(command, "STATUS") == 0) {
            update_watering_station_status(db, ws_id, payload);
            send_network_message(client_fd, "ACK#STATUS_UPDATED\n");
        } else {
            send_network_message(client_fd, "NACK#UNKNOWN_COMMAND\n");
        }

        pthread_mutex_unlock(&core_mutex);
    } else {
        send_network_message(client_fd, "NACK#INVALID_FORMAT\n");
    }
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

	socket_server_t sock_server;
    if (start_socket_server(&sock_server, port_monitors, on_socket_event_received) != SOCKET_SUCCESS) {
        fprintf(stderr, "[MAIN ERROR] Failed to start socket server on port %d\n", port_monitors);
        close_kafka(&kafka_ctx);
        close_db(&db);
        return EXIT_FAILURE;
    }

	db_response = create_watering_station(db, "1", "A", "D");
	db_response = create_watering_station(db, "2", "B", "D");
	db_response = create_watering_station(db, "3", "C", "A");
	db_response = update_watering_station_status(db, "1", "Actualizado");

	db_response = create_operator(db, "1", "Javier");
	db_response = create_operator(db, "2", "Paco");
	db_response = create_operator(db, "3", "Amadeus");

	while (1) {
		fflush(stdout);
		sleep(1);
	}

	stop_socket_server(&sock_server);
    close_kafka(&kafka_ctx);
	close_db(&db);

	return EXIT_SUCCESS;

}
