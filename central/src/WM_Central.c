#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include "db.h"
#include "kafka_client.h"
#include "socket_server.h"

static pthread_mutex_t core_mutex = PTHREAD_MUTEX_INITIALIZER;
static sqlite3* db = NULL;
static kafka_context_t kafka_ctx;

void on_kafka_event_received(const char *topic, const char *payload) {
	printf("[CENTRAL KAFKA] Received on '%s': %s\n", topic, payload);

	pthread_mutex_lock(&core_mutex);

	if (strcmp(topic, "central_requests") == 0) {
		char command[32] = {0};
		char ws_id[32] = {0};
		char op_id[32] = {0};
		int duration = 0;

		if (sscanf(payload, "%31[^#]#%31[^#]#%31[^#]#%d", command, ws_id, op_id, &duration) >= 3) {
			if (strcmp(command, "REQUEST_WATER") == 0) {
				if (find_operator_by_id(db, op_id) != DB_SUCCESS) {
					printf("[CENTRAL ERROR] Operario '%s' no encontrado en BD\n", op_id);
					char op_resp[256];
					snprintf(op_resp, sizeof(op_resp), "RESPONSE#%s#%s#DENIED#UNAUTHORIZED_OPERATOR\n", op_id, ws_id);
					kafka_send_message(&kafka_ctx, "operator_responses", op_resp);
					pthread_mutex_unlock(&core_mutex);
					return;
				}

				char location[128] = {0};
				char status[32] = {0};

				db_status_t db_res = find_watering_station_by_id(db, ws_id, location, status);

				bool is_available = (db_res == DB_SUCCESS && strcmp(status, "DISPONIBLE") == 0);

				if (is_available) {
					printf("[CENTRAL] Autorizando riego para operario %s en %s (%s) [Estado: %s]\n", 
							op_id, ws_id, location, status);

					char op_resp[256];
					snprintf(op_resp, sizeof(op_resp), "RESPONSE#%s#%s#AUTHORIZED#OK\n", op_id, ws_id);
					kafka_send_message(&kafka_ctx, "operator_responses", op_resp);

					char cmd_msg[256];
					snprintf(cmd_msg, sizeof(cmd_msg), "START#%s#%d\n", ws_id, duration);
					kafka_send_message(&kafka_ctx, "station_commands", cmd_msg);

				} else {
					printf("[CENTRAL] Denegando riego para operario %s en %s [Estado actual: %s]\n", 
							op_id, ws_id, (db_res == DB_SUCCESS) ? status : "NO_ENCONTRADA");

					char op_resp[256];
					snprintf(op_resp, sizeof(op_resp), "RESPONSE#%s#%s#DENIED#UNAVAILABLE\n", op_id, ws_id);
					kafka_send_message(&kafka_ctx, "operator_responses", op_resp);
				}
			}
		}
	} 
	else if (strcmp(topic, "central_events") == 0) {
		char event_type[32] = {0};
		char ws_id[32] = {0};
		int flow = 0, vol = 0;

		if (sscanf(payload, "%31[^#]#%31[^#]#%d#%d", event_type, ws_id, &flow, &vol) == 4) {
			if (strcmp(event_type, "TELEMETRY") == 0) {
				printf("[CENTRAL TELEMETRÍA] %s -> Caudal: %d L/min | Acumulado: %d L\n", ws_id, flow, vol);
			}
		}
	}

	pthread_mutex_unlock(&core_mutex);
}

void on_socket_event_received(int client_fd, const char *message) {
	printf("[CENTRAL SOCKET] Received from fd %d: %s\n", client_fd, message);

	char command[32] = {0};
	char ws_id[32] = {0};
	char payload[128] = {0};

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

	if (argc != 3) {
		fprintf(stderr, "Uso: %s <port_monitors> <kafka_server>\n", argv[0]);
		exit(1);
	}

	int port_monitors = atoi(argv[1]);
	char* kafka_server = argv[2];

	db_status_t db_response = init_db("data/water_managment.db", &db); 
	if (db_response != DB_SUCCESS) {
		fprintf(stderr, "[MAIN ERROR] Database init failed\n");
		return EXIT_FAILURE;
	}

	mark_all_stations_disconnected(db);

	if (init_kafka(&kafka_ctx, kafka_server, "wm_central_group") == KAFKA_SUCCESS) {
		kafka_send_message(&kafka_ctx, "central_events", "INIT#CENTRAL");
		kafka_send_message(&kafka_ctx, "central_requests", "INIT#CENTRAL");
		kafka_send_message(&kafka_ctx, "station_commands", "INIT#CENTRAL");
		kafka_send_message(&kafka_ctx, "operator_responses", "INIT#CENTRAL");

		sleep(1);

		start_kafka_consumer(&kafka_ctx, "central_events", on_kafka_event_received);
		start_kafka_consumer(&kafka_ctx, "central_requests", on_kafka_event_received);
		printf("[CENTRAL KAFKA] Listening on 'central_events' and 'central_requests'\n");
	}

	socket_server_t sock_server;
	if (start_socket_server(&sock_server, port_monitors, on_socket_event_received) != SOCKET_SUCCESS) {
		fprintf(stderr, "[MAIN ERROR] Failed to start socket server on port %d\n", port_monitors);
		close_kafka(&kafka_ctx);
		close_db(&db);
		return EXIT_FAILURE;
	}

	while (1) {
		fflush(stdout);
		sleep(1);
	}

	stop_socket_server(&sock_server);
	close_kafka(&kafka_ctx);
	close_db(&db);

	return EXIT_SUCCESS;
}
