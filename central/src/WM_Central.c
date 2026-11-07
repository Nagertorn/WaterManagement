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

typedef struct {
    int fd;
    char ws_id[32];
} active_client_t;

static active_client_t active_clients[64];
static int num_active_clients = 0;

void register_client_fd(int fd, const char *ws_id) {
    for (int i = 0; i < num_active_clients; i++) {
        if (active_clients[i].fd == fd) {
            strncpy(active_clients[i].ws_id, ws_id, sizeof(active_clients[i].ws_id) - 1);
            return;
        }
    }
    if (num_active_clients < 64) {
        active_clients[num_active_clients].fd = fd;
        strncpy(active_clients[num_active_clients].ws_id, ws_id, sizeof(active_clients[num_active_clients].ws_id) - 1);
        num_active_clients++;
    }
}

void handle_client_disconnect(int fd) {
    pthread_mutex_lock(&core_mutex);
    for (int i = 0; i < num_active_clients; i++) {
        if (active_clients[i].fd == fd) {
            printf("[CENTRAL] Estación '%s' desconectada (fd: %d). Actualizando BD...\n", active_clients[i].ws_id, fd);
            update_watering_station_status(db, active_clients[i].ws_id, "DESCONECTADA");
            
            active_clients[i] = active_clients[num_active_clients - 1];
            num_active_clients--;
            break;
        }
    }
    pthread_mutex_unlock(&core_mutex);
}

bool is_ws_id_connected(const char *ws_id) {
    for (int i = 0; i < num_active_clients; i++) {
        if (strcmp(active_clients[i].ws_id, ws_id) == 0) {
            return true;
        }
    }
    return false;
}

void on_kafka_event_received(const char *topic, const char *payload) {
	printf("[CENTRAL KAFKA] Received on '%s': %s\n", topic, payload);

	pthread_mutex_lock(&core_mutex);

	if (strcmp(topic, "central_requests") == 0) {
		char command[32] = {0};
		char ws_id[32] = {0};
		char op_id[32] = {0};
		int duration = 0;

		if (sscanf(payload, "%31[^#]#%31[^#]#%31[^#]#%d", command, ws_id, op_id, &duration) >= 3) {
			if (strcmp(command, "OPERATOR_WATER_REQUEST") == 0) {
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

					update_watering_station_status(db, ws_id, "REGANDO");

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

				char op_telem[256];
				snprintf(op_telem, sizeof(op_telem), "RESPONSE#ALL#%s#TELEMETRY#Caudal:%dL/min_Vol:%dL\n", ws_id, flow, vol);
				kafka_send_message(&kafka_ctx, "operator_responses", op_telem);

			} else if (strcmp(event_type, "END_WATERING") == 0) {
				int total_vol = flow;
				int total_duration = vol;

				printf("\n=======================================================\n");
				printf("[CENTRAL RESUMEN RIEGO] Estación: %s | Duración: %d s | Vol Total: %d L\n", ws_id, total_duration, total_vol);
				printf("=======================================================\n\n");

				update_watering_station_status(db, ws_id, "DISPONIBLE");

				char op_resp[256];
				snprintf(op_resp, sizeof(op_resp), "RESPONSE#ALL#%s#FINISHED#Volumen_Total:%dL_Duracion:%ds\n", ws_id, total_vol, total_duration);
				kafka_send_message(&kafka_ctx, "operator_responses", op_resp);
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
			if (is_ws_id_connected(ws_id)) {
				printf("[CENTRAL ERROR] Intento de registro duplicado rechazado para '%s' (fd: %d)\n", ws_id, client_fd);
				send_network_message(client_fd, "NACK#ALREADY_REGISTERED\n");
				pthread_mutex_unlock(&core_mutex);
				close(client_fd);
				return;
			}
			register_client_fd(client_fd, ws_id);
			create_watering_station(db, ws_id, payload, "DISPONIBLE");
			send_network_message(client_fd, "ACK#REGISTERED\n");

		} else if (strcmp(command, "STATUS") == 0) {
			update_watering_station_status(db, ws_id, payload);
			send_network_message(client_fd, "ACK#STATUS_UPDATED\n");

		} else if (strcmp(command, "LOCAL_WATER_REQUEST") == 0) {
			int duration = atoi(payload);
			char location[128] = {0};
			char status[32] = {0};

			db_status_t db_res = find_watering_station_by_id(db, ws_id, location, status);
			bool is_available = (db_res == DB_SUCCESS && strcmp(status, "DISPONIBLE") == 0);

			if (is_available) {
				printf("[CENTRAL] Autorizando riego LOCAL en %s por %d segundos\n", ws_id, duration);
				
				update_watering_station_status(db, ws_id, "REGANDO");

				char cmd_msg[256];
				snprintf(cmd_msg, sizeof(cmd_msg), "START#%s#%d\n", ws_id, duration);
				kafka_send_message(&kafka_ctx, "station_commands", cmd_msg);

				send_network_message(client_fd, "ACK#LOCAL_WATER_AUTHORIZED\n");

			} else {
				printf("[CENTRAL] Denegando riego LOCAL en %s [Estado actual: %s]\n", 
						ws_id, (db_res == DB_SUCCESS) ? status : "NO_ENCONTRADA");
				send_network_message(client_fd, "NACK#UNAVAILABLE\n");
			}

		} else if (strcmp(command, "LOCAL_STOP_REQUEST") == 0) {
			printf("[CENTRAL] Orden de parada manual recibida para %s desde su Monitor\n", ws_id);
			char cmd_msg[256];
			snprintf(cmd_msg, sizeof(cmd_msg), "STOP#%s#0\n", ws_id);
			kafka_send_message(&kafka_ctx, "station_commands", cmd_msg);
			send_network_message(client_fd, "ACK#STOP_SENT\n");
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

		start_kafka_consumer(&kafka_ctx, "central_events,central_requests", on_kafka_event_received);
		printf("[CENTRAL KAFKA] Listening on 'central_events' and 'central_requests'\n");
	}

	socket_server_t sock_server;
	if (start_socket_server(&sock_server, port_monitors, on_socket_event_received, handle_client_disconnect) != SOCKET_SUCCESS) {
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
