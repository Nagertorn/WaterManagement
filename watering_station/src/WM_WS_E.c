#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include "kafka_client.h"
#include "socket_client.h"

static kafka_context_t kafka_ctx;
static socket_client_t monitor_client;
static int is_running = 1;
static int is_watering = 0;
static char ws_id[32] = {0};

static pthread_t timer_thread;
static time_t start_time = 0;
static int current_vol = 0;

typedef struct {
	int duration;
} watering_timer_args_t;

static void* watering_timer_loop(void *arg) {
	watering_timer_args_t *args = (watering_timer_args_t*)arg;
	int duration = args->duration;
	free(args);

	printf("\n[ENGINE] >>> Electrovalvula ABIERTA (%d seg) para %s <<<\n", duration, ws_id);
	is_watering = 1;
	start_time = time(NULL);

	sleep(duration);

	if (is_watering) {
		is_watering = 0;
		int real_duration = (int)difftime(time(NULL), start_time);
		printf("\n[ENGINE] >>> Temporizador cumplido (%d seg). Electrovalvula CERRADA para %s <<<\n", real_duration, ws_id);

		char finish_msg[256];
		snprintf(finish_msg, sizeof(finish_msg), "END_WATERING#%s#%d#%d\n", ws_id, current_vol, real_duration);
		kafka_send_message(&kafka_ctx, "central_events", finish_msg);
	}
	return NULL;
}

void on_kafka_command_received(const char *topic, const char *payload) {
	(void)topic;
	char command[32] = {0};
	char target_id[32] = {0};
	int duration = 0;

	if (strlen(ws_id) == 0) return;

	if (sscanf(payload, "%31[^#]#%31[^#]#%d", command, target_id, &duration) >= 2) {
		if (strcmp(target_id, ws_id) == 0) {
			if (strcmp(command, "START") == 0) {
				if (is_watering) {
					printf("[ENGINE] Estación %s ya se encuentra regando. Ignorando nueva orden.\n", ws_id);
					return;
				}
				current_vol = 0;
				watering_timer_args_t *args = malloc(sizeof(watering_timer_args_t));
				if (args) {
					args->duration = duration;
					pthread_create(&timer_thread, NULL, watering_timer_loop, args);
					pthread_detach(timer_thread);
				}
			} else if (strcmp(command, "STOP") == 0) {
				if (is_watering) {
					is_watering = 0;
					int real_duration = (int)difftime(time(NULL), start_time);
					printf("\n[ENGINE] >>> Parada manual/forzada. Electrovalvula CERRADA para %s <<<\n", ws_id);

					char finish_msg[256];
					snprintf(finish_msg, sizeof(finish_msg), "END_WATERING#%s#%d#%d\n", ws_id, current_vol, real_duration);
					kafka_send_message(&kafka_ctx, "central_events", finish_msg);
				}
			}
		}
	}
}

static void* watering_telemetry_loop(void *arg) {
	setbuf(stdout, NULL);
	(void)arg;
	int flow_rate = 12;

	while (is_running) {
		if (is_watering && strlen(ws_id) > 0) {
			current_vol += flow_rate;

			char telemetry_msg[256];
			snprintf(telemetry_msg, sizeof(telemetry_msg), "TELEMETRY#%s#%d#%d\n", ws_id, flow_rate, current_vol);

			kafka_send_message(&kafka_ctx, "central_events", telemetry_msg);
			printf("[ENGINE TELEMETRIA] Caudal: %d L/min | Acumulado: %d L\n", flow_rate, current_vol);
		}
		sleep(1);
	}
	return NULL;
}

int main(int argc, char *argv[]) {
	if (argc != 3) {
		fprintf(stderr, "Uso: %s <bootstrap_servers_kafka> <ip_puerto_monitor>\n", argv[0]);
		return EXIT_FAILURE;
	}

	char *kafka_servers = argv[1];
	char *monitor_addr = argv[2];

	printf("[ENGINE] Iniciando WS Engine...\n");

	char mon_ip[64] = {0};
	int mon_port = 0;
	sscanf(monitor_addr, "%64[^:]:%d", mon_ip, &mon_port);

	if (socket_client_connect(&monitor_client, mon_ip, mon_port) == 0) {
		printf("[ENGINE] Conectado al Monitor local (%s:%d)\n", mon_ip, mon_port);

		socket_client_send(&monitor_client, "GET_ID\n");
		char response[128] = {0};
		if (socket_client_receive(&monitor_client, response, sizeof(response)) > 0) {
			if (sscanf(response, "ID#%31s", ws_id) == 1) {
				printf("[ENGINE] Identificador asignado por Monitor: %s\n", ws_id);
			}
		}
	} else {
		fprintf(stderr, "[ENGINE ERROR] No se pudo conectar al Monitor local.\n");
		return EXIT_FAILURE;
	}

	char group_id[64];
	snprintf(group_id, sizeof(group_id), "engine_group_%s", ws_id);

	if (init_kafka(&kafka_ctx, kafka_servers, group_id) == KAFKA_SUCCESS) {
		start_kafka_consumer(&kafka_ctx, "station_commands", on_kafka_command_received);
		printf("[ENGINE] Suscrito a Kafka (topic: 'station_commands')\n");
	} else {
		fprintf(stderr, "[ENGINE ERROR] Fallo al conectar con Kafka\n");
		socket_client_close(&monitor_client);
		return EXIT_FAILURE;
	}

	pthread_t telemetry_thread;
	pthread_create(&telemetry_thread, NULL, watering_telemetry_loop, NULL);
	pthread_detach(telemetry_thread);

	while (is_running) {
		sleep(1);
	}

	close_kafka(&kafka_ctx);
	socket_client_close(&monitor_client);
	return EXIT_SUCCESS;
}
