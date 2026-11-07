#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "socket_client.h"
#include "socket_server.h"

static socket_client_t central_client;
static socket_server_t engine_server;
static int is_running = 1;
static char ws_id[32];

static int current_engine_fd = -1;
static pthread_mutex_t engine_fd_mutex = PTHREAD_MUTEX_INITIALIZER;

static int is_in_leak = 0;

static void notify_central_status(const char *status) {
	char msg[256], resp[128];
	snprintf(msg, sizeof(msg), "STATUS#%s#%s\n", ws_id, status);
	printf("[MONITOR] Notificando estado '%s' a Central...\n", status);
	socket_client_send(&central_client, msg);
	if (socket_client_receive(&central_client, resp, sizeof(resp)) > 0) {
		printf("[CENTRAL RESPONSE] %s\n", resp);
	}
}

static void* health_checker_thread(void *arg) {
	(void)arg;
	char resp[128];

	while (is_running) {
		pthread_mutex_lock(&engine_fd_mutex);
		int fd = current_engine_fd;
		pthread_mutex_unlock(&engine_fd_mutex);

		if (fd > 0) {
			if (send_network_message(fd, "PING\n") < 0) {
				printf("\n[MONITOR ALERTA] Error al enviar PING al Engine (fd %d). Notificando LEAK...\n", fd);
				notify_central_status("LEAK");
				pthread_mutex_lock(&engine_fd_mutex);
				current_engine_fd = -1;
				pthread_mutex_unlock(&engine_fd_mutex);
			} else {
				int bytes = read_network_message(fd, resp, sizeof(resp));
				
				if (bytes <= 0) {
					if (!is_in_leak) {
						printf("\n[MONITOR ALERTA] Conexión con el Engine perdida. Notificando LEAK...\n");
						notify_central_status("LEAK");
						is_in_leak = 1;
					}
					pthread_mutex_lock(&engine_fd_mutex);
					current_engine_fd = -1;
					pthread_mutex_unlock(&engine_fd_mutex);
				} else if (strstr(resp, "KO") != NULL) {
					if (!is_in_leak) {
						printf("\n[MONITOR ALERTA] El Engine ha respondido KO. Notificando LEAK...\n");
						notify_central_status("LEAK");
						is_in_leak = 1;
					}
				} else {
					if (is_in_leak) {
						printf("\n[MONITOR] El Engine se ha recuperado. Notificando DISPONIBLE...\n");
						notify_central_status("DISPONIBLE");
						is_in_leak = 0; 
					}
				}
			}
		}
		sleep(1);
	}
	return NULL;
}

void on_engine_message_received(int client_fd, const char *message) {
	if (strcmp(message, "GET_ID") == 0) {
		pthread_mutex_lock(&engine_fd_mutex);
		current_engine_fd = client_fd;
		pthread_mutex_unlock(&engine_fd_mutex);

		char resp[64];
		snprintf(resp, sizeof(resp), "ID#%s\n", ws_id);
		send_network_message(client_fd, resp);
		printf("[MONITOR] Handshake completo. Engine registrado (fd: %d) con ID '%s'\n", client_fd, ws_id);
		
		notify_central_status("DISPONIBLE");

	} else if (strcmp(message, "KO") == 0) {
		printf("\n[MONITOR ALERTA] Mensaje KO recibido del Engine. Notificando LEAK a Central...\n");
		notify_central_status("LEAK");
	}
}

void on_engine_disconnect(int client_fd) {
	pthread_mutex_lock(&engine_fd_mutex);
	if (current_engine_fd == client_fd) {
		current_engine_fd = -1;
		printf("\n[MONITOR ALERTA] Pérdida física de conexión con el Engine (fd: %d). Notificando LEAK a Central...\n", client_fd);
		notify_central_status("LEAK");
	}
	pthread_mutex_unlock(&engine_fd_mutex);
}

static void* console_handler_thread(void *arg) {
	(void)arg;
	char input[64];

	printf("\n===========================================\n");
	printf(" MONITOR CONTROL INTERFACE (%s)\n", ws_id);
	printf(" Escribe 'fuga' o 'ko' -> Simular averia\n");
	printf(" Escribe 'ok'          -> Restaurar servicio\n");
	printf(" Escribe 'regar#<dur>' -> Solicitar riego local\n");
	printf(" Escribe 'parar'       -> Finalizar riego en curso\n");
	printf("===========================================\n\n");

	while (is_running) {
		if (fgets(input, sizeof(input), stdin) != NULL) {
			input[strcspn(input, "\r\n")] = 0;

			int dur = 0;

			if (strcmp(input, "fuga") == 0 || strcmp(input, "ko") == 0) {
				notify_central_status("LEAK");

			} else if (strcmp(input, "ok") == 0) {
				notify_central_status("DISPONIBLE");

			} else if (sscanf(input, "regar#%d", &dur) == 1) {
				char message[256], response[128];
				snprintf(message, sizeof(message), "LOCAL_WATER_REQUEST#%s#%d\n", ws_id, dur);
				printf("[MONITOR] Solicitud de riego local (%d seg) a Central...\n", dur);
				socket_client_send(&central_client, message);
				socket_client_receive(&central_client, response, sizeof(response));
				printf("[CENTRAL RESPONSE] %s\n", response);

			} else if (strcmp(input, "parar") == 0) {
				char message[256], response[128];
				snprintf(message, sizeof(message), "LOCAL_STOP_REQUEST#%s\n", ws_id);
				printf("[MONITOR] Solicitando parada manual a Central...\n");
				socket_client_send(&central_client, message);
				socket_client_receive(&central_client, response, sizeof(response));
				printf("[CENTRAL RESPONSE] %s\n", response);
			}
		}
	}
	return NULL;
}

int main(int argc, char *argv[]) {
	if (argc != 5) {
		fprintf(stderr, "Uso: %s <puerto_engine> <ip_central> <puerto_central> <id_ws>\n", argv[0]);
		return EXIT_FAILURE;
	}

	int port_engine = atoi(argv[1]);
	char *ip_central = argv[2];
	int port_central = atoi(argv[3]);
	strncpy(ws_id, argv[4], sizeof(ws_id) - 1);

	printf("[MONITOR] Iniciando WS Monitor ID: %s...\n", ws_id);

	if (socket_client_connect(&central_client, ip_central, port_central) != 0) {
		fprintf(stderr, "[MONITOR ERROR] No se pudo conectar a Central (%s:%d)\n", ip_central, port_central);
		return EXIT_FAILURE;
	}

	char reg_msg[256], response[128];
	snprintf(reg_msg, sizeof(reg_msg), "REGISTER#%s#Parque_Zona_%s\n", ws_id, ws_id);
	socket_client_send(&central_client, reg_msg);

	if (socket_client_receive(&central_client, response, sizeof(response)) > 0) {
		printf("[CENTRAL RESPONSE] %s\n", response);
		if (strstr(response, "NACK#ALREADY_REGISTERED") != NULL) {
			fprintf(stderr, "[MONITOR ERROR] El ID '%s' ya está registrado y activo.\n", ws_id);
			socket_client_close(&central_client);
			return EXIT_FAILURE;
		}
	}

	if (start_socket_server(&engine_server, port_engine, on_engine_message_received, on_engine_disconnect) != SOCKET_SUCCESS) {
		fprintf(stderr, "[MONITOR ERROR] No se pudo arrancar el puerto del Engine (%d)\n", port_engine);
		socket_client_close(&central_client);
		return EXIT_FAILURE;
	}
	printf("[MONITOR] Escuchando al Engine local en puerto %d...\n", port_engine);

	pthread_t health_th, console_th;
	pthread_create(&health_th, NULL, health_checker_thread, NULL);
	pthread_detach(health_th);

	pthread_create(&console_th, NULL, console_handler_thread, NULL);
	pthread_detach(console_th);

	while (is_running) {
		sleep(1);
	}

	stop_socket_server(&engine_server);
	socket_client_close(&central_client);
	return EXIT_SUCCESS;
}
