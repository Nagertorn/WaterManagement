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

void on_engine_message_received(int client_fd, const char *message) {
    printf("[MONITOR] Message from Engine (fd %d): %s\n", client_fd, message);

    if (strcmp(message, "GET_ID") == 0) {
        char resp[64];
        snprintf(resp, sizeof(resp), "ID#%s\n", ws_id);
        send_network_message(client_fd, resp);
        printf("[MONITOR] Handshake complete. Sent ID '%s' to Engine\n", ws_id);

    } else if (strcmp(message, "PING") == 0) {
        send_network_message(client_fd, "PONG_OK\n");

    } else if (strcmp(message, "KO") == 0) {
        printf("[MONITOR] Engine reported KO! Notifying Central of LEAK...\n");
        
        char msg[256], resp[128];
        snprintf(msg, sizeof(msg), "STATUS#%s#LEAK\n", ws_id);
        socket_client_send(&central_client, msg);
        socket_client_receive(&central_client, resp, sizeof(resp));
    }
}

static void* console_handler_thread(void *arg) {
    (void)arg;
    char input[64];

    printf("\n===========================================\n");
    printf(" MONITOR CONTROL INTERFACE (%s)\n", ws_id);
    printf(" Escribe 'fuga' o 'ko' -> Simular averia\n");
    printf(" Escribe 'ok'          -> Restaurar servicio\n");
    printf(" Escribe 'regar#<dur>' -> Solicitar riego local\n");
    printf("===========================================\n\n");

    while (is_running) {
        if (fgets(input, sizeof(input), stdin) != NULL) {
            input[strcspn(input, "\r\n")] = 0;

            char message[256];
            char response[128];
			int dur = 0;

            if (strcmp(input, "fuga") == 0 || strcmp(input, "ko") == 0) {
                snprintf(message, sizeof(message), "STATUS#%s#LEAK\n", ws_id);
                printf("[MONITOR] Reporting LEAK to Central...\n");
                socket_client_send(&central_client, message);
                socket_client_receive(&central_client, response, sizeof(response));
                printf("[CENTRAL RESPONSE] %s\n", response);

            } else if (strcmp(input, "ok") == 0) {
                snprintf(message, sizeof(message), "STATUS#%s#DISPONIBLE\n", ws_id);
                printf("[MONITOR] Reporting DISPONIBLE to Central...\n");
                socket_client_send(&central_client, message);
                socket_client_receive(&central_client, response, sizeof(response));
                printf("[CENTRAL RESPONSE] %s\n", response);
			} else if (sscanf(input, "regar#%d", &dur) == 1) {
                snprintf(message, sizeof(message), "LOCAL_WATER_REQUEST#%s#%d\n", ws_id, dur);
                printf("[MONITOR] Peticion de riego local (%d seg) enviada a Central...\n", dur);
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

    printf("[MONITOR] Starting WS Monitor ID: %s...\n", ws_id);

    if (socket_client_connect(&central_client, ip_central, port_central) != 0) {
        fprintf(stderr, "[MONITOR ERROR] Failed connection to Central (%s:%d)\n", ip_central, port_central);
        return EXIT_FAILURE;
    }

    char reg_msg[256], response[128];
    snprintf(reg_msg, sizeof(reg_msg), "REGISTER#%s#Parque_Zona_%s\n", ws_id, ws_id);
    socket_client_send(&central_client, reg_msg);

    if (socket_client_receive(&central_client, response, sizeof(response)) > 0) {
        printf("[CENTRAL RESPONSE] %s\n", response);
		if (strstr(response, "NACK#ALREADY_REGISTERED") != NULL) {
            fprintf(stderr, "[MONITOR ERROR] Ya existe una estación activa conectada con el ID '%s'. Abortando...\n", ws_id);
            socket_client_close(&central_client);
            return EXIT_FAILURE;
        }
    } else {
        fprintf(stderr, "[MONITOR ERROR] No response from Central\n");
        socket_client_close(&central_client);
        return EXIT_FAILURE;
    }

    if (start_socket_server(&engine_server, port_engine, on_engine_message_received) != SOCKET_SUCCESS) {
        fprintf(stderr, "[MONITOR ERROR] Failed to start local server for Engine on port %d\n", port_engine);
        socket_client_close(&central_client);
        return EXIT_FAILURE;
    }
    printf("[MONITOR] Listening for Engine on local port %d...\n", port_engine);

    pthread_t console_thread;
    pthread_create(&console_thread, NULL, console_handler_thread, NULL);
    pthread_detach(console_thread);

    while (is_running) {
        sleep(1);
    }

    stop_socket_server(&engine_server);
    socket_client_close(&central_client);
    return EXIT_SUCCESS;
}
