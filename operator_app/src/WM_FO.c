#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "kafka_client.h"

static kafka_context_t kafka_ctx;
static char operator_id[32];
static int service_completed = 0;
static int is_running = 1;

void on_kafka_response_received(const char *topic, const char *payload) {
    (void)topic;
    char target_op[32] = {0};
    char ws_id[32] = {0};
    char status[32] = {0};
    char details[128] = {0};

    if (sscanf(payload, "RESPONSE#%31[^#]#%31[^#]#%31[^#]#%127s", target_op, ws_id, status, details) >= 3) {
        if (strcmp(target_op, operator_id) == 0) {
            printf("\n[NOTIFICACIÓN CENTRAL] Estación: %s | Estado: %s | Info: %s\n", ws_id, status, details);
            
            if (strcmp(status, "AUTHORIZED") == 0 || strcmp(status, "DENIED") == 0 || strcmp(status, "FINISHED") == 0) {
                service_completed = 1;
            }
        }
    }
}

void request_watering(const char *ws_id, int duration) {
    char request_msg[256];
    snprintf(request_msg, sizeof(request_msg), "REQUEST_WATER#%s#%s#%d\n", ws_id, operator_id, duration);

    service_completed = 0;
    printf("[OPERARIO %s] Solicitando riego en %s por %d segundos...\n", operator_id, ws_id, duration);
    kafka_send_message(&kafka_ctx, "central_requests", request_msg);

    while (!service_completed && is_running) {
        usleep(100000);
    }
}

void process_commands_file(const char *filepath) {
    FILE *file = fopen(filepath, "r");
    if (!file) {
        perror("[ERROR] No se pudo abrir el fichero de comandos");
        return;
    }

    printf("[OPERARIO] Modo automático: procesando fichero '%s'...\n", filepath);
    char line[128];

    while (fgets(line, sizeof(line), file) && is_running) {
        line[strcspn(line, "\r\n")] = 0;
        if (strlen(line) == 0 || line[0] == '#') continue;

        char ws_id[32] = {0};
        int duration = 0;

        if (sscanf(line, "%31[^#]#%d", ws_id, &duration) == 2) {
            request_watering(ws_id, duration);

            printf("[OPERARIO] Servicio finalizado. Esperando 4 segundos antes de la siguiente solicitud...\n");
            sleep(4);
        }
    }

    fclose(file);
    printf("[OPERARIO] Procesamiento del fichero finalizado.\n");
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Uso: %s <bootstrap_servers_kafka> <id_operador> [fichero_ordenes]\n", argv[0]);
        return EXIT_FAILURE;
    }

    char *kafka_servers = argv[1];
    strncpy(operator_id, argv[2], sizeof(operator_id) - 1);
    char *commands_file = (argc >= 4) ? argv[3] : NULL;

    printf("[OPERARIO] Iniciando WM_FO (ID: %s)...\n", operator_id);

    char group_id[64];
    snprintf(group_id, sizeof(group_id), "operator_group_%s", operator_id);

    if (init_kafka(&kafka_ctx, kafka_servers, group_id) != KAFKA_SUCCESS) {
        fprintf(stderr, "[ERROR] Fallo al conectar con Kafka (%s)\n", kafka_servers);
        return EXIT_FAILURE;
    }

    start_kafka_consumer(&kafka_ctx, "operator_responses", on_kafka_response_received);

    if (commands_file) {
        process_commands_file(commands_file);
    } else {
        char input[128];
        while (is_running) {
            printf("\n--- MENU OPERARIO (%s) ---\n", operator_id);
            printf(" Introduce orden formato: <WS_ID>#<DURACION_SEG> (ej: WS-01#30)\n");
            printf(" Escribe 'salir' para terminar.\n");
            printf("> ");
            fflush(stdout);

            if (fgets(input, sizeof(input), stdin) != NULL) {
                input[strcspn(input, "\r\n")] = 0;
                if (strcmp(input, "salir") == 0) break;

                char ws_id[32] = {0};
                int duration = 0;

                if (sscanf(input, "%31[^#]#%d", ws_id, &duration) == 2) {
                    request_watering(ws_id, duration);
                } else {
                    printf("[ERROR] Formato incorrecto. Ejemplo válido: WS-01#30\n");
                }
            }
        }
    }

    close_kafka(&kafka_ctx);
    return EXIT_SUCCESS;
}
