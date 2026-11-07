#ifndef SOCKET_SERVER_H
#define SOCKET_SERVER_H

#include <pthread.h>
#include <stddef.h>

typedef enum {
    SOCKET_SUCCESS = 0,
    SOCKET_ERR_CREATE,
    SOCKET_ERR_BIND,
    SOCKET_ERR_LISTEN,
    SOCKET_ERR_THREAD
} socket_status_t;

typedef void (*socket_msg_callback_t)(int client_fd, const char *message);

typedef struct {
    int server_fd;
    int port;
    pthread_t listener_thread;
    int is_running;
    socket_msg_callback_t msg_callback;
} socket_server_t;

socket_status_t start_socket_server(socket_server_t *server, int port, socket_msg_callback_t callback);

void stop_socket_server(socket_server_t *server);

int read_network_message(int client_fd, char *buffer, size_t max_len);

int send_network_message(int client_fd, const char *message);

#endif
