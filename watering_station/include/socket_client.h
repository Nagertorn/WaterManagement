#ifndef SOCKET_CLIENT_H
#define SOCKET_CLIENT_H

#include <stddef.h>

typedef struct {
    int socket_fd;
    char server_ip[64];
    int server_port;
    int is_connected;
} socket_client_t;

int socket_client_connect(socket_client_t *client, const char *ip, int port);
int socket_client_send(socket_client_t *client, const char *message);
int socket_client_receive(socket_client_t *client, char *buffer, size_t max_len);
void socket_client_close(socket_client_t *client);

#endif
