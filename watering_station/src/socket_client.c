#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "socket_client.h"
#include <netdb.h>

int socket_client_connect(socket_client_t *client, const char *ip, int port) {
    if (!client || !ip) return -1;

    memset(client, 0, sizeof(socket_client_t));
    strncpy(client->server_ip, ip, sizeof(client->server_ip) - 1);
    client->server_port = port;

    client->socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client->socket_fd < 0) return -1;

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    // Intentar resolver como IP literal primero
    if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
        // Si no es IP pura, resolver el nombre del host (ej: "wm_central") mediante el DNS de Docker
        struct hostent *hp = gethostbyname(ip);
        if (hp == NULL) {
            fprintf(stderr, "[SOCKET ERROR] No se pudo resolver el host: %s\n", ip);
            close(client->socket_fd);
            client->socket_fd = -1;
            return -1;
        }
        memcpy(&server_addr.sin_addr, hp->h_addr_list[0], hp->h_length);
    }

    if (connect(client->socket_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        close(client->socket_fd);
        client->socket_fd = -1;
        return -1;
    }

    client->is_connected = 1;
    return 0;
}

int socket_client_send(socket_client_t *client, const char *message) {
    if (!client || !client->is_connected || client->socket_fd < 0) return -1;
    return (int)send(client->socket_fd, message, strlen(message), 0);
}

int socket_client_receive(socket_client_t *client, char *buffer, size_t max_len) {
    if (!client || !client->is_connected || client->socket_fd < 0) return -1;

    memset(buffer, 0, max_len);
    ssize_t bytes = recv(client->socket_fd, buffer, max_len - 1, 0);

    if (bytes <= 0) {
        client->is_connected = 0;
        return (int)bytes;
    }

    buffer[strcspn(buffer, "\r\n")] = 0;
    return (int)bytes;
}

void socket_client_close(socket_client_t *client) {
    if (!client) return;
    if (client->socket_fd >= 0) {
        close(client->socket_fd);
        client->socket_fd = -1;
    }
    client->is_connected = 0;
}
