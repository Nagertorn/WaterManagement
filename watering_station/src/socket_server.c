#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "socket_server.h"

typedef struct {
    int client_fd;
    socket_server_t *server;
} client_args_t;


int read_network_message(int client_fd, char *buffer, size_t max_len) {
    memset(buffer, 0, max_len);
    ssize_t bytes_read = recv(client_fd, buffer, max_len - 1, 0);
    
    if (bytes_read <= 0) {
        return (int)bytes_read;
    }

    buffer[strcspn(buffer, "\r\n")] = 0;
    return (int)bytes_read;
}

int send_network_message(int client_fd, const char *message) {
    return (int)send(client_fd, message, strlen(message), 0);
}

static void* client_handler_thread(void *arg) {
    client_args_t *cargs = (client_args_t*)arg;
    int client_fd = cargs->client_fd;
    socket_server_t *server = cargs->server;
    free(cargs);

    char buffer[512];
    printf("[SOCKET] New client connected (fd: %d)\n", client_fd);

    while (server->is_running) {
        int bytes = read_network_message(client_fd, buffer, sizeof(buffer));
        if (bytes <= 0) {
            printf("[SOCKET] Client (fd: %d) disconnected.\n", client_fd);
			if (server->disconnect_callback) {
            	server->disconnect_callback(client_fd);
         	}
            break;
        }

        if (server->msg_callback) {
            server->msg_callback(client_fd, buffer);
        }
    }

    close(client_fd);
    return NULL;
}

static void* socket_listener_loop(void *arg) {
    socket_server_t *server = (socket_server_t*)arg;

    while (server->is_running) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        int client_fd = accept(server->server_fd, (struct sockaddr*)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (!server->is_running) break;
            perror("[SOCKET ERROR] Accept failed");
            continue;
        }

        client_args_t *cargs = malloc(sizeof(client_args_t));
        if (!cargs) {
            close(client_fd);
            continue;
        }
        cargs->client_fd = client_fd;
        cargs->server = server;

        pthread_t client_thread;
        if (pthread_create(&client_thread, NULL, client_handler_thread, cargs) != 0) {
            fprintf(stderr, "[SOCKET ERROR] Could not create thread for client fd %d\n", client_fd);
            close(client_fd);
            free(cargs);
        } else {
            pthread_detach(client_thread);
        }
    }

    return NULL;
}

socket_status_t start_socket_server(socket_server_t *server, int port, socket_msg_callback_t callback, socket_disconnect_callback_t disconnect_callback) {
    if (!server) return SOCKET_ERR_CREATE;

    server->port = port;
    server->is_running = 0;
    server->msg_callback = callback;
    server->disconnect_callback = disconnect_callback;

    server->server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server->server_fd < 0) {
        perror("[SOCKET ERROR] Socket creation failed");
        return SOCKET_ERR_CREATE;
    }

    int opt = 1;
    setsockopt(server->server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server->server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("[SOCKET ERROR] Bind failed");
        close(server->server_fd);
        return SOCKET_ERR_BIND;
    }

    if (listen(server->server_fd, 10) < 0) {
        perror("[SOCKET ERROR] Listen failed");
        close(server->server_fd);
        return SOCKET_ERR_LISTEN;
    }

    server->is_running = 1;
    if (pthread_create(&server->listener_thread, NULL, socket_listener_loop, server) != 0) {
        fprintf(stderr, "[SOCKET ERROR] Listener thread creation failed\n");
        close(server->server_fd);
        server->is_running = 0;
        return SOCKET_ERR_THREAD;
    }

    printf("[SOCKET] Server successfully listening on port %d...\n", port);
    return SOCKET_SUCCESS;
}

void stop_socket_server(socket_server_t *server) {
    if (!server || !server->is_running) return;

    server->is_running = 0;
    close(server->server_fd);
    pthread_join(server->listener_thread, NULL);
    printf("[SOCKET] Server stopped cleanly.\n");
}
