CC = gcc
CFLAGS = -Wall -Wextra -Iinclude -pthread
LDFLAGS = -lrdkafka -lpthread

SRC_DIR = src
OBJ_DIR = obj
BIN_DIR = bin

TARGET_MONITOR = $(BIN_DIR)/WM_WS_M
TARGET_ENGINE  = $(BIN_DIR)/WM_WS_E

# Archivos del Monitor
MONITOR_SRCS = $(SRC_DIR)/WM_WS_M.c $(SRC_DIR)/socket_client.c $(SRC_DIR)/socket_server.c
MONITOR_OBJS = $(patsubst $(SRC_DIR)/%.c, $(OBJ_DIR)/%.o, $(MONITOR_SRCS))

# Archivos del Engine
ENGINE_SRCS  = $(SRC_DIR)/WM_WS_E.c $(SRC_DIR)/socket_client.c $(SRC_DIR)/kafka_client.c
ENGINE_OBJS  = $(patsubst $(SRC_DIR)/%.c, $(OBJ_DIR)/%.o, $(ENGINE_SRCS))

all: $(TARGET_MONITOR) $(TARGET_ENGINE)

$(TARGET_MONITOR): $(MONITOR_OBJS) | $(BIN_DIR)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(TARGET_ENGINE): $(ENGINE_OBJS) | $(BIN_DIR)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN_DIR) $(OBJ_DIR):
	mkdir -p $@

clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR)

.PHONY: all clean
