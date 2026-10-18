CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -O2 -Iinclude
LDFLAGS = -lsqlite3 -lrdkafka -pthread

TARGET = WM_Central.out

SRCS = $(wildcard src/*.c)
OBJS = $(SRCS:.c=.o)

all: create-dirs $(TARGET)

create-dirs:
	mkdir -p data

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

clean-db: clean
	rm -f data/water_management.db

.PHONY: all create-dirs clean clean-db
