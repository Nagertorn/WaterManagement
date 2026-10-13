# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -O2
LDFLAGS = -lsqlite3

# Target executable name
TARGET = WM_Central

# Source and Object files
SRCS = WM_Central.c db.c
OBJS = $(SRCS:.c=.o)

# Default rule: build the executable
all: $(TARGET)

# Link object files to create executable
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

# Compile C source files into object files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Clean compiled binaries and object files
clean:
	rm -f $(OBJS) $(TARGET)

# Clean everything including the generated SQLite database file
clean-db: clean
	rm -f water_management.db

.PHONY: all clean clean-db
