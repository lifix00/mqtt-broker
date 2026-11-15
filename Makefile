CC      = gcc
CFLAGS  = -Wall -Wextra -g -O0 -std=c11 -D_GNU_SOURCE -pthread
LDFLAGS =
LDLIBS  = -lssl -lcrypto -pthread

SRCS    = $(wildcard src/*.c)
OBJS    = $(SRCS:.c=.o)
TARGET  = mqtt-broker

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LDLIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(TARGET)

test:
	uv run pytest tests/ -v

.PHONY: all clean
