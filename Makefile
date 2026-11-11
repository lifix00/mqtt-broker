CC      = gcc
CFLAGS  = -Wall -Wextra -g -O0 -std=c11 -D_GNU_SOURCE
LDFLAGS = 

SRCS    = $(wildcard src/*.c)
OBJS    = $(SRCS:.c=.o)
TARGET  = mqtt-broker

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(TARGET)

test:
	uv run pytest tests/ -v

.PHONY: all clean
