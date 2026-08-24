CC      ?= cc
CFLAGS  ?= -O2 -g -std=c11 -Wall -Wextra -Wpedantic
LDFLAGS ?=
LDLIBS  ?= -lpthread

BIN     := bin
SRC     := src

SERVER_SRC  := $(SRC)/server.c $(SRC)/server_epoll.c $(SRC)/server_thread.c $(SRC)/common.c
LOADGEN_SRC := $(SRC)/loadgen.c $(SRC)/common.c
HDRS        := $(SRC)/server.h $(SRC)/common.h

all: $(BIN)/server $(BIN)/loadgen

$(BIN)/server: $(SERVER_SRC) $(HDRS) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $(SERVER_SRC) $(LDFLAGS) $(LDLIBS)

$(BIN)/loadgen: $(LOADGEN_SRC) $(SRC)/common.h | $(BIN)
	$(CC) $(CFLAGS) -o $@ $(LOADGEN_SRC) $(LDFLAGS) $(LDLIBS)

$(BIN):
	mkdir -p $(BIN)

clean:
	rm -rf $(BIN)

.PHONY: all clean
