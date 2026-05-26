GO        ?= go
PKG       := ./...
BIN_DIR   := bin
BIN       := $(BIN_DIR)/kv

.PHONY: dep test test-race build run clean

dep:
	$(GO) mod download

test:
	$(GO) test -timeout 30s -v $(PKG)

test-race:
	$(GO) test -timeout 60s -race $(PKG)

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN) ./cmd/kv

run: build
	$(BIN)

clean:
	rm -rf $(BIN_DIR)
