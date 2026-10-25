CC ?= cc
CFLAGS ?= -O2
WARNINGS := -Wall -Wextra -Werror -Wpedantic
PLUGIN_SO := build/opencode-go.so

.PHONY: all test clean

all: $(PLUGIN_SO)

$(PLUGIN_SO): main.go auth.go models.go executor.go quota.go go.mod
	mkdir -p build
	CGO_ENABLED=1 go build -buildvcs=false -buildmode=c-shared -o $(PLUGIN_SO) .
	rm -f build/opencode-go.h

build/abi-test: tests/abi_test.c
	mkdir -p build
	$(CC) $(CFLAGS) $(WARNINGS) -o $@ $< -ldl

test: $(PLUGIN_SO) build/abi-test
	go test -v ./...
	./build/abi-test ./$(PLUGIN_SO)

clean:
	rm -rf build
