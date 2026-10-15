PREFIX   ?= $(HOME)/.local
BUILD_ID := $(shell date +%s)-$(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
PKG      := github.com/shxntanu/rift
LDFLAGS  := -s -w -X main.buildID=$(BUILD_ID) -X $(PKG)/internal/transport.SourceDir=$(CURDIR)
GOBUILD  := CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)"

# Server binaries for remote platforms are built alongside the client so it
# can install itself on any of them over ssh.
TARGETS := linux/amd64 linux/arm64 darwin/arm64 darwin/amd64

.PHONY: all rift cross install clean

all: rift cross

rift:
	$(GOBUILD) -o bin/rift ./cmd/rift

cross:
	@for t in $(TARGETS); do \
		os=$${t%/*}; arch=$${t#*/}; \
		echo "build bin/rift-$$os-$$arch"; \
		GOOS=$$os GOARCH=$$arch $(GOBUILD) -o bin/rift-$$os-$$arch ./cmd/rift || exit 1; \
	done

install: all
	mkdir -p $(PREFIX)/bin
	cp bin/rift bin/rift-* $(PREFIX)/bin/

clean:
	rm -rf bin
