# MASS Build System
#
# MASS is a pure-Go scheduler/UI/API. Inference runs on workers (e.g.
# mass-worker-llama) that connect over gRPC — MASS itself has no CGO or
# inference-library dependencies.
#
# Usage: make <target> [VAR=val]
#
# On Windows (Git Bash / MSYS2): delegates to make-win.ps1 for build/run.
# On Linux / macOS: runs native go commands.

# -- Platform detection -------------------------------------------------------

UNAME_S := $(shell uname -s)
IS_WIN  := $(findstring MINGW,$(UNAME_S))$(findstring MSYS,$(UNAME_S))$(findstring CYGWIN,$(UNAME_S))

# -- Common variables ---------------------------------------------------------

BIN_DIR := bin
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# -- Windows: delegate to make-win.ps1 ---------------------------------------

ifdef IS_WIN

PS    := $(shell command -v pwsh 2>/dev/null || command -v powershell 2>/dev/null)
MinGW := /c/msys64/mingw64/bin

PS_SCRIPT := make-win.ps1
PS_ARGS :=
ifdef CONFIG
  PS_ARGS += -Config "$(CONFIG)"
endif

define run_ps
	$(PS) -NoProfile -ExecutionPolicy Bypass -File $(PS_SCRIPT) $(1) $(PS_ARGS)
endef

# `go test -race` uses Go's race detector, which is implemented in C and so
# requires a C toolchain even though MASS itself is pure Go. On Windows we
# pull in MinGW for that reason — `make build` and `make lint` don't need it.
RACE_ENV := PATH="$(MinGW):$$PATH" CGO_ENABLED=1

.PHONY: build build-web run clean help lint test unittest vulncheck fmt tidy proto

build:
	$(call run_ps,build)

build-web:
	$(call run_ps,build-web)

run:
	$(call run_ps,run)

clean:
	$(call run_ps,clean)

help:
	$(call run_ps,help)

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --timeout 10m ./...

unittest:
	go test ./internal/... ./pkg/... -short -count=1

test:
	$(RACE_ENV) go test ./internal/... ./pkg/... -race -covermode=atomic -coverprofile=coverage.out -count=1 -timeout 15m

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest
	protoc --go_out=. --go_opt=paths=source_relative --connect-go_out=. --connect-go_opt=paths=source_relative rpc/service.proto
	protoc --go_out=. --go_opt=paths=source_relative --connect-go_out=. --connect-go_opt=paths=source_relative -I. rpc/worker/worker.proto
	@echo "Protobuf code generated."

# -- Linux / macOS: native build ----------------------------------------------

else

ifneq ($(wildcard $(HOME)/.goenv/shims),)
  export PATH := $(HOME)/.goenv/bin:$(HOME)/.goenv/shims:$(PATH)
endif

.PHONY: build build-web run proto test unittest vulncheck lint fmt tidy clean help

BINARY := $(BIN_DIR)/mass

# -- Generate web assets (templ + Tailwind CSS) -------------------------------

build-web:
	@echo "==> Generating web assets..."
	@if command -v templ >/dev/null 2>&1; then \
		for f in internal/web/templates/*.templ; do \
			templ generate -f "$$f"; \
		done; \
		echo "    templ generated"; \
	fi
	@if [ -f web/package.json ]; then \
		cd web && \
		if command -v bun >/dev/null 2>&1; then \
			bun install --frozen-lockfile 2>/dev/null || true; \
			bun run build:css; \
		elif command -v npx >/dev/null 2>&1; then \
			npm install 2>/dev/null || true; \
			npx postcss input.tw.css -o public/dist.css --config postcss.config.js; \
		fi; \
		echo "    CSS built"; \
	fi

# -- Build mass binary (pure Go, no CGO) --------------------------------------

build: build-web
	@echo "==> Building mass ($(VERSION))..."
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -ldflags '-X main.version=$(VERSION)' -o $(BINARY) ./cmd/mass
	@echo "    Built: $(BINARY)"

run: build
	@echo "==> Starting mass..."
	./$(BINARY)

proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest
	protoc --go_out=. --go_opt=paths=source_relative --connect-go_out=. --connect-go_opt=paths=source_relative rpc/service.proto
	protoc --go_out=. --go_opt=paths=source_relative --connect-go_out=. --connect-go_opt=paths=source_relative -I. rpc/worker/worker.proto
	@echo "Protobuf code generated."

unittest:
	go test ./internal/... ./pkg/... -short -count=1

test:
	go test ./internal/... ./pkg/... -race -covermode=atomic -coverprofile=coverage.out -count=1 -timeout 15m

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --timeout 10m ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

clean:
	rm -rf $(BIN_DIR)
	@echo "Cleaned."

help:
	@echo ""
	@echo "  MASS Build System (pure Go — workers handle inference)"
	@echo "  ======================================================"
	@echo ""
	@echo "  Usage: make <target>"
	@echo ""
	@echo "  Targets:"
	@echo "    build       Build mass binary (web assets + Go build)"
	@echo "    build-web   Generate web assets only (templ + Tailwind)"
	@echo "    run         Build and start mass"
	@echo "    test        Run tests with -race"
	@echo "    unittest    Run tests with -short"
	@echo "    lint        Run golangci-lint"
	@echo "    vulncheck   Run govulncheck"
	@echo "    fmt         Format Go code"
	@echo "    tidy        Run go mod tidy"
	@echo "    proto       Regenerate protobuf/ConnectRPC code"
	@echo "    clean       Remove build outputs"
	@echo ""
	@echo "  Note: MASS no longer links llama.cpp. Install and run"
	@echo "  mass-worker-<runtime> (e.g. mass-worker-llama) separately"
	@echo "  to provide inference capacity."
	@echo ""

endif
