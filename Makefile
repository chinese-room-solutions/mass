# MASS Build System
#
# Usage: make <target> [VAR=val]
#
# On Windows (Git Bash / MSYS2): delegates to make-win.ps1
# On Linux / macOS: runs native build commands
#
# Variables:
#   LLAMA_GO_DIR  Path to llama-go (default: ../llama-go)
#   BUILD_TAGS    Go build tags, e.g. "cublas" for CUDA/GPU (default: empty = CPU-only)
#   CUDA_ARCH     CUDA compute capability (default: auto-detect, only used with BUILD_TAGS=cublas)
#   CONFIG        Config file for run (default: config/dev.yml)
#   JOBS          Parallel jobs (default: CPU count)

# -- Platform detection -------------------------------------------------------

UNAME_S := $(shell uname -s)
IS_WIN  := $(findstring MINGW,$(UNAME_S))$(findstring MSYS,$(UNAME_S))$(findstring CYGWIN,$(UNAME_S))

# -- Common variables ---------------------------------------------------------

LLAMA_GO_DIR ?= $(shell cd "$(dir $(lastword $(MAKEFILE_LIST)))/.." && pwd)/llama-go
BIN_DIR      := bin
BINARY       := $(BIN_DIR)/mass

# -- Windows: delegate to make-win.ps1 ---------------------------------------

ifdef IS_WIN

PS    := $(shell command -v pwsh 2>/dev/null || command -v powershell 2>/dev/null)
MinGW := /c/msys64/mingw64/bin

PS_SCRIPT := make-win.ps1
PS_ARGS :=
ifdef CONFIG
  PS_ARGS += -Config "$(CONFIG)"
endif
ifdef CUDA_ARCH
  PS_ARGS += -CudaArch "$(CUDA_ARCH)"
endif
ifdef JOBS
  PS_ARGS += -Jobs $(JOBS)
endif

define run_ps
	$(PS) -NoProfile -ExecutionPolicy Bypass -File $(PS_SCRIPT) $(1) $(PS_ARGS)
endef

.PHONY: build build-libs run clean clean-all help lint test fmt tidy

build:
	$(call run_ps,build)

build-libs:
	$(call run_ps,build-libs)

run:
	$(call run_ps,run)

clean:
	$(call run_ps,clean)

clean-all:
	$(call run_ps,clean-all)

help:
	$(call run_ps,help)

lint:
	PATH="$(MinGW):$(LLAMA_GO_DIR):$$PATH" CGO_ENABLED=1 C_INCLUDE_PATH="$(LLAMA_GO_DIR)" LIBRARY_PATH="$(LLAMA_GO_DIR)" go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --timeout 2m ./...

test:
	PATH="$(MinGW):$(LLAMA_GO_DIR):$$PATH" CGO_ENABLED=1 C_INCLUDE_PATH="$(LLAMA_GO_DIR)" LIBRARY_PATH="$(LLAMA_GO_DIR)" go test ./internal/... -v -count=1

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

# -- Linux / macOS: native build ----------------------------------------------

else

# Setup PATH for goenv / CUDA
ifneq ($(wildcard $(HOME)/.goenv/shims),)
  export PATH := $(HOME)/.goenv/bin:$(HOME)/.goenv/shims:$(PATH)
endif
ifneq ($(wildcard /usr/local/cuda/bin),)
  export PATH := /usr/local/cuda/bin:$(PATH)
endif

UNAME_M := $(shell uname -m)
ifeq ($(UNAME_M),x86_64)
  ARCH := amd64
else ifeq ($(UNAME_M),aarch64)
  ARCH := arm64
else ifeq ($(UNAME_M),arm64)
  ARCH := arm64
else
  ARCH := amd64
endif

# Auto-detect CUDA architecture from nvidia-smi if not specified.
ifndef CUDA_ARCH
  CUDA_ARCH := $(shell nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1 | tr -d '.' || echo "86")
  ifeq ($(CUDA_ARCH),)
    CUDA_ARCH := 86
  endif
endif

.PHONY: build build-libs build-web run proto test lint fmt tidy clean clean-all help

# -- Ensure llama-go is cloned ------------------------------------------------

$(LLAMA_GO_DIR):
	git clone --recurse-submodules https://github.com/tcpipuk/llama-go.git $(LLAMA_GO_DIR)

# -- Build native libraries ---------------------------------------------------

$(LLAMA_GO_DIR)/libbinding.a: | $(LLAMA_GO_DIR)
ifeq ($(BUILD_TAGS),cublas)
	@echo "==> Building llama-go static libraries (CUDA arch: $(CUDA_ARCH))..."
	@command -v nvcc >/dev/null 2>&1 || { echo "Error: nvcc not found. Install CUDA Toolkit."; exit 1; }
	cd $(LLAMA_GO_DIR) && \
		BUILD_TYPE=cublas \
		CMAKE_ARGS="-DBUILD_SHARED_LIBS=OFF -DCMAKE_CUDA_ARCHITECTURES=$(CUDA_ARCH)" \
		make libbinding.a
	@# Copy libggml-cuda.a if present (llama-go Makefile doesn't always do this)
	@if [ -f $(LLAMA_GO_DIR)/build/ggml/src/ggml-cuda/libggml-cuda.a ]; then \
		cp $(LLAMA_GO_DIR)/build/ggml/src/ggml-cuda/libggml-cuda.a $(LLAMA_GO_DIR)/; \
	elif [ -f $(LLAMA_GO_DIR)/build/ggml/src/libggml-cuda.a ]; then \
		cp $(LLAMA_GO_DIR)/build/ggml/src/libggml-cuda.a $(LLAMA_GO_DIR)/; \
	else \
		echo "Warning: libggml-cuda.a not found — GPU linker may fail"; \
	fi
else
	@echo "==> Building llama-go static libraries (CPU-only)..."
	cd $(LLAMA_GO_DIR) && \
		CMAKE_ARGS="-DBUILD_SHARED_LIBS=OFF" \
		make libbinding.a
endif
	@echo "    Static libraries built"

build-libs: $(LLAMA_GO_DIR)/libbinding.a

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

# -- Build mass binary --------------------------------------------------------

build: build-libs build-web
	@echo "==> Building mass..."
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=1 \
	C_INCLUDE_PATH=$(LLAMA_GO_DIR) \
	LIBRARY_PATH=$(LLAMA_GO_DIR) \
	go build -a $(if $(BUILD_TAGS),-tags $(BUILD_TAGS),) -ldflags '-extldflags "-static"' -o $(BINARY) ./cmd/mass
	@echo "    Built: $(BINARY)"

# -- Run ----------------------------------------------------------------------

run: build
	@echo "==> Starting mass..."
	./$(BINARY)

# -- Dev tools ----------------------------------------------------------------

proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest
	protoc --go_out=. --go_opt=paths=source_relative --connect-go_out=. --connect-go_opt=paths=source_relative rpc/service.proto
	protoc --go_out=. --go_opt=paths=source_relative --connect-go_out=. --connect-go_opt=paths=source_relative -I. rpc/agent/agent.proto
	@echo "Protobuf code generated."

test:
	go test ./internal/... -v -count=1

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --timeout 2m ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

# -- Clean --------------------------------------------------------------------

clean:
	rm -rf $(BIN_DIR)
	@echo "Cleaned."

clean-all: clean
	@if [ -d "$(LLAMA_GO_DIR)" ]; then \
		echo "Cleaning llama-go build..."; \
		cd $(LLAMA_GO_DIR) && make clean || true; \
	fi
	@echo "All cleaned."

# -- Help ---------------------------------------------------------------------

help:
	@echo ""
	@echo "  MASS Build System"
	@echo "  ================="
	@echo ""
	@echo "  Usage: make <target> [VAR=val]"
	@echo ""
	@echo "  Targets:"
	@echo "    build        Full build (native libs + web assets + mass binary)"
	@echo "    build-libs   Build llama-go native libraries"
	@echo "    build-web    Rebuild web assets only (templ + Tailwind CSS)"
	@echo "    run          Build and run mass (web UI)"
	@echo "    proto        Generate protobuf/ConnectRPC code"
	@echo "    test         Run unit tests"
	@echo "    lint         Run linter"
	@echo "    fmt          Format Go code"
	@echo "    tidy         Tidy go.mod"
	@echo "    clean        Remove bin/"
	@echo "    clean-all    Remove bin/ + llama-go build artifacts"
	@echo ""
	@echo "  Variables:"
	@echo "    LLAMA_GO_DIR  Path to llama-go (default: ../llama-go)"
	@echo "    BUILD_TAGS    Go build tags, e.g. 'cublas' for GPU (default: CPU-only)"
	@echo "    CUDA_ARCH     CUDA compute capability (default: auto-detect)"
	@echo "    CONFIG        Config file for run (Windows only)"
	@echo "    JOBS          Parallel jobs (Windows only)"
	@echo ""
	@echo "  Examples:"
	@echo "    make build                        # CPU-only build"
	@echo "    make build BUILD_TAGS=cublas       # CUDA/GPU build"
	@echo ""

endif
