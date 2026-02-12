# syntax=docker/dockerfile:1
#
# Unified MASS Dockerfile — CPU and GPU builds.
#
# CPU build (default):
#   docker build -t mass .
#
# GPU build:
#   docker build --build-arg GPU=on -t mass:gpu .
#
# Run:
#   docker run -p 3455:3455 mass
#
# Cache behaviour:
#   The llama-go compile layer is cached by LLAMA_GO_COMMIT.
#   It only rebuilds when you bump the llama-go commit.
#   MASS source changes only rerun the fast `go build` step.

ARG GPU=off
ARG CUDA_VERSION=12.8.0
ARG CUDA_ARCHITECTURES=86-89-90-120
ARG LLAMA_GO_COMMIT=7fbd220
ARG GO_VERSION=1.26.0

# ─────────────────────────────────────────────────────────────────────────────
# Stage: CPU llama-go builder
# ─────────────────────────────────────────────────────────────────────────────
FROM golang:${GO_VERSION} AS llama-cpu

ARG LLAMA_GO_COMMIT

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake git libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/tcpipuk/llama-go.git /workspace/llama-go && \
    cd /workspace/llama-go && \
    git checkout ${LLAMA_GO_COMMIT} && \
    git submodule update --init --depth=1 llama.cpp

WORKDIR /workspace/llama-go
RUN CMAKE_ARGS="-DBUILD_SHARED_LIBS=OFF -DGGML_NATIVE=OFF" make libbinding.a

# ─────────────────────────────────────────────────────────────────────────────
# Stage: GPU llama-go builder
# ─────────────────────────────────────────────────────────────────────────────
FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu22.04 AS llama-gpu

ARG CUDA_ARCHITECTURES
ARG LLAMA_GO_COMMIT
ARG GO_VERSION

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake ccache wget git libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CCACHE_DIR=/ccache
ENV PATH=/usr/lib/ccache:${PATH}

RUN wget -qO- https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz \
    | tar -C /usr/local -xz
ENV PATH=/usr/local/go/bin:${PATH}

RUN git clone https://github.com/tcpipuk/llama-go.git /workspace/llama-go && \
    cd /workspace/llama-go && \
    git checkout ${LLAMA_GO_COMMIT} && \
    git submodule update --init --depth=1 llama.cpp

WORKDIR /workspace/llama-go

ENV LIBRARY_PATH=/workspace/llama-go
ENV C_INCLUDE_PATH=/workspace/llama-go
ENV LD_LIBRARY_PATH=/workspace/llama-go:/usr/local/cuda/lib64
ENV PATH=/usr/local/cuda/bin:${PATH}

RUN --mount=type=cache,target=/ccache \
    CUDA_ARCHS=$(echo "${CUDA_ARCHITECTURES}" | tr '-' ';') && \
    BUILD_TYPE=cublas \
    CUDA_ARCHITECTURES="$CUDA_ARCHS" \
    CMAKE_ARGS="-DBUILD_SHARED_LIBS=OFF -DGGML_NATIVE=OFF" \
    CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) \
    make libbinding.a && \
    ( cp build/ggml/src/ggml-cuda/libggml-cuda.a . 2>/dev/null || \
      cp build/ggml/src/libggml-cuda.a . 2>/dev/null || \
      { echo "ERROR: libggml-cuda.a not found after build"; exit 1; } )

# ─────────────────────────────────────────────────────────────────────────────
# Stage: Select llama-go based on GPU arg
# ─────────────────────────────────────────────────────────────────────────────
FROM llama-${GPU} AS llama-builder

# ─────────────────────────────────────────────────────────────────────────────
# Stage: Build the MASS Go binary (CPU)
# ─────────────────────────────────────────────────────────────────────────────
FROM golang:${GO_VERSION} AS builder-off

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY --from=llama-builder /workspace/llama-go /workspace/llama-go
COPY . ./mass/
WORKDIR /workspace/mass

ENV C_INCLUDE_PATH=/workspace/llama-go
ENV LIBRARY_PATH=/workspace/llama-go
ENV CGO_LDFLAGS="-L/workspace/llama-go"

RUN go build -o /bin/mass ./cmd/mass

# ─────────────────────────────────────────────────────────────────────────────
# Stage: Build the MASS Go binary (GPU)
# ─────────────────────────────────────────────────────────────────────────────
FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu22.04 AS builder-on

ARG GO_VERSION

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential wget libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN wget -qO- https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz \
    | tar -C /usr/local -xz
ENV PATH=/usr/local/go/bin:${PATH}

WORKDIR /workspace
COPY --from=llama-builder /workspace/llama-go /workspace/llama-go
COPY . ./mass/
WORKDIR /workspace/mass

ENV C_INCLUDE_PATH=/workspace/llama-go
ENV LIBRARY_PATH=/workspace/llama-go
ENV PATH=/usr/local/cuda/bin:${PATH}

RUN go build -tags cublas -o /bin/mass ./cmd/mass

# ─────────────────────────────────────────────────────────────────────────────
# Stage: Select builder based on GPU arg
# ─────────────────────────────────────────────────────────────────────────────
FROM builder-${GPU} AS builder

# ─────────────────────────────────────────────────────────────────────────────
# Stage: CPU runtime
# ─────────────────────────────────────────────────────────────────────────────
FROM debian:trixie-slim AS runtime-off

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libcurl4 libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# ─────────────────────────────────────────────────────────────────────────────
# Stage: GPU runtime
# ─────────────────────────────────────────────────────────────────────────────
FROM nvidia/cuda:${CUDA_VERSION}-runtime-ubuntu22.04 AS runtime-on

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libcurl4 libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# ─────────────────────────────────────────────────────────────────────────────
# Stage: Final image
# ─────────────────────────────────────────────────────────────────────────────
FROM runtime-${GPU}

WORKDIR /app
COPY --from=builder /bin/mass ./mass

ENV LLAMA_LOG=error

EXPOSE 3455

ENTRYPOINT ["/app/mass", "--no-browser"]
