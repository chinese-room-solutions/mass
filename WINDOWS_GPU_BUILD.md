# Building MASS with GPU (CUDA) on Windows

MASS's default build targets Linux/WSL2. This document describes native Windows GPU compilation.

## Prerequisites

| Tool | Version tested | Install |
|------|---------------|---------|
| Go | 1.26.0 | https://go.dev/dl/ |
| MSYS2 MinGW-w64 GCC | 15.2.0 | `pacman -S mingw-w64-x86_64-gcc` via MSYS2 |
| MSYS2 tools | — | `pacman -S mingw-w64-x86_64-ninja mingw-w64-x86_64-cmake` |
| CUDA Toolkit | 12.9 | https://developer.nvidia.com/cuda-downloads |
| Visual Studio 2022 | 17.x (MSVC 19.43) | Community edition, "Desktop C++" workload |
| CMake | 3.x | Bundled with VS or standalone |

> **Why both MSVC and MinGW?**
> - `nvcc` (CUDA compiler) only supports MSVC (`cl.exe`) as its host compiler on Windows.
> - Go's CGO only supports GCC-compatible compilers (MinGW) on Windows.
> - MSVC and MinGW produce incompatible C++ ABIs (different name mangling, different STL).
>
> The solution is a **hybrid build**: CUDA/ggml backends as DLLs (MSVC), everything else as static libs (MinGW).

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  mass.exe  (Go + CGO, linked by MinGW)               │
│                                                     │
│  ┌─────────────┐ ┌──────────┐ ┌──────────────────┐  │
│  │libbinding.a │ │libllama.a│ │  libcommon.a     │  │
│  │(wrapper.cpp)│ │          │ │                  │  │
│  │  MinGW g++  │ │ MinGW    │ │  MinGW           │  │
│  └──────┬──────┘ └────┬─────┘ └────────┬─────────┘  │
│         │             │                │             │
│         ▼             ▼                ▼             │
│  ┌────────────────────────────────────────────────┐  │
│  │  Import libs (libggml*.a via dlltool)          │  │
│  └──────────────────────┬─────────────────────────┘  │
└─────────────────────────┼────────────────────────────┘
                          │ runtime DLL loading
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
   ┌────────────┐  ┌────────────┐  ┌──────────────┐
   │ggml-base.dll│ │ggml-cpu.dll│  │ggml-cuda.dll │
   │  ggml.dll   │ │            │  │  (57 MB)     │
   │   MSVC      │ │   MSVC     │  │ MSVC + nvcc  │
   └─────────────┘ └────────────┘  └──────────────┘
```

## Build steps

All commands run from Git Bash (MSYS2 terminal).

### 1. Build ggml DLLs with MSVC + CUDA

This builds all ggml backends (including CUDA) as shared libraries using MSVC.

```powershell
# build_ggml_dlls.ps1 — run with: powershell -ExecutionPolicy Bypass -File build_ggml_dlls.ps1
$ErrorActionPreference = "Stop"
Set-Location D:\workspace\llama-go

# Import MSVC environment
$vcvars = "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat"
$envBlock = cmd /c "`"$vcvars`" amd64 >nul 2>&1 && set" 2>&1
foreach ($line in $envBlock) {
    if ($line -match '^([^=]+)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
    }
}

if (Test-Path build-cuda-dll) { Remove-Item -Recurse -Force build-cuda-dll }

# Configure — adjust CUDA_ARCHITECTURES for your GPU:
#   75 = RTX 20xx, 86 = RTX 30xx, 89 = RTX 40xx, 90 = H100, 120 = RTX 50xx
cmake -S llama.cpp -B build-cuda-dll -G Ninja `
    -DCMAKE_BUILD_TYPE=Release `
    -DBUILD_SHARED_LIBS=ON `
    -DGGML_CUDA=ON `
    -DGGML_CUDA_FA_ALL_QUANTS=ON `
    -DGGML_CUDA_GRAPHS=ON `
    -DCMAKE_CUDA_ARCHITECTURES=86 `
    -DLLAMA_CURL=OFF

# Build all ggml targets with max parallelism
cmake --build build-cuda-dll --config Release --target ggml ggml-cpu ggml-cuda -j 16
```

### 2. Build llama/common static libs with MinGW

This builds the C++ libraries that CGO links against, using MinGW for ABI compatibility.

```bash
export PATH="/c/msys64/mingw64/bin:$PATH"
cd /d/workspace/llama-go

rm -rf build-mingw
mkdir -p build-mingw && cd build-mingw

cmake ../llama.cpp \
  -G "MinGW Makefiles" \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=OFF \
  -DGGML_CUDA=OFF \
  -DLLAMA_CURL=OFF \
  -DGGML_NATIVE=ON \
  -DCMAKE_CXX_FLAGS="-D_WIN32_WINNT=0x0A00" \
  -DCMAKE_C_FLAGS="-D_WIN32_WINNT=0x0A00"

mingw32-make -j16 ggml llama common
cd ..
```

> The `-D_WIN32_WINNT=0x0A00` flag targets Windows 10+ and fixes `CreateFile2` compilation
> errors in the bundled cpp-httplib.

### 3. Assemble libraries

Copy everything to the llama-go root directory where CGO expects them:

```bash
export PATH="/c/msys64/mingw64/bin:$PATH"
cd /d/workspace/llama-go

# MinGW-built static libs
cp build-mingw/common/libcommon.a .
cp build-mingw/src/libllama.a .
cp build-mingw/ggml/src/ggml.a libggml.a
cp build-mingw/ggml/src/ggml-base.a libggml-base.a
cp build-mingw/ggml/src/ggml-cpu.a libggml-cpu.a

# MSVC-built DLLs
cp build-cuda-dll/bin/ggml-base.dll .
cp build-cuda-dll/bin/ggml-cpu.dll .
cp build-cuda-dll/bin/ggml-cuda.dll .
cp build-cuda-dll/bin/ggml.dll .

# Generate MinGW import libraries from the DLLs
for dll in ggml-base ggml-cpu ggml-cuda ggml; do
    gendef.exe ${dll}.dll
    dlltool.exe -d ${dll}.def -l lib${dll}.a -D ${dll}.dll
done

# Copy CUDA runtime import libs (renamed for MinGW -l flag)
CUDA_LIB="/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v12.9/lib/x64"
cp "${CUDA_LIB}/cublas.lib"  libcublas.a
cp "${CUDA_LIB}/cudart.lib"  libcudart.a
cp "${CUDA_LIB}/cuda.lib"    libcuda.a

# Compile wrapper.cpp with MinGW (CGO bridge)
g++.exe -c -O3 -DNDEBUG -std=c++17 -DGGML_USE_CUDA -D_WIN32_WINNT=0x0A00 \
    -I. -Illama.cpp -Illama.cpp/common -Illama.cpp/ggml/include \
    -Illama.cpp/include -Illama.cpp/vendor \
    wrapper.cpp -o wrapper_mingw.o

ar.exe crs libbinding.a wrapper_mingw.o
```

### 4. Build MASS

```bash
export PATH="/c/msys64/mingw64/bin:$PATH"
export CGO_ENABLED=1
export C_INCLUDE_PATH="/d/workspace/llama-go"
export LIBRARY_PATH="/d/workspace/llama-go"
export CGO_LDFLAGS="-L/d/workspace/llama-go"

cd /d/workspace/mass
go build -tags cublas -p 16 -o bin/mass.exe ./cmd/mass
```

### 5. Deploy

Copy the ggml DLLs alongside the binary:

```bash
cp /d/workspace/llama-go/ggml-base.dll bin/
cp /d/workspace/llama-go/ggml-cpu.dll  bin/
cp /d/workspace/llama-go/ggml-cuda.dll bin/
cp /d/workspace/llama-go/ggml.dll      bin/
```

Runtime layout:
```
bin/
├── mass.exe         (32.6 MB)
├── ggml.dll         (0.1 MB)
├── ggml-base.dll    (0.5 MB)
├── ggml-cpu.dll     (0.9 MB)
└── ggml-cuda.dll    (57.4 MB)
```

## GPU detection

MASS detects GPU availability at startup by checking for CUDA libraries:
- **Linux/WSL2**: looks for `libcuda.so` in `/usr/lib/wsl/lib/`, `/usr/local/cuda/lib64/`, etc.
- **Windows**: looks for `nvcuda.dll` in `%SystemRoot%\System32\`

When a GPU is detected, MASS automatically:
- Offloads all model layers to GPU (`-1` = all layers)
- Uses batch size 2048 (vs 512 on CPU)
- Enables flash attention

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `nvcc fatal: Cannot find compiler 'cl.exe'` | Run the MSVC build from a VS Developer terminal, or source `vcvarsall.bat amd64` first |
| `CreateFile2 has not been declared` | Add `-DCMAKE_CXX_FLAGS="-D_WIN32_WINNT=0x0A00"` to the MinGW cmake |
| `undefined reference to std::__cxx11::basic_string` | ABI mismatch — you mixed MSVC and MinGW static libs. Only ggml should be MSVC (DLLs) |
| `cannot find -lcublas` | CUDA import libs not copied. Run the `cp` commands from step 3 |
| `cgo.exe: exit status 2` (no other output) | MinGW gcc not in PATH. Use `export PATH="/c/msys64/mingw64/bin:$PATH"` and set `CC=gcc` via `go env -w` |
| DLL not found at runtime | Copy all 4 `ggml*.dll` files next to `mass.exe` |

## CUDA architecture reference

| Arch | GPUs |
|------|------|
| 75 | RTX 2060–2080, Quadro RTX, Tesla T4 |
| 86 | RTX 3060–3090, A5000, A6000 |
| 89 | RTX 4060–4090, L40 |
| 90 | H100, H200 |
| 120 | RTX 5070–5090, Blackwell |

Detect yours: `nvidia-smi --query-gpu=compute_cap --format=csv,noheader`
