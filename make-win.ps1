#Requires -Version 5.1
<#
.SYNOPSIS
    Build MASS on native Windows.

.DESCRIPTION
    Hybrid build: MSVC+nvcc for ggml DLLs (CUDA), MinGW for C++ static libs (CGO ABI).
    The resulting binary supports both GPU and CPU inference at runtime.
    See WINDOWS_GPU_BUILD.md for architecture details.

.EXAMPLE
    .\make-win.ps1 build
    .\make-win.ps1 build -CudaArch 89
    .\make-win.ps1 build-libs
    .\make-win.ps1 clean
    .\make-win.ps1 run
    .\make-win.ps1 run
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet("build", "build-libs", "build-libs-gpu", "build-libs-mingw", "run", "clean", "clean-all", "help")]
    [string]$Command = "help",

    [Parameter()]
    [string]$Config = "",

    [Parameter()]
    [string]$CudaArch = "",

    [Parameter()]
    [int]$Jobs = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve config: -Config flag > MASS_CONFIG env > default
if (-not $Config) {
    $Config = if ($env:MASS_CONFIG) { $env:MASS_CONFIG } else { "config\dev.yml" }
}

# -- Paths --------------------------------------------------------------------

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Definition
$LlamaGoDir  = if ($env:LLAMA_GO_DIR) { $env:LLAMA_GO_DIR } else { Join-Path (Split-Path $ScriptDir) "llama-go" }
$BinDir      = Join-Path $ScriptDir "bin"
$MinGW       = "C:\msys64\mingw64\bin"
$VsVcvars    = "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat"

if ($Jobs -le 0) {
    $Jobs = (Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum
    if ($Jobs -le 0) { $Jobs = 8 }
}

# Note: MinGW is added to PATH only in functions that need it (Build-MinGWLibs,
# Assemble-Libs, Build-Mass). The CUDA DLL build uses MSVC tools via Import-VsEnv
# and only needs ninja, which we ensure is available via the system PATH or MSYS2.

# -- Helpers ------------------------------------------------------------------

function Write-Step([string]$msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Write-OK([string]$msg)   { Write-Host "    $msg" -ForegroundColor Green }
function Write-Err([string]$msg)  { Write-Host "    $msg" -ForegroundColor Red; exit 1 }

function Assert-Command([string]$name, [string]$hint) {
    if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
        Write-Err "$name not found. $hint"
    }
}


function Import-VsEnv {
    if (-not (Test-Path $VsVcvars)) {
        Write-Err "vcvarsall.bat not found at $VsVcvars - install VS 2022 'Desktop C++' workload"
    }
    $cmdLine = '"{0}" amd64 >nul 2>&1 & if not errorlevel 1 set' -f $VsVcvars
    $envBlock = cmd /c $cmdLine
    foreach ($line in $envBlock) {
        if ($line -match '^([^=]+)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
        }
    }
}

function Detect-CudaArch {
    if ($CudaArch) { return $CudaArch }
    return "61;75;86;89;90;120"
}

function Ensure-LlamaGo {
    if (Test-Path $LlamaGoDir) { return }
    Write-Step "Cloning llama-go..."
    git clone --recurse-submodules https://github.com/tcpipuk/llama-go.git $LlamaGoDir
    if ($LASTEXITCODE -ne 0) { Write-Err "git clone failed (exit code $LASTEXITCODE)" }
}

# -- Build: ggml DLLs with MSVC + CUDA ---------------------------------------

function Build-GgmlDlls {
    $arch = Detect-CudaArch
    Write-Step "Building ggml DLLs with MSVC + CUDA (arch=$arch, jobs=$Jobs)..."

    Assert-Command "nvcc" "Install CUDA Toolkit: https://developer.nvidia.com/cuda-downloads"
    Assert-Command "cmake" "Install CMake: https://cmake.org/download/"

    Import-VsEnv

    # Import-VsEnv overwrites PATH — re-add MinGW for ninja (append so system cmake takes priority)
    if (-not (Get-Command "ninja" -ErrorAction SilentlyContinue)) {
        if (Test-Path (Join-Path $MinGW "ninja.exe")) {
            $env:PATH = "$env:PATH;$MinGW"
        } else {
            Write-Err "ninja not found. Install via MSYS2: pacman -S mingw-w64-x86_64-ninja"
        }
    }

    Set-Location $LlamaGoDir
    $cudaArchArg = "-DCMAKE_CUDA_ARCHITECTURES=$arch"
    cmake -S llama.cpp -B build-cuda-dll -G Ninja `
        -DCMAKE_BUILD_TYPE=Release `
        -DBUILD_SHARED_LIBS=ON `
        -DGGML_CUDA=ON `
        -DGGML_CUDA_FA_ALL_QUANTS=ON `
        -DGGML_CUDA_GRAPHS=ON `
        $cudaArchArg `
        -DLLAMA_CURL=OFF
    if ($LASTEXITCODE -ne 0) { Write-Err "cmake configure failed (exit code $LASTEXITCODE)" }

    cmake --build build-cuda-dll --config Release --target ggml ggml-cpu ggml-cuda -j $Jobs
    if ($LASTEXITCODE -ne 0) { Write-Err "cmake build failed (exit code $LASTEXITCODE)" }

    Set-Location $ScriptDir
    Write-OK "ggml DLLs built"
}

# -- Build: llama/common static libs with MinGW ------------------------------

function Build-MinGWLibs {
    Write-Step "Building llama/common with MinGW (jobs=$Jobs)..."

    $gcc = Join-Path $MinGW "gcc.exe"
    if (-not (Test-Path $gcc)) { Write-Err "MinGW gcc not found at $gcc - install MSYS2 mingw-w64-x86_64-gcc" }

    $env:PATH = "$MinGW;$env:PATH"

    Set-Location $LlamaGoDir
    cmake -S llama.cpp -B build-mingw -G "MinGW Makefiles" `
        -DCMAKE_BUILD_TYPE=Release `
        -DBUILD_SHARED_LIBS=OFF `
        -DGGML_CUDA=OFF `
        -DLLAMA_CURL=OFF `
        -DGGML_NATIVE=ON `
        "-DCMAKE_CXX_FLAGS=-D_WIN32_WINNT=0x0A00" `
        "-DCMAKE_C_FLAGS=-D_WIN32_WINNT=0x0A00"
    if ($LASTEXITCODE -ne 0) { Write-Err "cmake configure failed (exit code $LASTEXITCODE)" }

    mingw32-make -C build-mingw -j $Jobs ggml llama common mtmd
    if ($LASTEXITCODE -ne 0) { Write-Err "mingw32-make failed (exit code $LASTEXITCODE)" }

    Set-Location $ScriptDir
    Write-OK "MinGW static libs built"
}

# -- Assemble: collect libs + import stubs ------------------------------------

function Assemble-Libs {
    Write-Step "Assembling libraries..."

    $env:PATH = "$MinGW;$env:PATH"
    Set-Location $LlamaGoDir

    # MinGW-built static libs
    Copy-Item build-mingw\common\libcommon.a   . -Force
    Copy-Item build-mingw\src\libllama.a       . -Force
    Copy-Item build-mingw\ggml\src\ggml.a      libggml.a      -Force
    Copy-Item build-mingw\ggml\src\ggml-base.a libggml-base.a -Force
    Copy-Item build-mingw\ggml\src\ggml-cpu.a  libggml-cpu.a  -Force

    # MSVC-built DLLs
    foreach ($dll in @("ggml-base", "ggml-cpu", "ggml-cuda", "ggml")) {
        Copy-Item "build-cuda-dll\bin\$dll.dll" . -Force
        # Generate MinGW import libs from DLLs
        # gendef/dlltool write info to stderr which PowerShell treats as errors
        # even with 2>&1; use cmd /c to fully isolate stderr handling.
        cmd /c "gendef.exe `"$dll.dll`" >nul 2>&1"
        cmd /c "dlltool.exe -d `"$dll.def`" -l `"lib$dll.a`" -D `"$dll.dll`" >nul 2>&1"
    }

    # CUDA runtime import libs (renamed for MinGW -l flag)
    $cudaLib = "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.9\lib\x64"
    if (-not (Test-Path $cudaLib)) {
        # Try to find any installed CUDA version
        $cudaLib = Get-ChildItem "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA" -Directory |
            Sort-Object Name -Descending | Select-Object -First 1
        if ($cudaLib) { $cudaLib = Join-Path $cudaLib.FullName "lib\x64" }
    }
    if (-not (Test-Path $cudaLib)) { Write-Err "CUDA lib directory not found" }

    Copy-Item "$cudaLib\cublas.lib"  libcublas.a  -Force
    Copy-Item "$cudaLib\cudart.lib"  libcudart.a  -Force
    Copy-Item "$cudaLib\cuda.lib"    libcuda.a    -Force

    # Compile wrapper.cpp with MinGW (CGO bridge)
    # Note: -I paths must be quoted to prevent PowerShell from misinterpreting forward slashes
    Write-Host "    Compiling wrapper.cpp..."
    & "$MinGW\g++.exe" -c -O3 -DNDEBUG "-std=c++17" -DGGML_USE_CUDA "-D_WIN32_WINNT=0x0A00" `
        "-I." "-Illama.cpp" "-Illama.cpp/common" "-Illama.cpp/ggml/include" `
        "-Illama.cpp/include" "-Illama.cpp/vendor" "-Illama.cpp/tools/mtmd" `
        wrapper.cpp -o wrapper_mingw.o
    if ($LASTEXITCODE -ne 0) { Write-Err "g++ wrapper compilation failed (exit code $LASTEXITCODE)" }
    & "$MinGW\ar.exe" crs libbinding.a wrapper_mingw.o

    # mtmd (multimodal/vision) static lib
    if (Test-Path "build-mingw\tools\mtmd\libmtmd.a") {
        Copy-Item build-mingw\tools\mtmd\libmtmd.a . -Force
    }

    Set-Location $ScriptDir
    Write-OK "Libraries assembled"
}

# -- Copy runtime DLLs --------------------------------------------------------

function Copy-RuntimeDlls {
    # ggml DLLs (CUDA inference — must be alongside the binary)
    foreach ($dll in @("ggml-base", "ggml-cpu", "ggml-cuda", "ggml")) {
        $src = "$LlamaGoDir\$dll.dll"
        if (Test-Path $src) {
            Copy-Item $src $BinDir -Force
        }
    }
    # MinGW C++ runtime is statically linked via -static flags, no DLLs needed.
}

# -- Generate: templ + Tailwind -----------------------------------------------

function Build-Web {
    Write-Step "Generating web assets..."

    # templ generate — use -f per file to bypass the hash cache
    if (Get-Command "templ" -ErrorAction SilentlyContinue) {
        Get-ChildItem -Path ".\internal\web\templates" -Filter "*.templ" | ForEach-Object {
            templ generate -f $_.FullName
            if ($LASTEXITCODE -ne 0) { Write-Err "templ generate failed for $($_.Name) (exit code $LASTEXITCODE)" }
        }
        Write-OK "templ generated"
    } else {
        Write-Host "    templ not found, skipping template generation"
    }

    # Tailwind CSS (via bun or npx)
    $webDir = Join-Path $ScriptDir "web"
    if (Test-Path (Join-Path $webDir "package.json")) {
        Set-Location $webDir
        if (Get-Command "bun" -ErrorAction SilentlyContinue) {
            bun install --frozen-lockfile 2>$null
            bun run build:css
        } elseif (Get-Command "npx" -ErrorAction SilentlyContinue) {
            $ErrorActionPreference = 'SilentlyContinue'
            npm install 2>&1 | Out-Null
            $ErrorActionPreference = 'Continue'
            npx postcss input.tw.css -o public/dist.css --config postcss.config.js
        } else {
            Write-Host "    No bun/npx found, skipping CSS build"
        }
        Set-Location $ScriptDir
        if ($LASTEXITCODE -eq 0) { Write-OK "Tailwind CSS built" }
    }
}

# -- Build: mass.exe (unified binary) -----------------------------------------

function Build-Mass {
    Build-Web

    Write-Step "Building mass.exe..."

    $env:PATH            = "$MinGW;$env:PATH"
    $env:CGO_ENABLED     = "1"
    $env:C_INCLUDE_PATH  = $LlamaGoDir
    $env:LIBRARY_PATH    = $LlamaGoDir
    $env:CGO_LDFLAGS     = "-L$LlamaGoDir -static -static-libgcc -static-libstdc++"

    New-Item -ItemType Directory -Path $BinDir -Force | Out-Null

    Set-Location $ScriptDir
    go build -a -tags cublas -p $Jobs -ldflags='-H windowsgui -extldflags "-static"' -o "$BinDir\mass.exe" .\cmd\mass
    if ($LASTEXITCODE -ne 0) { Write-Err "go build failed (exit code $LASTEXITCODE)" }

    Copy-RuntimeDlls

    $size = [math]::Round((Get-Item "$BinDir\mass.exe").Length / 1MB, 1)
    Write-OK "Built: bin\mass.exe ($size MB)"
}

# -- Commands -----------------------------------------------------------------

function Show-Help {
    Write-Host @"

  MASS Windows Build
  ==================

  Usage: .\make-win.ps1 <command> [options]

  Commands:
    build              Full build (CUDA DLLs + MinGW libs + web assets + mass.exe)
    build-libs         Build all native libs (MSVC DLLs + MinGW static)
    build-libs-gpu     Build ggml DLLs only (MSVC + CUDA)
    build-libs-mingw   Build llama/common only (MinGW)
    run                Build (if needed) and run mass.exe (web UI, opens browser)
    clean              Remove bin/ directory
    clean-all          Remove bin/ and llama-go build directories
    help               Show this help

  Use 'mass.exe -headless' to run without the webview window or browser.

  The resulting binary uses GPU when available, falls back to CPU otherwise.

  Options:
    -CudaArch <list>    CUDA architectures, semicolon-separated (default: all common)
                        Default: 61;75;86;89;90;120
                        61=GTX10xx  75=RTX20xx  86=RTX30xx  89=RTX40xx  90=H100  120=RTX50xx
    -Config <path>     Config file for 'run' (default: `$MASS_CONFIG or config\dev.yml)
    -Jobs <n>          Parallel jobs (default: all logical CPUs)

  Environment:
    MASS_CONFIG         Config file path (default: config\dev.yml)
    LLAMA_GO_DIR       Path to llama-go (default: ..\llama-go)

  Prerequisites:
    - MSYS2 MinGW-w64: pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-ninja
    - CUDA Toolkit:    https://developer.nvidia.com/cuda-downloads
    - Visual Studio:   2022 Community, "Desktop C++" workload
    - Go:              https://go.dev/dl/

  See WINDOWS_GPU_BUILD.md for details.

"@
}

function Invoke-Clean {
    Write-Step "Cleaning..."
    if (Test-Path $BinDir) { Remove-Item -Recurse -Force $BinDir }
    Write-OK "Cleaned bin/"
}

function Invoke-CleanAll {
    Invoke-Clean
    $dirs = @("build-cuda-dll", "build-mingw")
    foreach ($d in $dirs) {
        $p = Join-Path $LlamaGoDir $d
        if (Test-Path $p) {
            Write-Host "    Removing $d..."
            Remove-Item -Recurse -Force $p
        }
    }
    # Remove assembled libs
    Get-ChildItem $LlamaGoDir -Filter "*.a"   -File -ErrorAction SilentlyContinue | Remove-Item -Force
    Get-ChildItem $LlamaGoDir -Filter "*.dll" -File -ErrorAction SilentlyContinue | Remove-Item -Force
    Get-ChildItem $LlamaGoDir -Filter "*.def" -File -ErrorAction SilentlyContinue | Remove-Item -Force
    Get-ChildItem $LlamaGoDir -Filter "*.obj" -File -ErrorAction SilentlyContinue | Remove-Item -Force
    Get-ChildItem $LlamaGoDir -Filter "wrapper_mingw.o" -File -ErrorAction SilentlyContinue | Remove-Item -Force
    Write-OK "All cleaned"
}

# -- Check if libs already exist ----------------------------------------------

function Test-GgmlDlls {
    $needed = @("ggml-base.dll", "ggml-cpu.dll", "ggml-cuda.dll", "ggml.dll")
    foreach ($f in $needed) {
        if (-not (Test-Path (Join-Path $LlamaGoDir $f))) { return $false }
    }
    return $true
}

function Test-MinGWLibs {
    $needed = @("libcommon.a", "libllama.a", "libbinding.a")
    foreach ($f in $needed) {
        if (-not (Test-Path (Join-Path $LlamaGoDir $f))) { return $false }
    }
    return $true
}

# -- Main ---------------------------------------------------------------------

Ensure-LlamaGo

switch ($Command) {
    "build" {
        if (-not (Test-GgmlDlls)) { Build-GgmlDlls } else { Write-Step "ggml DLLs already built, skipping (use clean-all to rebuild)" }
        if (-not (Test-MinGWLibs)) { Build-MinGWLibs }  else { Write-Step "MinGW libs already built, skipping (use clean-all to rebuild)" }
        Assemble-Libs
        Build-Mass
    }
    "build-libs" {
        Build-GgmlDlls
        Build-MinGWLibs
        Assemble-Libs
    }
    "build-libs-gpu" {
        Build-GgmlDlls
    }
    "build-libs-mingw" {
        Build-MinGWLibs
        Assemble-Libs
    }
    "run" {
        if (-not (Test-GgmlDlls)) { Build-GgmlDlls }
        if (-not (Test-MinGWLibs)) { Build-MinGWLibs }
        Assemble-Libs
        Build-Mass
        Write-Step "Starting mass (web UI)..."
        & "$BinDir\mass.exe"
    }
    "clean"     { Invoke-Clean }
    "clean-all" { Invoke-CleanAll }
    "help"      { Show-Help }
}
