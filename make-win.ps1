#Requires -Version 5.1
<#
.SYNOPSIS
    Build MASS on native Windows.

.DESCRIPTION
    MASS is pure Go (no CGO). Inference runs on workers (mass-worker-llama
    etc.) that connect over gRPC; install those separately.

.EXAMPLE
    .\make-win.ps1 build
    .\make-win.ps1 run
    .\make-win.ps1 clean
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet("build", "build-web", "run", "clean", "help")]
    [string]$Command = "help",

    [Parameter()]
    [string]$Config = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve config: -Config flag > MASS_CONFIG env > default
if (-not $Config) {
    $Config = if ($env:MASS_CONFIG) { $env:MASS_CONFIG } else { "config\dev.yml" }
}

$RepoRoot = $PSScriptRoot
$BinDir   = Join-Path $RepoRoot "bin"
$Binary   = Join-Path $BinDir "mass.exe"

function Write-Step  { param([string]$Msg) Write-Host "==> $Msg" -ForegroundColor Cyan }
function Write-Info  { param([string]$Msg) Write-Host "    $Msg" -ForegroundColor Gray }

function Get-Version {
    try {
        $v = git describe --tags --always --dirty 2>$null
        if ($LASTEXITCODE -eq 0 -and $v) { return $v }
    } catch {}
    return "dev"
}

function Invoke-BuildWeb {
    Write-Step "Generating web assets..."

    $templ = Get-Command templ -ErrorAction SilentlyContinue
    if ($templ) {
        Get-ChildItem "internal\web\templates\*.templ" -ErrorAction SilentlyContinue | ForEach-Object {
            & templ generate -f $_.FullName
        }
        Write-Info "templ generated"
    }

    $webPkg = Join-Path $RepoRoot "web\package.json"
    if (Test-Path $webPkg) {
        Push-Location (Join-Path $RepoRoot "web")
        try {
            $bun = Get-Command bun -ErrorAction SilentlyContinue
            if ($bun) {
                & bun install --frozen-lockfile 2>$null
                & bun run build:css
            } else {
                $npx = Get-Command npx -ErrorAction SilentlyContinue
                if ($npx) {
                    & npm install 2>$null
                    & npx postcss input.tw.css -o public/dist.css --config postcss.config.js
                }
            }
            Write-Info "CSS built"
        } finally {
            Pop-Location
        }
    }
}

function Invoke-Build {
    Invoke-BuildWeb

    $version = Get-Version
    Write-Step "Building mass.exe ($version)..."
    if (-not (Test-Path $BinDir)) { New-Item -ItemType Directory -Path $BinDir | Out-Null }

    # -H windowsgui marks the binary as a GUI app so Windows doesn't allocate
    # a console for it. The --headless flag re-attaches to the parent console
    # at runtime via attachOrAllocConsole (cmd/mass/console_windows.go).
    $env:CGO_ENABLED = "0"
    & go build -ldflags "-H windowsgui -X main.version=$version" -o $Binary ".\cmd\mass"
    if ($LASTEXITCODE -ne 0) { throw "go build failed" }

    $size = [math]::Round((Get-Item $Binary).Length / 1MB, 1)
    Write-Info "Built: bin\mass.exe ($size MB)"
}

function Invoke-Run {
    Invoke-Build
    Write-Step "Starting mass..."
    & $Binary
}

function Invoke-Clean {
    if (Test-Path $BinDir) {
        Remove-Item -Recurse -Force $BinDir
        Write-Info "Removed bin\"
    }
}

function Show-Help {
    Write-Host ""
    Write-Host "  MASS Build System (pure Go - workers handle inference)"
    Write-Host "  ======================================================"
    Write-Host ""
    Write-Host "  Usage: .\make-win.ps1 <command>"
    Write-Host ""
    Write-Host "  Commands:"
    Write-Host "    build       Build mass.exe (web assets + Go build)"
    Write-Host "    build-web   Generate web assets only"
    Write-Host "    run         Build and start mass"
    Write-Host "    clean       Remove build outputs"
    Write-Host ""
    Write-Host "  Note: MASS no longer links llama.cpp. Install and run"
    Write-Host "  mass-worker-<runtime> (e.g. mass-worker-llama) separately"
    Write-Host "  to provide inference capacity."
    Write-Host ""
}

switch ($Command) {
    "build"     { Invoke-Build }
    "build-web" { Invoke-BuildWeb }
    "run"       { Invoke-Run }
    "clean"     { Invoke-Clean }
    "help"      { Show-Help }
    default     { Show-Help }
}
