# codebase-memory-cli setup script (Windows)
# Default: download pre-built native Windows binary
# -FromSource: build a native Windows executable with MSYS2/Clang

param(
    [switch]$FromSource,
    [switch]$Help
)

$ErrorActionPreference = "Stop"

$Repo = "DeusData/codebase-memory-mcp"
$BinaryName = "codebase-memory-cli"
$InstallDir = Join-Path $env:LOCALAPPDATA "codebase-memory-cli"

# --- Helpers ---

function Write-Ok($msg)   { Write-Host "  $msg" -ForegroundColor Green }
function Write-Fail($msg)  { Write-Host "  $msg" -ForegroundColor Red }
function Write-Warn($msg)  { Write-Host "  $msg" -ForegroundColor Yellow }

function Write-AgentIntegrationGuidance($Command) {
    Write-Host ""
    Write-Host "  Codebase Memory is CLI-first; this setup script does not write MCP client configuration." -ForegroundColor White
    Write-Host "  To install CLI-first skills/instructions/hooks for detected agents, run:" -ForegroundColor White
    Write-Host ""
    Write-Host "    $Command install --skip-binary" -ForegroundColor Yellow
}


# --- Main ---

if ($Help) {
    Write-Host ""
    Write-Host "Usage: .\setup-windows.ps1 [-FromSource] [-Help]"
    Write-Host ""
    Write-Host "  Default:      Download pre-built Windows binary"
    Write-Host "  -FromSource:  Build a native Windows .exe with MSYS2/Clang (no WSL)"
    Write-Host ""
    exit 0
}

Write-Host ""
Write-Host "codebase-memory-cli installer (Windows)" -ForegroundColor White
Write-Host ""

if ($FromSource) {
    # --- Native Windows source build via MSYS2/Clang ---
    $git = Get-Command git.exe -ErrorAction SilentlyContinue
    if (-not $git) {
        Write-Fail "git.exe is required for -FromSource."
        exit 1
    }

    $msysRoot = if ($env:MSYS2_ROOT) { $env:MSYS2_ROOT } else { "C:\msys64" }
    $buildHelper = Join-Path $msysRoot "usr\bin\make.exe"
    if (-not (Test-Path -LiteralPath $buildHelper)) {
        Write-Fail "MSYS2 was not found at $msysRoot."
        Write-Host "  Install MSYS2 and its CLANG64 Clang/zlib + make packages, then retry." -ForegroundColor Yellow
        Write-Host "  This source-build path intentionally does not use WSL." -ForegroundColor Yellow
        exit 1
    }

    $sourceDir = Join-Path $env:LOCALAPPDATA "codebase-memory-cli-src"
    if (Test-Path -LiteralPath (Join-Path $sourceDir ".git")) {
        Write-Host "Updating source..." -ForegroundColor White
        & $git.Source -C $sourceDir pull --ff-only
        if ($LASTEXITCODE -ne 0) { throw "git pull failed" }
    } else {
        Write-Host "Cloning repository..." -ForegroundColor White
        & $git.Source clone "https://github.com/$Repo.git" $sourceDir
        if ($LASTEXITCODE -ne 0) { throw "git clone failed" }
    }
    Write-Ok "Source at $sourceDir"

    Write-Host "Building native Windows executable..." -ForegroundColor White
    & (Join-Path $sourceDir "scripts\build-windows.ps1") -Msys2Root $msysRoot
    if ($LASTEXITCODE -ne 0) { throw "native Windows source build failed" }

    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }
    $builtBinary = Join-Path $sourceDir "build\c\codebase-memory-cli.exe"
    $binaryPath = Join-Path $InstallDir "$BinaryName.exe"
    Copy-Item -LiteralPath $builtBinary -Destination $binaryPath -Force
    Write-Ok "Installed native executable to $binaryPath"

    $verOut = & $binaryPath --version 2>&1
    if ($LASTEXITCODE -ne 0) { throw "installed executable failed --version" }
    Write-Ok "Version: $verOut"
    Write-AgentIntegrationGuidance ('"' + $binaryPath + '"')

    Write-Host ""
    Write-Ok "Done! Try: $binaryPath --help"
    Write-Host ""
    Write-Host "  To uninstall:" -ForegroundColor White
    Write-Host "    Remove-Item -Recurse -Force '$InstallDir'"
    Write-Host "    Remove-Item -Recurse -Force '$sourceDir'"

} else {
    # --- Download pre-built native Windows binary ---
    Write-Host "Fetching latest release..." -ForegroundColor White

    $releaseUrl = "https://api.github.com/repos/$Repo/releases/latest"
    $release = Invoke-RestMethod -Uri $releaseUrl -Headers @{ "User-Agent" = "codebase-memory-cli-setup" }
    $tag = $release.tag_name

    if (-not $tag) {
        Write-Fail "Could not determine latest release."
        Write-Host "  Check: https://github.com/$Repo/releases"
        exit 1
    }
    Write-Ok "Latest release: $tag"

    $asset = "codebase-memory-cli-windows-amd64.zip"
    $downloadUrl = "https://github.com/$Repo/releases/download/$tag/$asset"

    Write-Host "Downloading $asset..." -ForegroundColor White

    # Create install directory
    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }

    $tmpZip = Join-Path $env:TEMP $asset
    Invoke-WebRequest -Uri $downloadUrl -OutFile $tmpZip -UseBasicParsing

    # Extract
    Expand-Archive -Path $tmpZip -DestinationPath $InstallDir -Force
    Remove-Item $tmpZip -Force

    $binaryPath = Join-Path $InstallDir "$BinaryName.exe"

    if (-not (Test-Path $binaryPath)) {
        Write-Fail "Binary not found at $binaryPath after extraction"
        exit 1
    }
    Write-Ok "Installed to $binaryPath"

    # Verify binary runs
    try {
        $verOut = & $binaryPath --version 2>&1
        Write-Ok "Version: $verOut"
    } catch {
        Write-Warn "Could not verify binary version (may still work)"
    }

    # SmartScreen note
    Write-Host ""
    Write-Warn "Windows SmartScreen may show a warning when the binary runs for the first time."
    Write-Host "    This is normal for unsigned open-source binaries." -ForegroundColor Yellow
    Write-Host "    Click 'More info' then 'Run anyway' to proceed." -ForegroundColor Yellow
    Write-Host "    Verify checksums at: https://github.com/$Repo/releases" -ForegroundColor Yellow

    # Check if install dir is on PATH
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($userPath -notlike "*$InstallDir*") {
        Write-Host ""
        Write-Warn "$InstallDir is not on your PATH."
        $addPath = Read-Host "  Add it to your user PATH? [y/N]"
        if ($addPath -match '^[Yy]$') {
            [Environment]::SetEnvironmentVariable("Path", "$userPath;$InstallDir", "User")
            Write-Ok "Added to user PATH (restart your terminal to take effect)"
        }
    }

    Write-AgentIntegrationGuidance ('"' + $binaryPath + '"')

    Write-Host ""
    Write-Ok "Done! Try: $binaryPath --help"
    Write-Host ""
    Write-Host "  To uninstall:" -ForegroundColor White
    Write-Host "    Remove-Item -Recurse -Force '$InstallDir'"
    Write-Host "    Remove-Item -Recurse -Force `"$env:LOCALAPPDATA\codebase-memory-mcp`"  # graph database"
}
