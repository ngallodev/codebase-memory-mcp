<#
.SYNOPSIS
    Build codebase-memory-cli as a native Windows executable.

.DESCRIPTION
    Uses the native MSYS2 LLVM/Clang toolchain. No WSL is used. On x64 hosts
    the CLANG64 environment is selected; on ARM64 hosts CLANGARM64 is selected.
    The output is build\c\codebase-memory-cli.exe and is suitable for Windows
    10/11 on the matching architecture.

    Required MSYS2 packages (x64):
      mingw-w64-clang-x86_64-clang mingw-w64-clang-x86_64-zlib make

    Required MSYS2 packages (ARM64):
      mingw-w64-clang-aarch64-clang mingw-w64-clang-aarch64-zlib make
#>
[CmdletBinding()]
param(
    [ValidateSet("cbm-with-ui", "cbm")]
    [string]$Target = "cbm-with-ui",
    [string]$Msys2Root = $(if ($env:MSYS2_ROOT) { $env:MSYS2_ROOT } else { "C:\msys64" }),
    [int]$Jobs = 0
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$arch = $env:PROCESSOR_ARCHITECTURE
if ($arch -eq "ARM64") {
    $toolchain = "clangarm64"
} else {
    $toolchain = "clang64"
}

$toolBin = Join-Path $Msys2Root "$toolchain\bin"
$usrBin = Join-Path $Msys2Root "usr\bin"
$clang = Join-Path $toolBin "clang.exe"
$clangxx = Join-Path $toolBin "clang++.exe"
$make = Join-Path $usrBin "make.exe"
$zlib = Join-Path $Msys2Root "$toolchain\lib\libz.a"

foreach ($required in @($clang, $clangxx, $make, $zlib)) {
    if (-not (Test-Path -LiteralPath $required)) {
        throw "native Windows build prerequisite missing: $required. Install the MSYS2 $toolchain Clang/zlib toolchain and make."
    }
}

if ($Jobs -le 0) {
    $Jobs = [Math]::Max(1, [Environment]::ProcessorCount)
}

$oldPath = $env:PATH
try {
    # GNU make recipes expect the MSYS2 Unix utilities; the compiler and linker
    # themselves remain native Windows programs and Makefile.cbm detects _WIN32.
    $env:PATH = "$toolBin;$usrBin;$oldPath"
    $cc = $clang -replace '\\', '/'
    $cxx = $clangxx -replace '\\', '/'
    Write-Host "Building native Windows codebase-memory-cli ($toolchain, $Target)..." -ForegroundColor Cyan
    & $make "-j$Jobs" "-f" "Makefile.cbm" $Target "SANITIZE=" "CC=$cc" "CXX=$cxx"
    if ($LASTEXITCODE -ne 0) {
        throw "native Windows build failed (exit $LASTEXITCODE)"
    }
} finally {
    $env:PATH = $oldPath
}

$binary = Join-Path $repoRoot "build\c\codebase-memory-cli.exe"
if (-not (Test-Path -LiteralPath $binary)) {
    throw "build completed without producing $binary"
}

$version = & $binary --version 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "built executable failed --version (exit $LASTEXITCODE): $version"
}
Write-Host "Built: $binary" -ForegroundColor Green
Write-Host "$version" -ForegroundColor Green
