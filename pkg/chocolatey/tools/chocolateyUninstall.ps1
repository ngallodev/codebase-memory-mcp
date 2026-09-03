$ErrorActionPreference = 'Stop'

$packageName = 'codebase-memory-cli'
$installDir  = Join-Path $env:ChocolateyBinRoot $packageName

Uninstall-BinFile -Name 'codebase-memory-cli'

if (Test-Path $installDir) {
  Remove-Item $installDir -Recurse -Force
}
