param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [int]$LookbackDays = 2
)

$ErrorActionPreference = "Stop"

Set-Location $ProjectRoot

$endUtc = [DateTime]::UtcNow
$startUtc = $endUtc.AddDays(-$LookbackDays)
$startIso = $startUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")
$endIso = $endUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")

$plfExe = Join-Path $ProjectRoot ".venv\\Scripts\\plf.exe"
$uvExe = Join-Path $ProjectRoot ".venv\\Scripts\\uv.exe"

if (Test-Path $plfExe) {
    $command = $plfExe
    $baseArgs = @()
} elseif (Test-Path $uvExe) {
    $command = $uvExe
    $baseArgs = @("run", "plf")
} else {
    throw "Neither .venv\\Scripts\\plf.exe nor .venv\\Scripts\\uv.exe was found under $ProjectRoot. Create the project virtual environment first."
}

Write-Host "Importing load data from $startIso to $endIso"

& $command @baseArgs load import `
    --start $startIso `
    --end $endIso

if ($LASTEXITCODE -ne 0) {
    throw "Load import failed with exit code $LASTEXITCODE."
}

Write-Host "Load import completed."
