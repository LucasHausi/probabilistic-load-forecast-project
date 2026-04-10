param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$AreaCode = "AT",
    [int]$ForecastDays = 1,
    [string[]]$Variables = @("t2m", "u10", "v10", "tp", "ssrd")
)

$ErrorActionPreference = "Stop"

Set-Location $ProjectRoot

$startUtc = [DateTime]::UtcNow.Date
$endUtc = $startUtc.AddDays($ForecastDays)
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

foreach ($variable in $Variables) {
    Write-Host "Importing $variable for $AreaCode from $startIso to $endIso"

    & $command @baseArgs weather import-forecast `
        --start $startIso `
        --end $endIso `
        --variable $variable `
        --area-code $AreaCode

    if ($LASTEXITCODE -ne 0) {
        throw "Forecast import failed for variable $variable with exit code $LASTEXITCODE."
    }
}

Write-Host "Weather forecast import completed."
