param(
    [int[]]$Rows = @(10000, 100000, 1000000),
    [ValidateRange(1, 5)][int]$Trials = 3
)
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $rowArguments = @('--rows') + @($Rows | ForEach-Object { $_.ToString() })
    docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze `
        python3 -m pipelines.benchmark_skew @rowArguments --trials $Trials
    if ($LASTEXITCODE -ne 0) { throw 'Skew benchmark failed. Inspect the printed report directory.' }
}
finally { Pop-Location }
