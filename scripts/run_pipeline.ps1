param(
    [Parameter(Mandatory = $true)][string]$GatewayTopics,
    [Parameter(Mandatory = $true)][string]$BankDirectory,
    [string]$Dataset = 'local',
    [string]$AsOfDate = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd'),
    [int]$MaxAgeHours = 72,
    [string]$FxRatesFile
)

$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $fxArguments = @()
    if ($FxRatesFile) { $fxArguments = @('--fx-rates', $FxRatesFile) }
    docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze `
        python3 -m pipelines.orchestrate --dataset $Dataset --gateway-topics $GatewayTopics `
        --bank-dir $BankDirectory --as-of-date $AsOfDate --max-age-hours $MaxAgeHours @fxArguments
    if ($LASTEXITCODE -ne 0) { throw 'Pipeline failed. Inspect the printed run report and stage logs.' }
}
finally { Pop-Location }
