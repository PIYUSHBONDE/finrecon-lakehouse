param([string]$Dataset = 'portfolio_demo')

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repositoryRoot
try {
    if ($Dataset -notmatch '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$') {
        throw 'Invalid dataset name.'
    }
    if (-not (Test-Path -LiteralPath "pipelines/output/$Dataset/verification.json")) {
        throw 'Verify Bronze for this dataset first.'
    }
    for ($pass = 1; $pass -le 2; $pass++) {
        Write-Output "Verifying CDC/SCD-2 in fresh container, pass $pass/2"
        docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze `
            python3 -m pipelines.verify_cdc --dataset $Dataset
        if ($LASTEXITCODE -ne 0) { throw "CDC verification failed on pass $pass." }
    }
    Get-Content -LiteralPath "pipelines/output/$Dataset/cdc_verification.json"
}
finally {
    Pop-Location
}
