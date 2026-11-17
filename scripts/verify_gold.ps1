param([string]$Dataset = 'portfolio_demo')

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repositoryRoot
try {
    if ($Dataset -notmatch '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$') {
        throw 'Invalid dataset name.'
    }
    foreach ($proof in @('verification.json', 'silver_verification.json', 'cdc_verification.json')) {
        if (-not (Test-Path -LiteralPath "pipelines/output/$Dataset/$proof")) {
            throw "Verify the preceding stages first: missing $proof."
        }
    }
    for ($pass = 1; $pass -le 2; $pass++) {
        Write-Output "Verifying Gold in fresh container, pass $pass/2"
        docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze `
            python3 -m pipelines.verify_gold --dataset $Dataset
        if ($LASTEXITCODE -ne 0) { throw "Gold verification failed on pass $pass." }
    }
    Get-Content -LiteralPath "pipelines/output/$Dataset/gold_verification.json"
}
finally {
    Pop-Location
}
