param([string]$Dataset = 'portfolio_demo')
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    if ($Dataset -notmatch '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$') { throw 'Invalid dataset name.' }
    for ($pass = 1; $pass -le 2; $pass++) {
        docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze `
            python3 -m pipelines.verify_exceptions --dataset $Dataset
        if ($LASTEXITCODE -ne 0) { throw "Exception verification failed on pass $pass." }
    }
    Get-Content -LiteralPath "pipelines/output/$Dataset/exception_verification.json"
}
finally { Pop-Location }
