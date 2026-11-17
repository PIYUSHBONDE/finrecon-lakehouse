param(
    [string]$Dataset = 'portfolio_demo',
    [string]$AsOfDate
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repositoryRoot
try {
    if ($Dataset -notmatch '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$') {
        throw 'Invalid dataset name.'
    }
    if (-not (Test-Path -LiteralPath "pipelines/output/$Dataset/verification.json")) {
        throw 'Verify Bronze for this dataset first with scripts/verify_bronze.ps1.'
    }
    $dateArguments = @()
    if ($AsOfDate) {
        $dateArguments = @('--as-of-date', $AsOfDate)
    }
    docker compose -f docker/docker-compose.yml --profile compute build bronze
    if ($LASTEXITCODE -ne 0) { throw 'Local Spark image build failed.' }
    for ($pass = 1; $pass -le 2; $pass++) {
        Write-Output "Verifying Silver in fresh container, pass $pass/2"
        docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze `
            python3 -m pipelines.verify_silver --dataset $Dataset @dateArguments
        if ($LASTEXITCODE -ne 0) { throw "Silver verification failed on pass $pass." }
    }
    Get-Content -LiteralPath "pipelines/output/$Dataset/silver_verification.json"
}
finally {
    Pop-Location
}
