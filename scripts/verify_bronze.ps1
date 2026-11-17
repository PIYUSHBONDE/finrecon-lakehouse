param(
    [string]$SourceReport,
    [string]$Dataset = 'portfolio_demo'
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repositoryRoot
try {
    if (-not $SourceReport) {
        $latest = Get-ChildItem -Path 'data_generator/output/correlated_*/live/verification.json' -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1
        if (-not $latest) {
            throw 'Create a correlated fixture first: .venv/Scripts/python.exe -m data_generator.verify_sources'
        }
        $SourceReport = $latest.FullName
    }
    $sourcePath = (Resolve-Path -LiteralPath $SourceReport).Path
    $repositoryPrefix = $repositoryRoot.TrimEnd('\') + '\'
    if (-not $sourcePath.StartsWith($repositoryPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw 'The source report must be inside this repository so Docker can read it.'
    }
    if ($Dataset -notmatch '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$') {
        throw 'Dataset must be a simple name of 1-64 letters, digits, underscores, or hyphens.'
    }
    $containerReport = '/workspace/' + $sourcePath.Substring($repositoryPrefix.Length).Replace('\', '/')
    docker compose -f docker/docker-compose.yml --profile compute build bronze
    if ($LASTEXITCODE -ne 0) { throw 'Local Spark image build failed.' }
    for ($pass = 1; $pass -le 2; $pass++) {
        Write-Output "Verifying Bronze in fresh container, pass $pass/2"
        docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze `
            python3 -m pipelines.verify_bronze --source-report $containerReport --dataset $Dataset
        if ($LASTEXITCODE -ne 0) { throw "Bronze verification failed on pass $pass." }
    }
    Get-Content -LiteralPath "pipelines/output/$Dataset/verification.json"
}
finally {
    Pop-Location
}
