param(
    [ValidateSet('fast', 'full')][string]$Profile = 'fast',
    [string]$Python = 'python'
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ') + '_' + [Guid]::NewGuid().ToString('N').Substring(0, 8)
$reportRoot = Join-Path $repositoryRoot "pipelines/output/ci/$runId"
Push-Location $repositoryRoot
$previousReportDirectory = $env:FINRECON_CI_REPORT_DIR
try {
    New-Item -ItemType Directory -Path $reportRoot > $null
    Write-Output "CI reports: $reportRoot"
    & $Python -m pipelines.ci --profile fast --report-dir (Join-Path $reportRoot 'fast')
    if ($LASTEXITCODE -ne 0) { throw 'Fast CI failed. Inspect its ci.json and logs.' }
    if ($Profile -eq 'full') {
        $env:FINRECON_CI_REPORT_DIR = $reportRoot.Replace('\', '/')
        docker compose -p finrecon-ci -f docker/docker-compose.ci.yml build spark-tests
        if ($LASTEXITCODE -ne 0) { throw 'CI image build failed.' }
        docker compose -p finrecon-ci -f docker/docker-compose.ci.yml run --rm --no-deps spark-tests
        if ($LASTEXITCODE -ne 0) { throw 'Spark CI failed. Inspect its ci.json and suite logs.' }
    }
    if ($env:GITHUB_STEP_SUMMARY) {
        "CI profile '$Profile' passed. Reports remain on this runner at '$reportRoot'." | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY
    }
}
finally {
    $env:FINRECON_CI_REPORT_DIR = $previousReportDirectory
    Pop-Location
}
