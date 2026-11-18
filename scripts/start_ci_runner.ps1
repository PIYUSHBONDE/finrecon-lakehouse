param(
    [string]$RunnerDirectory = (Join-Path $PSScriptRoot '../pipelines/output/actions-runner')
)

$ErrorActionPreference = 'Stop'
$runnerRoot = (Resolve-Path -LiteralPath $RunnerDirectory).Path
$configuration = Join-Path $runnerRoot '.runner'
$entrypoint = Join-Path $runnerRoot 'run.cmd'
$listener = Join-Path $runnerRoot 'bin/Runner.Listener.exe'
if (-not (Test-Path -LiteralPath $configuration) -or -not (Test-Path -LiteralPath $entrypoint)) {
    throw 'Register the dedicated runner before starting it. See pipelines/CI.md.'
}

$existing = Get-CimInstance Win32_Process -Filter "Name='Runner.Listener.exe'" |
    Where-Object { $_.ExecutablePath -eq $listener }
if ($existing) {
    Write-Output "The dedicated runner is already running: $($existing.ProcessId -join ', ')."
    return
}

$python = Get-Command python -ErrorAction Stop
& $python.Source -c 'import sys; assert sys.version_info[:2] == (3, 11), "Python 3.11 is required on the runner PATH"'
if ($LASTEXITCODE -ne 0) { throw 'Python preflight failed.' }
$null = Get-Command docker -ErrorAction Stop

$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$stdout = Join-Path $runnerRoot "launcher-$runId.stdout.log"
$stderr = Join-Path $runnerRoot "launcher-$runId.stderr.log"
$process = Start-Process -FilePath $env:ComSpec -ArgumentList @('/d', '/c', ('call "{0}"' -f $entrypoint)) `
    -WorkingDirectory $runnerRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr

@{
    launcher_pid = $process.Id
    started_at = (Get-Date).ToUniversalTime().ToString('o')
    runner_directory = $runnerRoot
    stdout = $stdout
    stderr = $stderr
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runnerRoot 'launcher.json') -Encoding UTF8
Write-Output "Runner launcher started: PID $($process.Id). Logs: $stdout"
Write-Output 'GitHub reports the authoritative online/job status. Keep this PC and Docker Desktop running.'
