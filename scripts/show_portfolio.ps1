param(
    [string]$EvidenceFile = (Join-Path $PSScriptRoot '../docs/evidence/local_validation.json')
)

$ErrorActionPreference = 'Stop'
$evidence = Get-Content -LiteralPath $EvidenceFile -Raw -Encoding UTF8 | ConvertFrom-Json
if ($evidence.schema_version -ne 1 -or $evidence.record_type -ne 'historical_local_validation') {
    throw 'Unsupported portfolio evidence format.'
}
if (-not $evidence.end_to_end.passed -or -not $evidence.ci.passed -or
    -not $evidence.recovery.passed -or -not $evidence.retention_loss.passed) {
    throw 'Recorded portfolio evidence contains a failed gate.'
}
if ($evidence.end_to_end.payments -le 0 -or
    $evidence.end_to_end.payments -ne $evidence.end_to_end.matched_payments -or
    $evidence.ci.total_tests -le 0 -or $evidence.ci.unexpected_skips -ne 0 -or
    $evidence.ci.total_tests -ne ($evidence.ci.fast_tests + $evidence.ci.compute_tests) -or
    $evidence.recovery.restored_old_rows -ne 0 -or $evidence.recovery.new_cdc_rows -le 0 -or
    -not $evidence.retention_loss.committed_delta_unchanged) {
    throw 'Recorded portfolio evidence contains inconsistent results.'
}

Write-Output 'FinRecon: recorded local validation, not a live health check'
Write-Output "Measured date: $($evidence.measured_date)"
Write-Output "End-to-end: $($evidence.end_to_end.matched_payments)/$($evidence.end_to_end.payments) payments matched"
Write-Output "Gateway: $($evidence.end_to_end.gateway_deliveries) deliveries; $($evidence.end_to_end.exact_retries) exact retries"
Write-Output "Fixture: $($evidence.end_to_end.hot_originals) hot-merchant originals; $($evidence.end_to_end.poison_pills) poison pills; $($evidence.end_to_end.validated_memo_links) memo links"
Write-Output "Quality: $($evidence.end_to_end.quality_checks) checks; historical open items retained: $($evidence.end_to_end.historical_open_items_retained)"
Write-Output "CI: $($evidence.ci.total_tests) selected tests; unexpected skips: $($evidence.ci.unexpected_skips)"
Write-Output "Restore: $($evidence.recovery.restored_old_rows) old rows replayed; $($evidence.recovery.new_cdc_rows) new CDC rows ingested"
Write-Output "Retention loss: expected Spark exit $($evidence.retention_loss.expected_spark_exit); committed Delta unchanged: $($evidence.retention_loss.committed_delta_unchanged)"
Write-Output "Scale boundary: $($evidence.skew.synthetic_payments) synthetic compute payments; end-to-end correctness fixture remains $($evidence.end_to_end.payments)"
Write-Output "Retry timing: $($evidence.end_to_end.retry_clock)"
Write-Output "Limits: $($evidence.boundaries -join '; ')"
$dataset = if ($evidence.dataset) { $evidence.dataset } else { 'readiness_20261114' }
Write-Output "For live services: .venv-ci/Scripts/python.exe -m operations.status --dataset $dataset"
