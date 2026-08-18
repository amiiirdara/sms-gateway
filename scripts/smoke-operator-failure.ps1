# Failure-injection smoke: force operator-mock 5xx → message failed + credit refund.
# Prerequisites: Compose stack up (operator-mock published on :8082).
# Usage: powershell -NoProfile -File scripts/smoke-operator-failure.ps1

$ErrorActionPreference = "Stop"
# Prefer IPv6 loopback on Windows hosts where Adobe Connect owns 127.0.0.1:8080.
$Base = if ($env:BASE_URL) { $env:BASE_URL } else { "http://[::1]:8080" }
$ReportBase = if ($env:REPORT_URL) { $env:REPORT_URL } else { "http://[::1]:8081" }
$Operator = if ($env:OPERATOR_URL) { $env:OPERATOR_URL } else { "http://[::1]:8082" }

function Set-FailureRate([double]$rate) {
  Invoke-RestMethod -Method Put -Uri "$Operator/admin/failure-rate" `
    -ContentType "application/json" -Body (@{ failureRate = $rate } | ConvertTo-Json)
}

function Wait-MessageStatus($headers, $messageId, $timeoutSec = 60) {
  $deadline = (Get-Date).AddSeconds($timeoutSec)
  $last = $null
  while ((Get-Date) -lt $deadline) {
    try {
      $last = Invoke-RestMethod -Method Get -Uri "$ReportBase/v1/messages/$messageId" -Headers $headers
      if ($last.status -in @("sent", "failed", "expired_sla_missed")) { return $last }
    } catch { }
    Start-Sleep -Milliseconds 500
  }
  return $last
}

Write-Host "== operator failure injection (always 502) =="
$prev = Set-FailureRate 1.0
Write-Host "OK: failureRate set to $($prev.failureRate)"

try {
  $acc = Invoke-RestMethod -Method Post -Uri "$Base/v1/accounts" -ContentType "application/json" `
    -Body (@{ name = "fail-inject-$(Get-Random)" } | ConvertTo-Json)
  $H = @{ Authorization = "Bearer $($acc.apiKey)"; "Content-Type" = "application/json" }
  Invoke-RestMethod -Method Post -Uri "$Base/v1/topups" -Headers ($H + @{ "Idempotency-Key" = [guid]::NewGuid().ToString() }) -Body '{"amount":1}' | Out-Null
  $msg = Invoke-RestMethod -Method Post -Uri "$Base/v1/messages" -Headers $H `
    -Body '{"to":"09129998877","text":"expect-fail","priority":"normal"}'
  if ($msg.status -ne "accepted") { throw "expected accepted, got $($msg.status)" }

  $final = Wait-MessageStatus $H $msg.messageId
  if ($null -eq $final) { throw "timed out waiting for terminal status" }
  if ($final.status -ne "failed") { throw "expected failed, got $($final.status)" }
  Write-Host "OK: message $($msg.messageId) → failed"

  $deadline = (Get-Date).AddSeconds(30)
  $bal = $null
  while ((Get-Date) -lt $deadline) {
    $bal = Invoke-RestMethod -Method Get -Uri "$Base/v1/balance" -Headers $H
    if ($bal.balance -eq 1) { break }
    Start-Sleep -Milliseconds 400
  }
  if ($bal.balance -ne 1) { throw "expected refunded balance 1, got $($bal.balance)" }
  Write-Host "OK: credit refunded (balance=1)"
}
finally {
  $restored = Set-FailureRate 0.02
  Write-Host "OK: failureRate restored to $($restored.failureRate)"
}

Write-Host "Operator failure-injection smoke passed."
