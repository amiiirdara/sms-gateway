# Edge-case smoke against a running Compose stack (api-gateway :8080).
# Usage: powershell -File scripts/smoke-edge.ps1

$ErrorActionPreference = "Stop"
$base = "http://localhost:8080"

function New-Account([string]$name) {
  Invoke-RestMethod -Method Post -Uri "$base/v1/accounts" -ContentType "application/json" -Body (@{ name = $name } | ConvertTo-Json)
}

Write-Host "== insufficient funds (single message) =="
$acc = New-Account "edge-insuf"
$H = @{ Authorization = "Bearer $($acc.apiKey)"; "Content-Type" = "application/json" }
try {
  Invoke-RestMethod -Method Post -Uri "$base/v1/messages" -Headers $H `
    -Body '{"to":"09121234567","text":"no-credit","priority":"normal"}'
  throw "expected 402"
} catch {
  $code = $_.Exception.Response.StatusCode.value__
  if ($code -ne 402) { throw "expected HTTP 402, got $code" }
  Write-Host "OK: single message rejected with 402"
}

Write-Host "== campaign all-or-nothing =="
$acc2 = New-Account "edge-camp"
$H2 = @{ Authorization = "Bearer $($acc2.apiKey)"; "Content-Type" = "application/json" }
Invoke-RestMethod -Method Post -Uri "$base/v1/topups" -Headers $H2 -Body '{"amount":1}' | Out-Null
try {
  Invoke-RestMethod -Method Post -Uri "$base/v1/campaigns" -Headers $H2 `
    -Body '{"text":"promo","recipients":["09121111111","09122222222"]}'
  throw "expected 402"
} catch {
  $resp = $_.ErrorDetails.Message
  $code = $_.Exception.Response.StatusCode.value__
  if ($code -ne 402) { throw "expected HTTP 402, got $code ($resp)" }
  Write-Host "OK: campaign rejected with 402 (required=2, available=1)"
}
$bal = Invoke-RestMethod -Method Get -Uri "$base/v1/balance" -Headers $H2
if ($bal.balance -ne 1) { throw "balance mutated after campaign reject: $($bal.balance)" }
Write-Host "OK: balance unchanged at 1"

Write-Host "== exact-zero spend =="
$acc3 = New-Account "edge-zero"
$H3 = @{ Authorization = "Bearer $($acc3.apiKey)"; "Content-Type" = "application/json" }
Invoke-RestMethod -Method Post -Uri "$base/v1/topups" -Headers $H3 -Body '{"amount":1}' | Out-Null
$msg = Invoke-RestMethod -Method Post -Uri "$base/v1/messages" -Headers $H3 `
  -Body '{"to":"+989121234567","text":"last-credit","priority":"normal"}'
if ($msg.status -ne "accepted") { throw "expected accepted" }
$bal3 = Invoke-RestMethod -Method Get -Uri "$base/v1/balance" -Headers $H3
if ($bal3.balance -ne 0) { throw "expected balance 0, got $($bal3.balance)" }
Write-Host "OK: spent down to exact zero (messageId=$($msg.messageId))"

Write-Host "All edge smokes passed."
