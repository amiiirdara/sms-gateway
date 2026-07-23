# Scenario suite for the metrics/ops report.
# Usage: powershell -NoProfile -File scripts/run-scenario-suite.ps1
# Writes JSON + raw metric snapshots under docs/scenario-report/

$ErrorActionPreference = "Stop"
$Base = if ($env:BASE_URL) { $env:BASE_URL } else { "http://[::1]:8080" }
$ReportBase = if ($env:REPORT_URL) { $env:REPORT_URL } else { "http://[::1]:8081" }
$OutDir = Join-Path $PSScriptRoot "..\docs\scenario-report"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $OutDir "raw") | Out-Null

function Get-MetricsMap {
  $text = (Invoke-WebRequest -Uri "$Base/metrics" -UseBasicParsing -TimeoutSec 10).Content
  $map = @{}
  foreach ($line in ($text -split "`n")) {
    if ($line -match '^([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?)\s+([0-9.eE+\-]+)\s*$') {
      $map[$Matches[1]] = [double]$Matches[2]
    }
  }
  return @{ text = $text; map = $map }
}

function MetricDelta([hashtable]$before, [hashtable]$after, [string]$name) {
  $a = if ($after.ContainsKey($name)) { $after[$name] } else { 0 }
  $b = if ($before.ContainsKey($name)) { $before[$name] } else { 0 }
  return [math]::Round($a - $b, 6)
}

function FindMetricSum([hashtable]$map, [string]$prefix) {
  $sum = 0.0
  foreach ($k in $map.Keys) {
    if ($k -like "$prefix*") { $sum += $map[$k] }
  }
  return $sum
}

function New-Account([string]$name) {
  Invoke-RestMethod -Method Post -Uri "$Base/v1/accounts" -ContentType "application/json" -Body (@{ name = $name } | ConvertTo-Json)
}

function AuthHeaders($apiKey) {
  @{ Authorization = "Bearer $apiKey"; "Content-Type" = "application/json" }
}

function Wait-MessageStatus($headers, $messageId, $timeoutSec = 45) {
  $deadline = (Get-Date).AddSeconds($timeoutSec)
  $last = $null
  while ((Get-Date) -lt $deadline) {
    try {
      $last = Invoke-RestMethod -Method Get -Uri "$ReportBase/v1/messages/$messageId" -Headers $headers
      if ($last.status -in @("sent", "failed", "expired_sla_missed")) { return $last }
    } catch {
      # reporting may lag while message row is created
    }
    Start-Sleep -Milliseconds 500
  }
  return $last
}

function CollectWorkerMetric([string]$service, [string]$pattern) {
  try {
    $raw = docker compose exec -T $service wget -qO- http://127.0.0.1:9090/metrics 2>$null
    if (-not $raw) { return @{} }
    $map = @{}
    foreach ($line in ($raw -split "`n")) {
      if ($line -match '^([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?)\s+([0-9.eE+\-]+)\s*$') {
        if ($Matches[1] -like "sms_*") { $map[$Matches[1]] = [double]$Matches[2] }
      }
    }
    return $map
  } catch {
    return @{}
  }
}

$suiteStart = Get-Date
$results = @()

Write-Host "== baseline metrics =="
$baseline = Get-MetricsMap
$baseline.text | Set-Content -Encoding utf8 (Join-Path $OutDir "raw\baseline.prom")

# ---------- Scenario 1: Normal happy path ----------
Write-Host "== S1 normal happy path =="
$m0 = Get-MetricsMap
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$acc1 = New-Account "scen-normal-$(Get-Random)"
$h1 = AuthHeaders $acc1.apiKey
$top1 = Invoke-RestMethod -Method Post -Uri "$Base/v1/topups" -Headers $h1 -Body '{"amount":10}'
$msg1 = Invoke-RestMethod -Method Post -Uri "$Base/v1/messages" -Headers $h1 -Body '{"to":"09121111111","text":"s1-normal","priority":"normal"}'
$final1 = Wait-MessageStatus $h1 $msg1.messageId
$bal1 = Invoke-RestMethod -Method Get -Uri "$Base/v1/balance" -Headers $h1
$sw.Stop()
$m1 = Get-MetricsMap
$m1.text | Set-Content -Encoding utf8 (Join-Path $OutDir "raw\s1-after.prom")
$results += [ordered]@{
  id = "S1"
  name = "Normal single-message happy path"
  description = "Create account --- topup 10 --- send normal SMS --- wait for terminal status --- check balance"
  elapsed_ms = $sw.ElapsedMilliseconds
  http = @{
    accountId = $acc1.accountId
    messageId = $msg1.messageId
    acceptStatus = $msg1.status
    finalStatus = if ($final1) { $final1.status } else { $null }
    operator = if ($final1) { $final1.operator } else { $null }
    balanceAfter = $bal1.balance
    topupBalance = $top1.balance
  }
  metrics_delta = @{
    accounts_created = MetricDelta $m0.map $m1.map "sms_accounts_created_total"
    messages_accepted_normal = MetricDelta $m0.map $m1.map 'sms_messages_accepted_total{priority="normal"}'
    credits_spent_single = MetricDelta $m0.map $m1.map 'sms_credits_spent_total{priority="normal",source="single"}'
    topups = MetricDelta $m0.map $m1.map "sms_topups_total"
    topup_credits = MetricDelta $m0.map $m1.map "sms_topup_credits_total"
    accept_duration_sum = MetricDelta $m0.map $m1.map 'sms_accept_duration_seconds_sum{priority="normal",result="accepted"}'
    accept_duration_count = MetricDelta $m0.map $m1.map 'sms_accept_duration_seconds_count{priority="normal",result="accepted"}'
  }
  passed = ($msg1.status -eq "accepted" -and $bal1.balance -eq 9 -and $final1 -and $final1.status -eq "sent")
}

# ---------- Scenario 2: Express happy path ----------
Write-Host "== S2 express happy path =="
$m0 = Get-MetricsMap
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$acc2 = New-Account "scen-express-$(Get-Random)"
$h2 = AuthHeaders $acc2.apiKey
Invoke-RestMethod -Method Post -Uri "$Base/v1/topups" -Headers $h2 -Body '{"amount":5}' | Out-Null
$msg2 = Invoke-RestMethod -Method Post -Uri "$Base/v1/messages" -Headers $h2 -Body '{"to":"+989122222222","text":"s2-otp","priority":"express"}'
$final2 = Wait-MessageStatus $h2 $msg2.messageId
$bal2 = Invoke-RestMethod -Method Get -Uri "$Base/v1/balance" -Headers $h2
$sw.Stop()
$m1 = Get-MetricsMap
$m1.text | Set-Content -Encoding utf8 (Join-Path $OutDir "raw\s2-after.prom")
$results += [ordered]@{
  id = "S2"
  name = "Express single-message happy path"
  description = "Create account --- topup --- send express SMS --- wait for sent"
  elapsed_ms = $sw.ElapsedMilliseconds
  http = @{
    messageId = $msg2.messageId
    acceptStatus = $msg2.status
    finalStatus = if ($final2) { $final2.status } else { $null }
    operator = if ($final2) { $final2.operator } else { $null }
    balanceAfter = $bal2.balance
  }
  metrics_delta = @{
    messages_accepted_express = MetricDelta $m0.map $m1.map 'sms_messages_accepted_total{priority="express"}'
    credits_spent = MetricDelta $m0.map $m1.map 'sms_credits_spent_total{priority="express",source="single"}'
    accept_duration_sum = MetricDelta $m0.map $m1.map 'sms_accept_duration_seconds_sum{priority="express",result="accepted"}'
  }
  passed = ($msg2.status -eq "accepted" -and $bal2.balance -eq 4 -and $final2 -and $final2.status -eq "sent")
}

# ---------- Scenario 3: Campaign happy path ----------
Write-Host "== S3 campaign happy path =="
$m0 = Get-MetricsMap
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$acc3 = New-Account "scen-camp-$(Get-Random)"
$h3 = AuthHeaders $acc3.apiKey
Invoke-RestMethod -Method Post -Uri "$Base/v1/topups" -Headers $h3 -Body '{"amount":10}' | Out-Null
$camp = Invoke-RestMethod -Method Post -Uri "$Base/v1/campaigns" -Headers $h3 -Body (@{
  text = "s3-promo"
  recipients = @("09121110001","09121110002","09121110003")
} | ConvertTo-Json)
Start-Sleep -Seconds 8
$bal3 = Invoke-RestMethod -Method Get -Uri "$Base/v1/balance" -Headers $h3
$campReport = $null
try {
  $campReport = Invoke-RestMethod -Method Get -Uri "$ReportBase/v1/campaigns/$($camp.campaignId)/report" -Headers $h3
} catch {
  $campReport = @{ error = $_.Exception.Message }
}
$sw.Stop()
$m1 = Get-MetricsMap
$m1.text | Set-Content -Encoding utf8 (Join-Path $OutDir "raw\s3-after.prom")
$results += [ordered]@{
  id = "S3"
  name = "Campaign all-or-nothing happy path (3 recipients)"
  description = "Topup 10 --- campaign 3 recipients --- balance 7 --- campaign report"
  elapsed_ms = $sw.ElapsedMilliseconds
  http = @{
    campaignId = $camp.campaignId
    totalRecipients = $camp.totalRecipients
    cost = $camp.cost
    balanceAfter = $bal3.balance
    campaignReport = $campReport
  }
  metrics_delta = @{
    campaigns_accepted = MetricDelta $m0.map $m1.map "sms_campaigns_accepted_total"
    recipients_accepted = MetricDelta $m0.map $m1.map "sms_campaign_recipients_accepted_total"
    credits_spent_campaign = MetricDelta $m0.map $m1.map 'sms_credits_spent_total{priority="normal",source="campaign"}'
  }
  passed = ($camp.totalRecipients -eq 3 -and $bal3.balance -eq 7)
}

# ---------- Scenario 4: Insufficient funds (single) ----------
Write-Host "== S4 insufficient funds single =="
$m0 = Get-MetricsMap
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$acc4 = New-Account "scen-insuf-$(Get-Random)"
$h4 = AuthHeaders $acc4.apiKey
$code4 = $null
$body4 = $null
try {
  Invoke-RestMethod -Method Post -Uri "$Base/v1/messages" -Headers $h4 -Body '{"to":"09123333333","text":"no-credit","priority":"normal"}'
  $code4 = 200
} catch {
  $code4 = [int]$_.Exception.Response.StatusCode.value__
  $body4 = $_.ErrorDetails.Message
}
$bal4 = Invoke-RestMethod -Method Get -Uri "$Base/v1/balance" -Headers $h4
$sw.Stop()
$m1 = Get-MetricsMap
$m1.text | Set-Content -Encoding utf8 (Join-Path $OutDir "raw\s4-after.prom")
$results += [ordered]@{
  id = "S4"
  name = "Insufficient funds --- single message"
  description = "New account with 0 balance attempts send --- expect 402"
  elapsed_ms = $sw.ElapsedMilliseconds
  http = @{ statusCode = $code4; body = $body4; balanceAfter = $bal4.balance }
  metrics_delta = @{
    rejected_insufficient = MetricDelta $m0.map $m1.map 'sms_messages_rejected_total{reason="insufficient_funds"}'
    messages_accepted = MetricDelta $m0.map $m1.map 'sms_messages_accepted_total{priority="normal"}'
  }
  passed = ($code4 -eq 402 -and $bal4.balance -eq 0)
}

# ---------- Scenario 5: Campaign AoN reject ----------
Write-Host "== S5 campaign AoN reject =="
$m0 = Get-MetricsMap
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$acc5 = New-Account "scen-aon-$(Get-Random)"
$h5 = AuthHeaders $acc5.apiKey
Invoke-RestMethod -Method Post -Uri "$Base/v1/topups" -Headers $h5 -Body '{"amount":1}' | Out-Null
$code5 = $null
$body5 = $null
try {
  Invoke-RestMethod -Method Post -Uri "$Base/v1/campaigns" -Headers $h5 -Body (@{
    text = "need-two"
    recipients = @("09124444444","09125555555")
  } | ConvertTo-Json)
  $code5 = 200
} catch {
  $code5 = [int]$_.Exception.Response.StatusCode.value__
  $body5 = $_.ErrorDetails.Message
}
$bal5 = Invoke-RestMethod -Method Get -Uri "$Base/v1/balance" -Headers $h5
$sw.Stop()
$m1 = Get-MetricsMap
$m1.text | Set-Content -Encoding utf8 (Join-Path $OutDir "raw\s5-after.prom")
$results += [ordered]@{
  id = "S5"
  name = "Campaign all-or-nothing reject"
  description = "Balance 1, campaign needs 2 --- 402, balance unchanged"
  elapsed_ms = $sw.ElapsedMilliseconds
  http = @{ statusCode = $code5; body = $body5; balanceAfter = $bal5.balance }
  metrics_delta = @{
    campaigns_rejected = MetricDelta $m0.map $m1.map 'sms_campaigns_rejected_total{reason="insufficient_funds"}'
    campaigns_accepted = MetricDelta $m0.map $m1.map "sms_campaigns_accepted_total"
  }
  passed = ($code5 -eq 402 -and $bal5.balance -eq 1)
}

# ---------- Scenario 6: Validation reject ----------
Write-Host "== S6 validation reject =="
$m0 = Get-MetricsMap
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$acc6 = New-Account "scen-val-$(Get-Random)"
$h6 = AuthHeaders $acc6.apiKey
Invoke-RestMethod -Method Post -Uri "$Base/v1/topups" -Headers $h6 -Body '{"amount":5}' | Out-Null
$code6 = $null
$body6 = $null
try {
  Invoke-RestMethod -Method Post -Uri "$Base/v1/messages" -Headers $h6 -Body '{"to":"not-a-phone","text":"bad","priority":"normal"}'
  $code6 = 200
} catch {
  $code6 = [int]$_.Exception.Response.StatusCode.value__
  $body6 = $_.ErrorDetails.Message
}
$bal6 = Invoke-RestMethod -Method Get -Uri "$Base/v1/balance" -Headers $h6
$sw.Stop()
$m1 = Get-MetricsMap
$m1.text | Set-Content -Encoding utf8 (Join-Path $OutDir "raw\s6-after.prom")
$results += [ordered]@{
  id = "S6"
  name = "Validation reject --- bad phone"
  description = "Malformed recipient --- 400, balance unchanged"
  elapsed_ms = $sw.ElapsedMilliseconds
  http = @{ statusCode = $code6; body = $body6; balanceAfter = $bal6.balance }
  metrics_delta = @{
    rejected_validation = MetricDelta $m0.map $m1.map 'sms_messages_rejected_total{reason="validation"}'
  }
  passed = ($code6 -eq 400 -and $bal6.balance -eq 5)
}

# ---------- Scenario 7: Burst accept (latency sample) ----------
Write-Host "== S7 burst accept (20 msgs) =="
$m0 = Get-MetricsMap
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$acc7 = New-Account "scen-burst-$(Get-Random)"
$h7 = AuthHeaders $acc7.apiKey
Invoke-RestMethod -Method Post -Uri "$Base/v1/topups" -Headers $h7 -Body '{"amount":50}' | Out-Null
$ok7 = 0
$latencies = @()
for ($i = 0; $i -lt 20; $i++) {
  $t0 = [DateTime]::UtcNow
  $r = Invoke-RestMethod -Method Post -Uri "$Base/v1/messages" -Headers $h7 -Body (@{
    to = ("09{0:D9}" -f (130000000 + $i))
    text = "burst-$i"
    priority = "normal"
  } | ConvertTo-Json)
  $latencies += ([DateTime]::UtcNow - $t0).TotalMilliseconds
  if ($r.status -eq "accepted") { $ok7++ }
}
$bal7 = Invoke-RestMethod -Method Get -Uri "$Base/v1/balance" -Headers $h7
$sw.Stop()
$m1 = Get-MetricsMap
$m1.text | Set-Content -Encoding utf8 (Join-Path $OutDir "raw\s7-after.prom")
$sorted = $latencies | Sort-Object
$p95 = $sorted[[math]::Min($sorted.Count - 1, [int][math]::Floor(0.95 * ($sorted.Count - 1)))]
$results += [ordered]@{
  id = "S7"
  name = "Burst accept --- 20 sequential normal messages"
  description = "Topup 50 --- 20 accepts --- measure client-side latency distribution"
  elapsed_ms = $sw.ElapsedMilliseconds
  http = @{
    accepted = $ok7
    balanceAfter = $bal7.balance
    latency_ms = @{
      min = [math]::Round(($latencies | Measure-Object -Minimum).Minimum, 2)
      avg = [math]::Round(($latencies | Measure-Object -Average).Average, 2)
      max = [math]::Round(($latencies | Measure-Object -Maximum).Maximum, 2)
      p95 = [math]::Round($p95, 2)
      samples = @($latencies | ForEach-Object { [math]::Round($_, 2) })
    }
  }
  metrics_delta = @{
    messages_accepted = MetricDelta $m0.map $m1.map 'sms_messages_accepted_total{priority="normal"}'
    credits_spent = MetricDelta $m0.map $m1.map 'sms_credits_spent_total{priority="normal",source="single"}'
  }
  passed = ($ok7 -eq 20 -and $bal7.balance -eq 30)
}

# ---------- Final aggregate + workers ----------
Write-Host "== final aggregates =="
$final = Get-MetricsMap
$final.text | Set-Content -Encoding utf8 (Join-Path $OutDir "raw\final.prom")
$workers = @{
  "dispatcher-normal" = CollectWorkerMetric "dispatcher-normal"
  "dispatcher-express" = CollectWorkerMetric "dispatcher-express"
  "outbox-relay" = CollectWorkerMetric "outbox-relay"
  "billing-consumer" = CollectWorkerMetric "billing-consumer"
  "report-sink" = CollectWorkerMetric "report-sink"
  "campaign-expander" = CollectWorkerMetric "campaign-expander"
}

$suite = [ordered]@{
  generated_at = (Get-Date).ToString("o")
  base_url = $Base
  report_url = $ReportBase
  duration_ms = [int]((Get-Date) - $suiteStart).TotalMilliseconds
  scenarios = $results
  suite_metrics_delta = @{
    accounts_created = MetricDelta $baseline.map $final.map "sms_accounts_created_total"
    messages_accepted_normal = MetricDelta $baseline.map $final.map 'sms_messages_accepted_total{priority="normal"}'
    messages_accepted_express = MetricDelta $baseline.map $final.map 'sms_messages_accepted_total{priority="express"}'
    messages_rejected_insufficient = MetricDelta $baseline.map $final.map 'sms_messages_rejected_total{reason="insufficient_funds"}'
    messages_rejected_validation = MetricDelta $baseline.map $final.map 'sms_messages_rejected_total{reason="validation"}'
    campaigns_accepted = MetricDelta $baseline.map $final.map "sms_campaigns_accepted_total"
    campaigns_rejected_insufficient = MetricDelta $baseline.map $final.map 'sms_campaigns_rejected_total{reason="insufficient_funds"}'
    topup_credits = MetricDelta $baseline.map $final.map "sms_topup_credits_total"
    credits_spent_single_normal = MetricDelta $baseline.map $final.map 'sms_credits_spent_total{priority="normal",source="single"}'
    credits_spent_campaign = MetricDelta $baseline.map $final.map 'sms_credits_spent_total{priority="normal",source="campaign"}'
  }
  workers = $workers
  pass_count = @($results | Where-Object { $_.passed }).Count
  total_count = $results.Count
}

$jsonPath = Join-Path $OutDir "results.json"
# utf8NoBOM so Python json.loads works without utf-8-sig
[System.IO.File]::WriteAllText($jsonPath, ($suite | ConvertTo-Json -Depth 10))
Write-Host "Wrote $jsonPath"
Write-Host ("Passed {0}/{1}" -f $suite.pass_count, $suite.total_count)

