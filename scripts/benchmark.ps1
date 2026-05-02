$cli     = ".\p2pcdn.exe"
$hash    = "e33a358763f967ec82d59349028b88761914781e3591a286cc721c98e112e81b"
$output  = "test.bin"
$metrics = "metrics.json"
$runs    = 5

Write-Host ""
Write-Host "=== Benchmark: $runs runs ===" -ForegroundColor Cyan

Write-Host "  Limpando metricas..." -ForegroundColor DarkGray
& $cli metrics reset 2>&1 | Out-Null

for ($i = 1; $i -le $runs; $i++) {
    Write-Host "  --- Run $i/$runs ---" -ForegroundColor White

    if (Test-Path $output) { Remove-Item $output -Force }

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & $cli download $hash --output $output --no-seed
    $sw.Stop()

    Write-Host "  Concluido em $([math]::Round($sw.Elapsed.TotalSeconds, 2))s" -ForegroundColor Yellow
}

if (Test-Path $metrics) {
    Copy-Item $metrics "metrics_result.json" -Force
    Write-Host "  Salvo: metrics_result.json" -ForegroundColor Green
} else {
    Write-Host "  AVISO: metrics.json nao encontrado" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Benchmark concluido!" -ForegroundColor Green
