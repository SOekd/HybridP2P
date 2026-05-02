#!/bin/bash
# Run from the directory containing the p2pcdn binary

CLI="./p2pcdn"
HASH="e33a358763f967ec82d59349028b88761914781e3591a286cc721c98e112e81b"
OUTPUT="test.bin"
METRICS=".p2pcdn/data/metrics.json"
RUNS=50

echo ""
echo "=== Benchmark: $RUNS runs ==="

echo "  Limpando metricas..."
"$CLI" metrics reset > /dev/null 2>&1

for ((i = 1; i <= RUNS; i++)); do
    echo "  --- Run $i/$RUNS ---"

    rm -f "$OUTPUT"

    start=$(date +%s%N)
    "$CLI" download "$HASH" --output "$OUTPUT" --no-seed
    end=$(date +%s%N)

    elapsed=$(echo "scale=2; ($end - $start) / 1000000000" | bc)
    echo "  Concluido em ${elapsed}s"
done

if [ -f "$METRICS" ]; then
    cp "$METRICS" "metrics_result.json"
    echo "  Salvo: metrics_result.json"
else
    echo "  AVISO: metrics.json nao encontrado"
fi

echo ""
echo "Benchmark concluido!"
