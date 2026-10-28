#!/bin/bash
# Quick test script for Arc integration

set -e

# Configuration
ARC_URL="${ARC_URL:-http://localhost:8000/api/v1/write/msgpack}"
ARC_API_KEY="${ARC_API_KEY:-}"
ARC_DATABASE="${ARC_DATABASE:-benchmark}"
SCALE="${SCALE:-1}"
WORKERS="${WORKERS:-4}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

echo "=== TSBS Arc Load Test ==="
echo "Arc URL: $ARC_URL"
echo "Database: $ARC_DATABASE"
echo "Scale: $SCALE hosts"
echo "Workers: $WORKERS"
echo "Batch Size: $BATCH_SIZE"
echo ""

# Check if binaries exist
if [ ! -f "bin/tsbs_generate_data" ] || [ ! -f "bin/tsbs_load_arc" ]; then
    echo "Building binaries..."
    make tsbs_generate_data tsbs_load_arc
    echo ""
fi

# Generate test data
echo "Generating test data..."
DATA_FILE="/tmp/arc_test_$(date +%s).msgpack"
./bin/tsbs_generate_data \
    --use-case="cpu-only" \
    --seed=123 \
    --scale=$SCALE \
    --timestamp-start="2024-01-01T00:00:00Z" \
    --timestamp-end="2024-01-01T00:10:00Z" \
    --log-interval="10s" \
    --format="arc" \
    > "$DATA_FILE"

DATA_SIZE=$(ls -lh "$DATA_FILE" | awk '{print $5}')
DATA_LINES=$(wc -l < "$DATA_FILE")
echo "Generated $DATA_FILE ($DATA_SIZE, $DATA_LINES points)"
echo ""

# Load data
echo "Loading data into Arc..."
API_KEY_FLAG=""
if [ -n "$ARC_API_KEY" ]; then
    API_KEY_FLAG="--api-key=$ARC_API_KEY"
fi

./bin/tsbs_load_arc \
    --file="$DATA_FILE" \
    --urls="$ARC_URL" \
    --database="$ARC_DATABASE" \
    $API_KEY_FLAG \
    --gzip=true \
    --workers=$WORKERS \
    --batch-size=$BATCH_SIZE \
    --report-interval=5s

echo ""
echo "Test complete! Data file: $DATA_FILE"
echo "To clean up: rm $DATA_FILE"
