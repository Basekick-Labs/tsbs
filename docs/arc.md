# Arc Time Series Database

Arc is a high-performance time series database that uses msgpack binary format with columnar data organization for optimal ingestion performance.

## Overview

Arc support in TSBS provides:
- **Msgpack binary serialization** for compact data representation
- **Columnar data format** for 25-35% faster ingestion (2.91M metrics/sec)
- **Gzip compression** for reduced network bandwidth
- **Connection pooling** for efficient HTTP communication
- **Automatic retry logic** with exponential backoff

## Data Format

Arc uses a columnar msgpack format optimized for time series data:

```json
{
    "m": "cpu",              // measurement name
    "columns": {             // all arrays must have same length
        "time": [1704067200000, 1704067210000, ...],  // milliseconds
        "host": ["server01", "server01", ...],
        "usage_idle": [95.0, 94.5, ...],
        "usage_user": [3.2, 3.8, ...]
    }
}
```

## Building

```bash
# Build the Arc loader
make tsbs_load_arc

# Build the data generator
make tsbs_generate_data

# Or build all loaders
make loaders
```

## Data Generation

Generate test data in Arc's msgpack format:

```bash
# Generate CPU metrics for 1 host over 1 hour
tsbs_generate_data \
    --use-case="cpu-only" \
    --seed=123 \
    --scale=1 \
    --timestamp-start="2024-01-01T00:00:00Z" \
    --timestamp-end="2024-01-01T01:00:00Z" \
    --log-interval="10s" \
    --format="arc" \
    > /tmp/arc_data.msgpack

# Generate DevOps use case with multiple metrics
tsbs_generate_data \
    --use-case="devops" \
    --seed=123 \
    --scale=10 \
    --timestamp-start="2024-01-01T00:00:00Z" \
    --timestamp-end="2024-01-01T12:00:00Z" \
    --log-interval="10s" \
    --format="arc" \
    > /tmp/arc_devops_data.msgpack
```

## Data Loading

Load data into Arc:

```bash
# Basic loading
tsbs_load_arc \
    --file=/tmp/arc_data.msgpack \
    --urls="http://localhost:8000/api/v1/write/msgpack" \
    --database="benchmark" \
    --api-key="your-api-key" \
    --workers=4 \
    --batch-size=1000

# With gzip compression (recommended)
tsbs_load_arc \
    --file=/tmp/arc_data.msgpack \
    --urls="http://localhost:8000/api/v1/write/msgpack" \
    --database="benchmark" \
    --api-key="your-api-key" \
    --gzip=true \
    --workers=4 \
    --batch-size=1000

# Multiple Arc instances (load balancing)
tsbs_load_arc \
    --file=/tmp/arc_data.msgpack \
    --urls="http://arc1:8000/api/v1/write/msgpack,http://arc2:8000/api/v1/write/msgpack" \
    --database="benchmark" \
    --workers=8 \
    --batch-size=1000

# Dry run (test without loading)
tsbs_load_arc \
    --file=/tmp/arc_data.msgpack \
    --urls="http://localhost:8000/api/v1/write/msgpack" \
    --do-load=false \
    --workers=1 \
    --batch-size=1000
```

## Configuration Options

### Required Flags

- `--file`: Path to the msgpack data file
- `--urls`: Comma-separated list of Arc msgpack ingestion URLs

### Optional Flags

- `--api-key`: API key for Arc authentication (x-api-key header)
- `--database`: Arc database name (default: "default")
- `--gzip`: Enable gzip compression (default: true, recommended)
- `--workers`: Number of parallel workers (default: 1)
- `--batch-size`: Number of points per batch (default: 10000, recommended: 1000)
- `--do-load`: Actually load data vs dry run (default: true)
- `--do-create-db`: Create database if it doesn't exist (default: true)

## Performance Tips

1. **Batch Size**: Use 1000 points per batch for optimal latency/throughput balance
2. **Compression**: Always enable gzip compression (`--gzip=true`) for 50-70% size reduction
3. **Workers**: Use 4-8 workers per Arc instance for best throughput
4. **Connection Pooling**: The loader automatically pools HTTP connections
5. **Multiple URLs**: Distribute load across multiple Arc instances with comma-separated URLs

## Architecture

### Data Flow

```
Generation Phase:
  Point → Serializer → Msgpack (row format) → File

Loading Phase:
  File → DataSource → Batch (accumulate rows) → Processor
         ↓
  Convert rows to columnar format → Msgpack encode → Gzip compress
         ↓
  HTTP POST to Arc → Response (204 No Content)
```

### Key Components

1. **Serializer** ([serializer.go](../pkg/targets/arc/serializer.go))
   - Converts Point to msgpack row format during generation
   - Handles tags and fields mapping

2. **Batch** ([batch.go](../pkg/targets/arc/batch.go))
   - Accumulates individual points
   - Converts rows to columnar format (key optimization!)
   - Groups by measurement

3. **Processor** ([processor.go](../pkg/targets/arc/processor.go))
   - Encodes columnar data as msgpack
   - Applies gzip compression
   - Sends HTTP POST to Arc with proper headers
   - Implements retry logic with exponential backoff

4. **Benchmark** ([benchmark.go](../pkg/targets/arc/benchmark.go))
   - Sets up data source, batch factory, processor
   - Configures connection parameters

## Use Cases Supported

Arc supports both TSBS use cases:

- ✅ **DevOps**: CPU, memory, disk, network, nginx, PostgreSQL, Redis metrics
- ✅ **IoT**: Truck diagnostics, GPS, fuel, load metrics

## Example Workflow

```bash
# 1. Generate data
tsbs_generate_data \
    --use-case="cpu-only" \
    --scale=100 \
    --timestamp-start="2024-01-01T00:00:00Z" \
    --timestamp-end="2024-01-01T01:00:00Z" \
    --log-interval="10s" \
    --format="arc" \
    > cpu_100hosts_1h.msgpack

# 2. Check file size
ls -lh cpu_100hosts_1h.msgpack
# Binary msgpack is 50-70% smaller than text formats

# 3. Load into Arc
tsbs_load_arc \
    --file=cpu_100hosts_1h.msgpack \
    --urls="http://localhost:8000/api/v1/write/msgpack" \
    --database="benchmark" \
    --gzip=true \
    --workers=8 \
    --batch-size=1000 \
    --report-interval=10s

# Output shows:
# - Per-period metrics/sec and rows/sec
# - Overall throughput
# - Total metrics and rows loaded
```

## Troubleshooting

### Connection Refused
- Verify Arc is running: `curl http://localhost:8000/health`
- Check the URL matches your Arc configuration

### Authentication Failed (401)
- Verify API key is correct
- Check Arc's authentication configuration

### Slow Performance
- Increase `--workers` (try 4-8)
- Reduce `--batch-size` to 1000 (lower latency)
- Verify `--gzip=true` is enabled
- Check network latency between loader and Arc

### Out of Memory
- Reduce `--batch-size`
- Reduce `--workers`
- Process data in smaller time windows

## Comparison with Other Formats

| Format | Type | Throughput | Size | Parse Speed |
|--------|------|------------|------|-------------|
| **Arc (columnar)** | Binary | **2.91M metrics/sec** | Smallest | Fastest |
| InfluxDB Line Protocol | Text | 2.0M metrics/sec | Large | Slow (regex) |
| VictoriaMetrics | Text | 2.1M metrics/sec | Large | Medium |

Arc's columnar msgpack format provides:
- **25-35% faster** ingestion vs row format
- **10-15x faster** parsing vs text formats
- **50-70% smaller** payload size

## Implementation Files

- [pkg/targets/arc/implemented_target.go](../pkg/targets/arc/implemented_target.go) - Entry point
- [pkg/targets/arc/serializer.go](../pkg/targets/arc/serializer.go) - Msgpack serialization
- [pkg/targets/arc/batch.go](../pkg/targets/arc/batch.go) - Columnar conversion
- [pkg/targets/arc/processor.go](../pkg/targets/arc/processor.go) - HTTP client & compression
- [pkg/targets/arc/benchmark.go](../pkg/targets/arc/benchmark.go) - Setup & configuration
- [pkg/targets/arc/data_source.go](../pkg/targets/arc/data_source.go) - File reading
- [pkg/targets/arc/creator.go](../pkg/targets/arc/creator.go) - Database management
- [cmd/tsbs_load_arc/main.go](../cmd/tsbs_load_arc/main.go) - CLI entry point

## References

- Arc msgpack API: `/api/v1/write/msgpack`
- Arc documentation: See Arc repository
- TSBS documentation: [README.md](../README.md)
