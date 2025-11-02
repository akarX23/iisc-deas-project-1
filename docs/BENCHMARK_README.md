# Automated Spark Benchmarking System

This system automates the process of benchmarking Spark clusters with different configurations by dynamically generating Docker Compose deployments and running benchmarks through a FastAPI server.

## Components

### 1. Configuration File: `benchmark_configs.json`
Define multiple benchmark scenarios with different Spark configurations:
```json
[
  {
    "name": "small_1worker",
    "num_workers": 1,
    "mem_per_worker": 4,
    "cores_per_worker": 2,
    "dataset_scale": 0.1
  }
]
```

### 2. Docker Compose Generator: `generate_compose.py`
Dynamically generates `docker-compose.yaml` files with:
- Spark master node
- Configurable number of worker nodes
- FastAPI server with proper networking
- Environment variables for Spark configuration

### 3. Benchmark Client: `benchmark.py`
Python script that:
- Waits for API to be ready
- Calls the benchmark API with configuration parameters
- Collects and displays results

### 4. Orchestration Script: `run_benchmarks.sh`
Bash script that:
- Reads configurations from JSON file
- For each configuration:
  - Generates new docker-compose.yaml
  - Tears down existing deployment
  - Starts new deployment with updated configuration
  - Runs benchmarks
  - Stops deployment
- Organizes results by timestamp

## Usage

### Quick Start
```bash
./run_benchmarks.sh benchmark_configs.json
```

### Step-by-Step

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Prepare your dataset:**
Ensure `train.csv` is available in the project directory

3. **Edit configurations:**
Modify `benchmark_configs.json` to define your benchmark scenarios

4. **Run benchmarks:**
```bash
./run_benchmarks.sh
```

### Manual Single Configuration

1. Generate docker-compose:
```bash
python generate_compose.py 2 8 4  # 2 workers, 8GB each, 4 cores each
```

2. Start deployment:
```bash
docker compose up -d --build
```

3. Run benchmark:
```bash
python benchmark.py benchmark_configs.json
```

4. Stop deployment:
```bash
docker compose down
```

## Results

Results are saved in `./logs/<timestamp>/<config_name>/`:
- `results.csv` - Detailed metrics for each benchmark
- `stagemetrics_*/` - Spark stage metrics in JSON format

## Environment Variables

The FastAPI server uses these environment variables (configured automatically):
- `SPARK_MASTER_HOST` - Spark master hostname
- `DATASET_PATH` - Path to the dataset file
- `DRIVER_MEMORY` - Spark driver memory allocation

## Requirements

- Docker and Docker Compose
- Python 3.10+
- jq (for JSON parsing in bash script)
- Sufficient system resources for your largest configuration

## Architecture

```
run_benchmarks.sh
    ↓
    ├─→ generate_compose.py (creates docker-compose.yaml)
    ├─→ docker compose up (deploys Spark + FastAPI)
    ├─→ benchmark.py (calls API, runs benchmark)
    └─→ docker compose down (cleanup)
```

## Notes

- Each configuration gets a fresh Spark cluster deployment
- The FastAPI server is deployed in Docker alongside Spark
- Results from all configurations are aggregated with timestamps
- The system includes health checks and retry logic
- Waits between configurations ensure clean state transitions
