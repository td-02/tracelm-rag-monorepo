# TraceLM RAG Monorepo

Distributed Retrieval-Augmented Generation (RAG) system with sharded workers, a coordinator control-plane, failure handling, and end-to-end tracing.

## Architecture

```text
                         +------------------------------+
Client / Scripts ------->|        Coordinator           |
(ingest/query/bench)     |  FastAPI (routing + scatter) |
                         +---------------+--------------+
                                         |
                    +--------------------+--------------------+
                    |                    |                    |
                    v                    v                    v
              +-----------+        +-----------+        +-----------+
              | worker_1  |        | worker_2  |        | worker_3  |
              | shard_1   |        | shard_2   |        | shard_3   |
              | FastAPI   |        | FastAPI   |        | FastAPI   |
              +-----+-----+        +-----+-----+        +-----+-----+
                    |                    |                    |
                    +--------------------+--------------------+
                                         |
                                         v
                                  +-------------+
                                  |   ChromaDB  |
                                  | vector store|
                                  +-------------+

                         +------------------------------+
                         |            Redis             |
                         | route map + degradation state|
                         +------------------------------+

                         +------------------------------+
                         | TraceLM + SQLite trace store |
                         | coordinator + workers spans  |
                         +------------------------------+
```

## Distributed Systems Concepts Demonstrated

- Consistent hashing:
  - `document_id` is mapped to worker shards with virtual nodes for stable, balanced routing.
- Scatter-gather query orchestration:
  - coordinator fans out queries to all healthy workers and merges/re-ranks global top-k.
- Fault tolerance and failover:
  - worker timeouts/failures are tracked; degraded nodes are excluded and later auto-recovered.
- Distributed tracing:
  - W3C `traceparent` propagation across coordinator and workers.
  - trace spans exported to local SQLite for trace-tree inspection.
- Shard-based retrieval:
  - each worker owns a local collection shard and serves retrieval from that shard.

## Setup

### Prerequisites

- Docker Desktop / Docker Engine
- Docker Compose v2

### Start the system

```bash
docker compose up --build
```

Core endpoints:

- Coordinator: `http://localhost:8000`
- Worker 1: `http://localhost:8001`
- Worker 2: `http://localhost:8002`
- Worker 3: `http://localhost:8003`
- ChromaDB: `http://localhost:8004`
- Redis: `localhost:6379`

## Run Ingestion

Wikipedia ingestion (20k subset via HuggingFace datasets):

```bash
python scripts/ingest_wikipedia.py
```

This script:

- loads `wikipedia/20220301.en` subset
- sends concurrent ingest requests to coordinator
- writes failures to `failed_ingestions.csv`
- prints per-worker distribution and latency summary

## Run Benchmark Suite

```bash
python scripts/benchmark.py
```

Produces:

- `benchmark_results.json`
- console summary table for 6 experiments:
  - ingestion speed
  - query latency under load
  - fault tolerance under node failure
  - scatter-gather overhead
  - query cache effectiveness
  - stress limit ramp to failure thresholds

Optional stress controls:

- `STRESS_MAX_CONCURRENCY` sets the upper bound for the ramp
- `STRESS_RAMP_STEP` sets how aggressively the load increases
- `STRESS_ROUNDS_PER_LEVEL` sets the request count per concurrency level
- `STRESS_P95_BREAKPOINT_MS` stops the ramp once tail latency crosses this threshold
- `STRESS_ERROR_RATE_BREAKPOINT` stops the ramp once failures cross this threshold

### Run the maximum stress profile

Start the stack first, then run the following profile from a separate terminal. It
ramps to 5,000 concurrent uncached queries in increments of 250, with 1,000
requests at each level. The run stops when the error rate reaches 2% or P95
latency reaches 2.5 seconds, which protects the host while still identifying
the system's practical saturation point.

```bash
STRESS_MAX_CONCURRENCY=5000 \\
STRESS_RAMP_STEP=250 \\
STRESS_ROUNDS_PER_LEVEL=1000 \\
STRESS_P95_BREAKPOINT_MS=2500 \\
STRESS_ERROR_RATE_BREAKPOINT=0.02 \\
python scripts/benchmark.py
```

The final `Exp6 Stress Limit` row reports the last completed concurrency level,
its P95 latency and error rate, plus the breakpoint that stopped the ramp. The
full per-level results are saved to `benchmark_results.json` under
`experiment_6_stress_limit`.

### Latest stress-run status

No live stress result is recorded as of 2026-09-18: Docker Engine was not
available in the benchmark environment and the coordinator was not reachable at
`http://localhost:8000`. The benchmark script passed Python syntax validation;
run the command above after Docker is running to publish a measured limit.

## Benchmark Results

| Experiment | Key Metrics | Notes |
|---|---|---|
| Ingestion Speed | `total_time_s=10.48s`, `throughput=95.38 docs/s` | Balanced distribution across shards (W1: 340, W2: 297, W3: 363) |
| Query Latency Under Load | `P50=1303.6 ms`, `P95=1602.2 ms`, `P99=1614.5 ms` | 100 concurrent queries (avg per-worker latency ~830ms) |
| Fault Tolerance | `success_rate=100.0%`, `recovery_time_s=1.08s` | worker_2 killed mid-run, auto-rerouted without query loss |
| Scatter-Gather Overhead | `scatter=5.3 ms`, `single=7.5 ms`, `quality_delta=0.200` | Global scatter-gather retrieved 20% more relevant documents |
| Query Cache Effectiveness | `uncached=13.8 ms`, `cached=4.0 ms`, `speedup=3.40x` | 19/20 cache hits using Redis query cache |

## Tech Stack

| Layer | Technology |
|---|---|
| API Services | FastAPI, Uvicorn |
| Coordination / Routing | Python asyncio, httpx |
| Vector Storage | ChromaDB |
| State / Health / Routing Map | Redis |
| Embeddings | sentence-transformers (`all-MiniLM-L6-v2`) |
| Tracing | TraceLM + SQLite |
| Dashboard | Streamlit + Plotly |
| Benchmarking | asyncio, httpx, Docker SDK |

## Observability Backbone

This system uses **TraceLM** for distributed trace context propagation and span export.

- TraceLM repository: [https://github.com/td-02/tracelm](https://github.com/td-02/tracelm)

## Why this matters for Big Data Engineering

Modern data platforms rely on horizontal partitioning, parallel query fan-out, and graceful failure handling at scale. This project mirrors the same operational patterns used in production search/vector systems:

- Elasticsearch clusters:
  - shard allocation, distributed query fan-out, and node-level resiliency.
- Pinecone / Weaviate-style vector clusters:
  - shard-level indexing and retrieval with coordinator-based request orchestration.
- Real-world SRE and platform operations:
  - degraded-node handling, health-based routing, and trace-driven debugging for latency spikes.

The result is a practical reference architecture for building robust, observable, and scalable retrieval systems in Big Data environments.
