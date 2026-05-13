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
- console summary table for 4 experiments:
  - ingestion speed
  - query latency under load
  - fault tolerance under node failure
  - scatter-gather overhead

## Sample Benchmark Results (Placeholder)

| Experiment | Key Metrics | Notes |
|---|---|---|
| Ingestion Speed | `total_time_s=...`, `throughput=... docs/s` | Includes per-worker shard distribution |
| Query Latency Under Load | `P50=... ms`, `P95=... ms`, `P99=... ms` | 100 concurrent queries |
| Fault Tolerance | `success_rate=...%`, `recovery_time_s=...` | worker_2 failure + restart |
| Scatter-Gather Overhead | `delta_latency=... ms`, `quality_delta=...` | single shard vs all shards |

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
