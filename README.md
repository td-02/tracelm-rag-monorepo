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

## Stress Test Report (2026-09-30)

A full stress run was executed against the current source on a single Docker Desktop VM
(10 CPUs, 7.75 GiB, arm64). Images were rebuilt from the working tree, ~7,500 synthetic
documents (27k chunks) were ingested, and load was generated inside the compose network
with a Python/httpx harness and with [oha](https://github.com/hatoo/oha) (Rust) for the
high-concurrency levels. Profiles were taken with `py-spy` inside the running containers.

### Measured capacity

| Path | Clients | Throughput | p50 | p95 | Notes |
|---|---|---|---|---|---|
| Ingest | 16-64 | 125-130 docs/s | 36-211 ms | 0.6-1.6 s | ChromaDB at 200%+ CPU; 83 docs/s at 256 clients with a worker degraded |
| Query, uncached | 1 | 81 q/s | 12 ms | 14 ms | best case |
| Query, uncached | 10 | 68 q/s | 156 ms | 215 ms | already saturated |
| Query, uncached | 100 | 39-51 q/s | 1.9 s | 4.5-5.2 s | the documented breakpoint (p95 >= 2.5 s) is crossed here |
| Query, uncached | 400 | 0 responses in 15 s | | | with the shipped pool config (600 max / 150 keep-alive) |
| Query, uncached | 2,000 / 3,000 | 29 / 13 q/s | 41 / 49 s | 77 / 93 s | 22% / 62% timeouts at 60 s |
| Query, cached | 10-3,200 | 475-615 q/s | 19-290 ms | | flat; 1,244 timeouts at 3,200 |
| ChromaDB, one shard, direct | 1 / 10 / 200 | 642 / 194 / 175 q/s | 1 / 50 / 1,246 ms | | throughput collapses under concurrency |
| Worker, direct | 1 / 200 | 322 / 182 q/s | 3 / 966 ms | | follows ChromaDB's curve |

- Hard breaking point: 3,000 concurrent clients (62% errors, 13 q/s).
- Practical breaking point: 25-50 concurrent clients, where p95 crosses 2.5 s.
- Workers never exceeded ~12% CPU. ChromaDB ran at 190-200% CPU at low concurrency; the
  coordinator pinned its single core at 100%+ from ~200 clients upward.

### Failure modes and root causes

- **ChromaDB is the real ceiling.** One process serves all three shards and its own
  throughput drops from 642 to ~170 queries/s once ~10 queries are in flight. Three shard
  queries per request gives the 65-80 q/s ceiling. Killing a worker *raised* throughput to
  79 q/s because fewer shards hit ChromaDB.
- **The coordinator connection pool makes overload worse.** Under load the coordinator's
  event loop was inside httpcore's pool scheduler in 12 of 12 stack samples. That scheduler
  rescans every connection for every queued request; the 600/150 pool (`HTTP_MAX_CONNECTIONS`
  / `HTTP_MAX_KEEPALIVE_CONNECTIONS`) left 7,000+ TIME_WAIT sockets from churn. Running the
  same image with a 30/30 or 100/100 pool still answered at 400 clients (43-45 q/s) where
  600/150 returned nothing within 15 s.
- **Silent shard dropouts.** At 100 clients for 45 s: 0% HTTP errors, but 84.7% of the
  responses were missing a shard (33% in an 80/20 mixed workload, 88% during fault
  injection). Cause: uvicorn closes idle keep-alive connections after 5 s while the
  coordinator's httpx pool reuses them for 30 s, producing an empty-message `ReadError`;
  the coordinator marks a worker degraded on *any* exception and only the 30 s health loop
  recovers it.
- **Empty results with HTTP 200.** A single ingest with a list-valued metadata field returned
  500 and within 42 ms degraded all three workers through the ingest failover loop; for the
  next 22 s every query returned 200 with zero results and an empty `per_worker_latency`.
  Overload reproduced the same state: ~609 "successful" empty responses per second for ~28 s.
- **Trace store grows without bound.** Every request writes a JSON row to SQLite via a fresh
  connection with fsync on the event loop. After ~40 minutes: 186 MB / 150k rows on the
  coordinator and 112-139 MB per worker.
- **Global cache invalidation.** Every ingest bumps the cache version; one ingest per five
  queries already drops the hit rate to 80%, and steady ingestion makes the cache useless.
- **Failover works.** Killing `worker_2` under load lost 1 request of 3,721 and the worker was
  healthy again within its restart window, but its `documents_indexed` counter reset from
  11,657 to 0 because it lives in process memory.

### Defects found in the repository

- `coordinator/app/main.py` line 434 references an undefined `redis_sets`, so every
  `POST /ingest/batch` returns 500 (introduced by commit `4b0df33`). Verified live.
- `python -m pytest` from the repository root fails at import time because the outer
  `shared/` directory shadows the package; the tests pass from `tests/` or with
  `PYTHONPATH=shared`.
- `scripts/benchmark.py` uses httpx's default pool of 100 connections, so the documented
  "5,000 concurrent" ramp never has more than 100 requests in flight. The maximum profile
  from this README stopped at its first level (250) with p95 = 6,915 ms.
- `simulate_failure.py` targets port 8001 (worker_1) instead of the coordinator on 8000.
- The Tech Stack table lists sentence-transformers `all-MiniLM-L6-v2`; the worker actually
  embeds with a SHA-256 hashed bag-of-words vector (384 dims).
- `WORKER_URLS` in `docker-compose.yml` is never read; `/ingest/batch` skips the degraded
  check and failover that `/ingest` has; the dashboard does not display experiment 6.

### Official benchmark (maximum stress profile, run from the host)

| Exp1 Ingestion | Exp2 Query Load p50 / p95 | Exp3 Fault Tolerance | Exp4 Scatter vs Single | Exp5 Query Cache | Exp6 Stress Limit |
|---|---|---|---|---|---|
| 75.9 docs/s (sequential) | 1,719 / 2,015 ms | 100% success, 1.1 s recovery | 4.9 vs 6.2 ms | 2.76x, 19/20 hits | stopped at level 250, `latency_breakpoint`, p95 6,915 ms |

### Caveats

- Load generator and services shared one VM; numbers are relative, not absolute hardware limits.
- A Python/httpx load generator saturates its own CPU above ~300 connections for the same
  httpcore reason, so the oha numbers are authoritative at high concurrency.
- Levels shorter than the 30 s health loop show run-to-run variance in when degradation flaps.

### Reproducing the high-concurrency numbers

Start the stack, then run oha inside the compose network (replace `-c` with the level):

```bash
docker run --rm --network tracelm-rag-monorepo_rag_network ghcr.io/hatoo/oha:latest \
  --no-tui -z 15s -c 100 -m POST -T application/json \
  -d '{"query":"stress probe","top_k":5,"use_cache":false}' \
  http://coordinator:8000/query
```

### Recommended fixes, in priority order

1. Make degraded fan-out visible to clients (flag or 503 when shards are missing or none are
   active) and stop degrading workers on non-timeout transport errors; retry once instead.
2. Align keep-alive: set httpx `keepalive_expiry` below uvicorn's 5 s `--timeout-keep-alive`,
   or raise the uvicorn timeout.
3. Run one ChromaDB per shard (or cap per-worker query concurrency) so ChromaDB stays near its
   single-client throughput.
4. Shrink the coordinator pool (30-100 with keep-alive == max) and run multiple uvicorn workers.
5. Export traces off the event loop in batches (WAL mode, persistent connection) with retention.
6. Scope cache invalidation to affected shards/documents instead of a global version bump.
7. Fix the batch-ingest `NameError`, the test import path, the benchmark client pool limits,
   the `simulate_failure.py` port, and the embedding claim in the Tech Stack table.
