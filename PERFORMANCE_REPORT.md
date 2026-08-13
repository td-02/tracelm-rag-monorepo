# Performance Report

Date: 2026-08-13

## What changed

- `worker/app/main.py`
  - Lazily initializes the Chroma collection instead of connecting at import time.
  - Retries collection initialization a few times so startup is less brittle when Chroma is still coming online.
  - Caches `documents_indexed` in memory and updates it on ingest, avoiding a full `collection.count()` on every `/health` check.

- `coordinator/app/main.py`
  - Batches Redis ingest routing updates and cache invalidation into a single pipelined operation for the single-document ingest path.

## Why this helps

- Faster and safer startup:
  - The worker no longer fails early just because Chroma is briefly unavailable during module import.
- Lower health-check overhead:
  - `/health` is now lightweight, which matters because the coordinator and benchmark harness hit it repeatedly.
- Less Redis chatter during ingest:
  - The coordinator now does less cross-thread Redis work per document.

## Baseline benchmark artifact

The repo already included `benchmark_results.json`. The baseline values recorded there were:

- Ingestion throughput: `95.38 docs/s`
- Query latency under load: `P50=1303.61 ms`, `P95=1602.22 ms`, `P99=1614.53 ms`
- Fault tolerance success rate: `100%`
- Cache speedup: `3.40x`

## Stress-test note

I could not rerun the full Docker-based stress suite in this workspace because the Docker daemon is not available here:

- `docker compose` is installed
- `docker ps` fails with a missing Docker API socket

That means the existing benchmark artifact remains the published baseline for this repo, and the code changes above are the performance improvements I was able to apply and verify statically.

