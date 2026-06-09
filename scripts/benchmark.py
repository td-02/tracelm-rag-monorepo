import asyncio
import json
import statistics
import time
from collections import Counter
from pathlib import Path
from typing import Any

import docker
import httpx

COORDINATOR_URL = "http://localhost:8000"
WORKER_URLS = {
    "worker_1": "http://localhost:8001",
    "worker_2": "http://localhost:8002",
    "worker_3": "http://localhost:8003",
}
RESULTS_PATH = Path("benchmark_results.json")


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = (len(ordered) - 1) * p
    f = int(k)
    c = min(f + 1, len(ordered) - 1)
    if f == c:
        return ordered[f]
    return ordered[f] * (c - k) + ordered[c] * (k - f)


def synthetic_doc(i: int) -> dict[str, Any]:
    text = (
        f"Document {i}. "
        + "Distributed systems benchmarking with sharded retrieval. " * 40
        + f"Unique marker {i}."
    )
    return {
        "document_id": f"bench-doc-{i}",
        "text": text,
        "metadata": {"experiment": "benchmark", "index": i},
    }


def summarize_latencies(latencies: list[float]) -> dict[str, float]:
    if not latencies:
        return {"avg_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0}
    return {
        "avg_ms": statistics.mean(latencies),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
    }


async def experiment_1_ingestion_speed(client: httpx.AsyncClient) -> dict[str, Any]:
    docs = [synthetic_doc(i) for i in range(1000)]
    worker_counts: Counter[str] = Counter()

    start = time.perf_counter()
    for doc in docs:
        resp = await client.post(f"{COORDINATOR_URL}/ingest", json=doc)
        resp.raise_for_status()
        data = resp.json()
        worker_counts[data.get("worker_id", "unknown")] += 1
    elapsed = time.perf_counter() - start

    per_worker_throughput = {
        worker: (count / elapsed if elapsed > 0 else 0.0)
        for worker, count in worker_counts.items()
    }
    expected = len(docs) / max(1, len(worker_counts))
    shard_imbalance_pct = (
        (max(worker_counts.values()) - min(worker_counts.values())) / expected * 100.0
        if len(worker_counts) > 1
        else 0.0
    )

    return {
        "documents": len(docs),
        "total_time_s": elapsed,
        "overall_throughput_docs_per_s": len(docs) / elapsed if elapsed > 0 else 0.0,
        "per_worker_counts": dict(worker_counts),
        "per_worker_throughput_docs_per_s": per_worker_throughput,
        "shard_imbalance_pct": shard_imbalance_pct,
    }


async def experiment_2_query_latency_under_load(client: httpx.AsyncClient) -> dict[str, Any]:
    latencies: list[float] = []
    per_worker_latency: dict[str, list[float]] = {"worker_1": [], "worker_2": [], "worker_3": []}

    sem = asyncio.Semaphore(100)

    async def run_one(i: int) -> None:
        payload = {"query": f"benchmark query {i}", "top_k": 5}
        async with sem:
            t0 = time.perf_counter()
            resp = await client.post(f"{COORDINATOR_URL}/query", json=payload)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            resp.raise_for_status()
            latencies.append(elapsed_ms)

            data = resp.json()
            worker_map = data.get("per_worker_latency", {})
            for worker_id, value in worker_map.items():
                if isinstance(value, (int, float)) and value >= 0:
                    per_worker_latency.setdefault(worker_id, []).append(float(value))

    await asyncio.gather(*(run_one(i) for i in range(100)))

    return {
        "queries": 100,
        **summarize_latencies(latencies),
        "per_worker_latency_ms_avg": {
            worker: (sum(vals) / len(vals) if vals else 0.0)
            for worker, vals in per_worker_latency.items()
        },
    }


def _find_worker2_container(docker_client: docker.DockerClient):
    for c in docker_client.containers.list(all=True):
        nm = c.name.lower()
        if "worker_2" in nm or "worker2" in nm:
            return c
    raise RuntimeError("worker_2 container not found")


async def experiment_3_fault_tolerance(client: httpx.AsyncClient) -> dict[str, Any]:
    docker_client = docker.from_env()
    worker2 = _find_worker2_container(docker_client)

    statuses: list[bool] = []
    latencies: list[float] = []
    failover_window: list[float] = []

    async def run_query(i: int) -> None:
        payload = {"query": f"fault query {i}", "top_k": 5}
        t0 = time.perf_counter()
        try:
            resp = await client.post(f"{COORDINATOR_URL}/query", json=payload)
            resp.raise_for_status()
            statuses.append(True)
        except Exception:
            statuses.append(False)
        finally:
            l = (time.perf_counter() - t0) * 1000.0
            latencies.append(l)
            if 50 <= i < 70:
                failover_window.append(l)

    tasks = []
    for i in range(100):
        tasks.append(asyncio.create_task(run_query(i)))
        if i == 49:
            worker2.kill()
        await asyncio.sleep(0.01)

    await asyncio.gather(*tasks)

    recovery_start = time.perf_counter()
    worker2.start()

    recovered = False
    for _ in range(30):
        try:
            r = await client.get(f"{WORKER_URLS['worker_2']}/health")
            if r.status_code == 200:
                recovered = True
                break
        except Exception:
            pass
        await asyncio.sleep(1)
    recovery_time_s = time.perf_counter() - recovery_start

    return {
        "queries": 100,
        "success_rate": (sum(statuses) / len(statuses)) if statuses else 0.0,
        "baseline_latency_ms": statistics.mean(latencies[:40]) if len(latencies) >= 40 else 0.0,
        "failover_window_latency_ms": statistics.mean(failover_window) if failover_window else 0.0,
        "latency_spike_ms": (
            (statistics.mean(failover_window) - statistics.mean(latencies[:40]))
            if failover_window and len(latencies) >= 40
            else 0.0
        ),
        "recovery_time_s": recovery_time_s,
        "recovered": recovered,
    }


async def experiment_4_scatter_gather_overhead(client: httpx.AsyncClient) -> dict[str, Any]:
    query_text = "distributed retrieval benchmark quality check"
    top_k = 5
    rounds = 30

    scatter_latencies: list[float] = []
    single_latencies: list[float] = []
    quality_diffs: list[float] = []

    for _ in range(rounds):
        t0 = time.perf_counter()
        scatter_resp = await client.post(f"{COORDINATOR_URL}/query", json={"query": query_text, "top_k": top_k})
        scatter_resp.raise_for_status()
        scatter_ms = (time.perf_counter() - t0) * 1000.0
        scatter_latencies.append(scatter_ms)
        scatter_data = scatter_resp.json()
        scatter_results = scatter_data.get("results", [])

        t1 = time.perf_counter()
        single_resp = await client.post(f"{WORKER_URLS['worker_1']}/query", json={"query": query_text, "top_k": top_k})
        single_resp.raise_for_status()
        single_ms = (time.perf_counter() - t1) * 1000.0
        single_latencies.append(single_ms)
        single_results = single_resp.json().get("results", [])

        scatter_texts = {r.get("text", "") for r in scatter_results}
        single_texts = {r.get("text", "") for r in single_results}
        overlap = len(scatter_texts.intersection(single_texts))
        quality_diffs.append((len(scatter_texts) - overlap) / max(1, len(scatter_texts)))

    return {
        "rounds": rounds,
        "scatter_avg_ms": statistics.mean(scatter_latencies) if scatter_latencies else 0.0,
        "single_avg_ms": statistics.mean(single_latencies) if single_latencies else 0.0,
        "scatter_p95_ms": percentile(scatter_latencies, 0.95) if scatter_latencies else 0.0,
        "single_p95_ms": percentile(single_latencies, 0.95) if single_latencies else 0.0,
        "latency_difference_ms": (
            statistics.mean(scatter_latencies) - statistics.mean(single_latencies)
            if scatter_latencies and single_latencies
            else 0.0
        ),
        "quality_difference_ratio": statistics.mean(quality_diffs) if quality_diffs else 0.0,
    }


async def experiment_5_query_cache_effectiveness(client: httpx.AsyncClient) -> dict[str, Any]:
    query_text = "cache benchmark repeated query"
    top_k = 5
    uncached_latencies: list[float] = []
    cached_latencies: list[float] = []
    cache_hits = 0

    for _ in range(20):
        t0 = time.perf_counter()
        resp = await client.post(
            f"{COORDINATOR_URL}/query",
            json={"query": query_text, "top_k": top_k, "use_cache": False},
        )
        resp.raise_for_status()
        uncached_latencies.append((time.perf_counter() - t0) * 1000.0)

    for _ in range(20):
        t0 = time.perf_counter()
        resp = await client.post(
            f"{COORDINATOR_URL}/query",
            json={"query": query_text, "top_k": top_k, "use_cache": True},
        )
        resp.raise_for_status()
        payload = resp.json()
        cached_latencies.append((time.perf_counter() - t0) * 1000.0)
        if payload.get("cache_hit"):
            cache_hits += 1

    uncached_avg = statistics.mean(uncached_latencies) if uncached_latencies else 0.0
    cached_avg = statistics.mean(cached_latencies) if cached_latencies else 0.0
    speedup = (uncached_avg / cached_avg) if cached_avg > 0 else 0.0

    return {
        "uncached": summarize_latencies(uncached_latencies),
        "cached": summarize_latencies(cached_latencies),
        "cache_hits": cache_hits,
        "cache_requests": len(cached_latencies),
        "speedup_ratio": speedup,
    }


def print_table(results: dict[str, Any]) -> None:
    rows = [
        (
            "Exp1 Ingestion",
            f"time={results['experiment_1_ingestion_speed']['total_time_s']:.2f}s",
            f"throughput={results['experiment_1_ingestion_speed']['overall_throughput_docs_per_s']:.2f} docs/s imbalance={results['experiment_1_ingestion_speed']['shard_imbalance_pct']:.1f}%",
        ),
        (
            "Exp2 Query Load",
            f"p50={results['experiment_2_query_latency_under_load']['p50_ms']:.1f}ms p95={results['experiment_2_query_latency_under_load']['p95_ms']:.1f}ms",
            f"p99={results['experiment_2_query_latency_under_load']['p99_ms']:.1f}ms",
        ),
        (
            "Exp3 Fault Tolerance",
            f"success={results['experiment_3_fault_tolerance']['success_rate']*100:.1f}%",
            f"recovery={results['experiment_3_fault_tolerance']['recovery_time_s']:.1f}s spike={results['experiment_3_fault_tolerance']['latency_spike_ms']:.1f}ms",
        ),
        (
            "Exp4 Scatter Overhead",
            f"scatter={results['experiment_4_scatter_gather_overhead']['scatter_avg_ms']:.1f}ms single={results['experiment_4_scatter_gather_overhead']['single_avg_ms']:.1f}ms",
            f"quality_delta={results['experiment_4_scatter_gather_overhead']['quality_difference_ratio']:.3f}",
        ),
        (
            "Exp5 Query Cache",
            f"uncached={results['experiment_5_query_cache_effectiveness']['uncached']['avg_ms']:.1f}ms cached={results['experiment_5_query_cache_effectiveness']['cached']['avg_ms']:.1f}ms",
            f"speedup={results['experiment_5_query_cache_effectiveness']['speedup_ratio']:.2f}x hits={results['experiment_5_query_cache_effectiveness']['cache_hits']}/{results['experiment_5_query_cache_effectiveness']['cache_requests']}",
        ),
    ]

    print("\nBenchmark Results")
    print("-" * 110)
    print(f"{'Experiment':<28} | {'Metric A':<42} | {'Metric B':<32}")
    print("-" * 110)
    for a, b, c in rows:
        print(f"{a:<28} | {b:<42} | {c:<32}")
    print("-" * 110)


async def main() -> None:
    timeout = httpx.Timeout(180.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        results = {
            "experiment_1_ingestion_speed": await experiment_1_ingestion_speed(client),
            "experiment_2_query_latency_under_load": await experiment_2_query_latency_under_load(client),
            "experiment_3_fault_tolerance": await experiment_3_fault_tolerance(client),
            "experiment_4_scatter_gather_overhead": await experiment_4_scatter_gather_overhead(client),
            "experiment_5_query_cache_effectiveness": await experiment_5_query_cache_effectiveness(client),
            "generated_at_epoch": time.time(),
        }

    RESULTS_PATH.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print_table(results)
    print(f"Saved: {RESULTS_PATH.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
