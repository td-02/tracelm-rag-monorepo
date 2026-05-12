import asyncio
import json
import os
import time
from contextlib import suppress
from typing import Any

import httpx
import redis
from fastapi import FastAPI
from pydantic import BaseModel, Field

from shared import ConsistentHashRing, get_logger

app = FastAPI(title="coordinator")
logger = get_logger("coordinator")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
WORKERS = [w.strip() for w in os.getenv("WORKERS", "worker1,worker2,worker3").split(",") if w.strip()]
WORKER_PORT = int(os.getenv("WORKER_PORT", "8000"))
REQUEST_TIMEOUT_S = float(os.getenv("REQUEST_TIMEOUT_S", "30"))

DEGRADED_WORKERS_KEY = "cluster:degraded_workers"
FAILOVER_LOG_KEY = "cluster:failover_log"
FAILOVER_LOG_LIMIT = 200

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
ring = ConsistentHashRing(nodes=WORKERS, vnodes=150)

_health_monitor_task: asyncio.Task | None = None


class IngestRequest(BaseModel):
    document_id: str = Field(..., min_length=1)
    text: str = Field(..., min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestResponse(BaseModel):
    worker_id: str
    chunks_stored: int
    total_latency_ms: float


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k: int = Field(default=5, ge=1, le=100)


class QueryResult(BaseModel):
    text: str
    score: float
    metadata: dict[str, Any]


class QueryResponse(BaseModel):
    results: list[QueryResult]
    per_worker_latency: dict[str, float]
    total_latency_ms: float


class BatchIngestRequest(BaseModel):
    documents: list[IngestRequest] = Field(default_factory=list, min_length=1)


class BatchDocumentStatus(BaseModel):
    document_id: str
    worker_id: str
    status: str
    chunks_stored: int | None = None
    latency_ms: float | None = None
    error: str | None = None


class BatchIngestResponse(BaseModel):
    results: list[BatchDocumentStatus]
    total_latency_ms: float


class WorkerHealth(BaseModel):
    status: str
    worker_id: str
    documents_indexed: int | None = None
    memory_usage_mb: float | None = None
    degraded: bool
    error: str | None = None


class ClusterStatusResponse(BaseModel):
    workers: dict[str, WorkerHealth]


class FailoverEvent(BaseModel):
    ts_ms: float
    worker_id: str
    reason: str
    action: str


class FailoverLogResponse(BaseModel):
    failures: list[FailoverEvent]


def _worker_url(worker_id: str, path: str) -> str:
    return f"http://{worker_id}:{WORKER_PORT}{path}"


def _log_failover(worker_id: str, reason: str, action: str) -> None:
    event = {
        "ts_ms": time.time() * 1000.0,
        "worker_id": worker_id,
        "reason": reason,
        "action": action,
    }
    redis_client.lpush(FAILOVER_LOG_KEY, json.dumps(event))
    redis_client.ltrim(FAILOVER_LOG_KEY, 0, FAILOVER_LOG_LIMIT - 1)
    logger.error(
        f"Failover event action={action} worker_id={worker_id} reason={reason}",
        extra={"request_id": worker_id},
    )


def _mark_worker_degraded(worker_id: str, reason: str) -> None:
    redis_client.sadd(DEGRADED_WORKERS_KEY, worker_id)
    _log_failover(worker_id, reason, "degraded")


def _recover_worker(worker_id: str, reason: str) -> None:
    if redis_client.srem(DEGRADED_WORKERS_KEY, worker_id):
        _log_failover(worker_id, reason, "recovered")


def _is_degraded(worker_id: str) -> bool:
    return bool(redis_client.sismember(DEGRADED_WORKERS_KEY, worker_id))


async def _post_json(
    client: httpx.AsyncClient,
    worker_id: str,
    path: str,
    payload: dict[str, Any],
    traceparent: str | None,
) -> tuple[dict[str, Any], float]:
    headers = {"traceparent": traceparent} if traceparent else {}
    start = time.perf_counter()
    try:
        resp = await client.post(_worker_url(worker_id, path), json=payload, headers=headers)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        resp.raise_for_status()
        return resp.json(), elapsed_ms
    except httpx.TimeoutException as exc:
        _mark_worker_degraded(worker_id, f"timeout {path}: {exc}")
        raise


async def _get_json(client: httpx.AsyncClient, worker_id: str, path: str) -> dict[str, Any]:
    try:
        resp = await client.get(_worker_url(worker_id, path))
        resp.raise_for_status()
        return resp.json()
    except httpx.TimeoutException as exc:
        _mark_worker_degraded(worker_id, f"timeout {path}: {exc}")
        raise


async def _health_monitor_loop() -> None:
    while True:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_S) as client:
            tasks = [_get_json(client, worker_id, "/health") for worker_id in WORKERS]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for worker_id, result in zip(WORKERS, results):
            if isinstance(result, Exception):
                _mark_worker_degraded(worker_id, f"health_check_failed: {result}")
                continue
            _recover_worker(worker_id, "health_check_ok")

        await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event() -> None:
    global _health_monitor_task
    _health_monitor_task = asyncio.create_task(_health_monitor_loop())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global _health_monitor_task
    if _health_monitor_task is not None:
        _health_monitor_task.cancel()
        with suppress(asyncio.CancelledError):
            await _health_monitor_task


@app.post("/ingest", response_model=IngestResponse)
async def ingest(payload: IngestRequest) -> IngestResponse:
    start = time.perf_counter()
    candidates = ring.get_nodes(payload.document_id, len(WORKERS))

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_S) as client:
        last_exc: Exception | None = None
        for worker_id in candidates:
            if _is_degraded(worker_id):
                continue
            try:
                worker_data, _ = await _post_json(client, worker_id, "/ingest", payload.model_dump(), None)
                await asyncio.to_thread(redis_client.set, payload.document_id, worker_id)
                total_latency_ms = (time.perf_counter() - start) * 1000.0
                logger.info("Ingest routed", extra={"request_id": payload.document_id})
                return IngestResponse(
                    worker_id=worker_id,
                    chunks_stored=int(worker_data.get("chunks_stored", 0)),
                    total_latency_ms=total_latency_ms,
                )
            except httpx.TimeoutException as exc:
                last_exc = exc
                continue
            except Exception as exc:
                _mark_worker_degraded(worker_id, f"ingest_failed: {exc}")
                last_exc = exc
                continue

    if last_exc:
        raise httpx.HTTPError(f"all workers unavailable for ingest: {last_exc}")
    raise httpx.HTTPError("all workers unavailable for ingest")


@app.post("/query", response_model=QueryResponse)
async def query(payload: QueryRequest) -> QueryResponse:
    start = time.perf_counter()

    active_workers = [w for w in WORKERS if not _is_degraded(w)]
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_S) as client:
        tasks = [
            _post_json(
                client,
                worker_id,
                "/query",
                {"query": payload.query, "top_k": payload.top_k},
                None,
            )
            for worker_id in active_workers
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    per_worker_latency: dict[str, float] = {}
    merged: list[QueryResult] = []

    for worker_id, result in zip(active_workers, responses):
        if isinstance(result, httpx.TimeoutException):
            per_worker_latency[worker_id] = -1.0
            _mark_worker_degraded(worker_id, f"query_timeout: {result}")
            logger.error(
                f"Query scatter failure worker_id={worker_id} reason={result}",
                extra={"request_id": worker_id},
            )
            continue
        if isinstance(result, Exception):
            per_worker_latency[worker_id] = -1.0
            _mark_worker_degraded(worker_id, f"query_failed: {result}")
            logger.error(
                f"Query scatter failure worker_id={worker_id} reason={result}",
                extra={"request_id": worker_id},
            )
            continue

        data, latency_ms = result
        per_worker_latency[worker_id] = latency_ms
        for item in data.get("results", []):
            merged.append(
                QueryResult(
                    text=item.get("text", ""),
                    score=float(item.get("score", 0.0)),
                    metadata=item.get("metadata", {}) or {},
                )
            )

    merged.sort(key=lambda x: x.score, reverse=True)
    top_results = merged[: payload.top_k]

    total_latency_ms = (time.perf_counter() - start) * 1000.0
    return QueryResponse(
        results=top_results,
        per_worker_latency=per_worker_latency,
        total_latency_ms=total_latency_ms,
    )


@app.post("/ingest/batch", response_model=BatchIngestResponse)
async def ingest_batch(payload: BatchIngestRequest) -> BatchIngestResponse:
    start = time.perf_counter()

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_S) as client:
        tasks = []
        worker_by_doc: dict[str, str] = {}

        for doc in payload.documents:
            worker_id = ring.get_node(doc.document_id)
            worker_by_doc[doc.document_id] = worker_id
            tasks.append(_post_json(client, worker_id, "/ingest", doc.model_dump(), None))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[BatchDocumentStatus] = []
    redis_sets = []

    for doc, result in zip(payload.documents, responses):
        worker_id = worker_by_doc[doc.document_id]
        if isinstance(result, Exception):
            if isinstance(result, httpx.TimeoutException):
                _mark_worker_degraded(worker_id, f"batch_ingest_timeout: {result}")
            results.append(
                BatchDocumentStatus(
                    document_id=doc.document_id,
                    worker_id=worker_id,
                    status="failed",
                    error=str(result),
                )
            )
            continue

        data, latency_ms = result
        redis_sets.append(asyncio.to_thread(redis_client.set, doc.document_id, worker_id))
        results.append(
            BatchDocumentStatus(
                document_id=doc.document_id,
                worker_id=worker_id,
                status="ok",
                chunks_stored=int(data.get("chunks_stored", 0)),
                latency_ms=latency_ms,
            )
        )

    if redis_sets:
        await asyncio.gather(*redis_sets)

    total_latency_ms = (time.perf_counter() - start) * 1000.0
    return BatchIngestResponse(results=results, total_latency_ms=total_latency_ms)


@app.get("/cluster/status", response_model=ClusterStatusResponse)
async def cluster_status() -> ClusterStatusResponse:
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_S) as client:
        tasks = [client.get(_worker_url(worker_id, "/health")) for worker_id in WORKERS]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    workers: dict[str, WorkerHealth] = {}

    for worker_id, result in zip(WORKERS, responses):
        if isinstance(result, Exception):
            workers[worker_id] = WorkerHealth(
                status="unreachable",
                worker_id=worker_id,
                degraded=True,
                error=str(result),
            )
            continue

        try:
            result.raise_for_status()
            data = result.json()
            workers[worker_id] = WorkerHealth(
                status="ok",
                worker_id=str(data.get("worker_id", worker_id)),
                documents_indexed=int(data.get("documents_indexed", 0)),
                memory_usage_mb=float(data.get("memory_usage_mb", 0.0)),
                degraded=_is_degraded(worker_id),
            )
        except Exception as exc:
            workers[worker_id] = WorkerHealth(
                status="error",
                worker_id=worker_id,
                degraded=True,
                error=str(exc),
            )

    return ClusterStatusResponse(workers=workers)


@app.get("/cluster/failover-log", response_model=FailoverLogResponse)
def failover_log() -> FailoverLogResponse:
    raw = redis_client.lrange(FAILOVER_LOG_KEY, 0, 99)
    failures = [FailoverEvent(**json.loads(item)) for item in raw]
    return FailoverLogResponse(failures=failures)
