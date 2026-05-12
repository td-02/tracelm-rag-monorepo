import asyncio
import os
import time
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

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
ring = ConsistentHashRing(nodes=WORKERS, vnodes=150)


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


def _worker_url(worker_id: str, path: str) -> str:
    return f"http://{worker_id}:{WORKER_PORT}{path}"


async def _post_json(
    client: httpx.AsyncClient,
    worker_id: str,
    path: str,
    payload: dict[str, Any],
    traceparent: str | None,
) -> tuple[dict[str, Any], float]:
    headers = {"traceparent": traceparent} if traceparent else {}
    start = time.perf_counter()
    resp = await client.post(_worker_url(worker_id, path), json=payload, headers=headers)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    resp.raise_for_status()
    return resp.json(), elapsed_ms


@app.post("/ingest", response_model=IngestResponse)
async def ingest(payload: IngestRequest) -> IngestResponse:
    start = time.perf_counter()
    worker_id = ring.get_node(payload.document_id)

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_S) as client:
        worker_data, _ = await _post_json(client, worker_id, "/ingest", payload.model_dump(), None)

    await asyncio.to_thread(redis_client.set, payload.document_id, worker_id)

    total_latency_ms = (time.perf_counter() - start) * 1000.0
    logger.info("Ingest routed", extra={"request_id": payload.document_id})

    return IngestResponse(
        worker_id=worker_id,
        chunks_stored=int(worker_data.get("chunks_stored", 0)),
        total_latency_ms=total_latency_ms,
    )


@app.post("/query", response_model=QueryResponse)
async def query(payload: QueryRequest) -> QueryResponse:
    start = time.perf_counter()

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_S) as client:
        tasks = [
            _post_json(
                client,
                worker_id,
                "/query",
                {"query": payload.query, "top_k": payload.top_k},
                None,
            )
            for worker_id in WORKERS
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    per_worker_latency: dict[str, float] = {}
    merged: list[QueryResult] = []

    for worker_id, result in zip(WORKERS, responses):
        if isinstance(result, Exception):
            per_worker_latency[worker_id] = -1.0
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
                degraded=False,
            )
        except Exception as exc:
            workers[worker_id] = WorkerHealth(
                status="error",
                worker_id=worker_id,
                degraded=True,
                error=str(exc),
            )

    return ClusterStatusResponse(workers=workers)
