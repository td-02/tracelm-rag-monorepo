import os
import time
from typing import Any

import psutil
from chromadb import HttpClient
from fastapi import FastAPI, Header, Response
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer

from shared import TraceContext, init_tracer

app = FastAPI(title="worker")

WORKER_ID = os.getenv("WORKER_ID", "worker")
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
CHROMA_HOST = os.getenv("CHROMA_HOST", "chromadb")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))

chroma = HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
collection = chroma.get_or_create_collection(name=WORKER_ID)
model = SentenceTransformer(MODEL_NAME)
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
tracer = init_tracer(service_name=WORKER_ID)


def _split_chunks(text: str, chunk_tokens: int = 256, overlap_tokens: int = 32) -> list[str]:
    token_ids = tokenizer.encode(text, add_special_tokens=False)
    if not token_ids:
        return []

    chunks: list[str] = []
    step = max(1, chunk_tokens - overlap_tokens)
    for start in range(0, len(token_ids), step):
        window = token_ids[start : start + chunk_tokens]
        if not window:
            break
        chunk_text = tokenizer.decode(window, skip_special_tokens=True).strip()
        if chunk_text:
            chunks.append(chunk_text)
        if start + chunk_tokens >= len(token_ids):
            break
    return chunks


class IngestRequest(BaseModel):
    document_id: str = Field(..., min_length=1)
    text: str = Field(..., min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestResponse(BaseModel):
    chunks_stored: int
    latency_ms: float


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k: int = Field(default=5, ge=1, le=100)


class QueryResult(BaseModel):
    text: str
    score: float
    metadata: dict[str, Any]


class QueryResponse(BaseModel):
    results: list[QueryResult]
    latency_ms: float


class HealthResponse(BaseModel):
    worker_id: str
    documents_indexed: int
    memory_usage_mb: float


@app.post("/ingest", response_model=IngestResponse)
def ingest(
    payload: IngestRequest,
    response: Response,
    traceparent: str | None = Header(default=None),
) -> IngestResponse:
    parent_context = tracer.extract_context({"traceparent": traceparent} if traceparent else None)
    root_span = tracer.create_span("worker.ingest", parent_context)
    response.headers.update(tracer.inject_headers(root_span))

    start_ms = time.time() * 1000.0
    with tracer.span(root_span, {"document_id": payload.document_id, "worker_id": WORKER_ID}) as root_attrs:
        chunk_span = tracer.create_span("worker.ingest.chunking", TraceContext(root_span.trace_id, root_span.span_id))
        with tracer.span(chunk_span) as attrs:
            chunks = _split_chunks(payload.text)
            attrs["chunk_count"] = len(chunks)

        embeddings = []
        if chunks:
            embed_span = tracer.create_span("worker.ingest.embedding", TraceContext(root_span.trace_id, root_span.span_id))
            embed_t0 = time.perf_counter()
            with tracer.span(embed_span) as attrs:
                embeddings = model.encode(chunks, convert_to_numpy=False)
                attrs["chunk_count"] = len(chunks)
            root_attrs["embedding_latency_ms"] = (time.perf_counter() - embed_t0) * 1000.0

            metadatas = []
            ids = []
            for idx, _chunk in enumerate(chunks):
                data = dict(payload.metadata)
                data["document_id"] = payload.document_id
                data["chunk_index"] = idx
                metadatas.append(data)
                ids.append(f"{payload.document_id}:{idx}")

            db_span = tracer.create_span("worker.ingest.chroma_write", TraceContext(root_span.trace_id, root_span.span_id))
            db_t0 = time.perf_counter()
            with tracer.span(db_span) as attrs:
                collection.upsert(ids=ids, embeddings=embeddings, documents=chunks, metadatas=metadatas)
                attrs["rows_upserted"] = len(ids)
            root_attrs["db_latency_ms"] = (time.perf_counter() - db_t0) * 1000.0

        root_attrs["chunk_count"] = len(chunks)

    latency_ms = (time.time() * 1000.0) - start_ms
    return IngestResponse(chunks_stored=len(chunks), latency_ms=latency_ms)


@app.post("/query", response_model=QueryResponse)
def query(
    payload: QueryRequest,
    response: Response,
    traceparent: str | None = Header(default=None),
) -> QueryResponse:
    parent_context = tracer.extract_context({"traceparent": traceparent} if traceparent else None)
    root_span = tracer.create_span("worker.query", parent_context)
    response.headers.update(tracer.inject_headers(root_span))

    start_ms = time.time() * 1000.0
    with tracer.span(root_span, {"worker_id": WORKER_ID, "top_k": payload.top_k}) as root_attrs:
        embed_span = tracer.create_span("worker.query.embedding", TraceContext(root_span.trace_id, root_span.span_id))
        embed_t0 = time.perf_counter()
        with tracer.span(embed_span):
            query_embedding = model.encode([payload.query], convert_to_numpy=False)[0]
        root_attrs["embedding_latency_ms"] = (time.perf_counter() - embed_t0) * 1000.0

        db_span = tracer.create_span("worker.query.chroma_read", TraceContext(root_span.trace_id, root_span.span_id))
        db_t0 = time.perf_counter()
        with tracer.span(db_span):
            found = collection.query(
                query_embeddings=[query_embedding],
                n_results=payload.top_k,
                include=["documents", "metadatas", "distances"],
            )
        root_attrs["db_latency_ms"] = (time.perf_counter() - db_t0) * 1000.0

    docs = found.get("documents", [[]])[0]
    metas = found.get("metadatas", [[]])[0]
    dists = found.get("distances", [[]])[0]

    results = [
        QueryResult(text=doc, score=1.0 / (1.0 + float(dist)), metadata=meta or {})
        for doc, meta, dist in zip(docs, metas, dists)
    ]

    latency_ms = (time.time() * 1000.0) - start_ms
    return QueryResponse(results=results, latency_ms=latency_ms)


@app.get("/health", response_model=HealthResponse)
def health(
    response: Response,
    traceparent: str | None = Header(default=None),
) -> HealthResponse:
    parent_context = tracer.extract_context({"traceparent": traceparent} if traceparent else None)
    span = tracer.create_span("worker.health", parent_context)
    response.headers.update(tracer.inject_headers(span))

    with tracer.span(span):
        documents_indexed = int(collection.count())
        memory_usage_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)

    return HealthResponse(worker_id=WORKER_ID, documents_indexed=documents_indexed, memory_usage_mb=round(memory_usage_mb, 2))
