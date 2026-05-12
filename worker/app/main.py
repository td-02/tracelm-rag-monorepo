import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from threading import Lock
from typing import Any

import psutil
from chromadb import HttpClient
from fastapi import FastAPI, Header, Response
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer

app = FastAPI(title="worker")

WORKER_ID = os.getenv("WORKER_ID", "worker")
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
TRACE_DB_PATH = os.getenv("TRACE_DB_PATH", "traces.db")
CHROMA_HOST = os.getenv("CHROMA_HOST", "chromadb")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))

chroma = HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
collection = chroma.get_or_create_collection(name=WORKER_ID)
model = SentenceTransformer(MODEL_NAME)
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

_db_lock = Lock()


def _init_trace_store() -> None:
    with _db_lock:
        conn = sqlite3.connect(TRACE_DB_PATH)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS spans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trace_id TEXT NOT NULL,
                    span_id TEXT NOT NULL,
                    parent_span_id TEXT,
                    name TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    start_ms REAL NOT NULL,
                    end_ms REAL NOT NULL,
                    duration_ms REAL NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()


def _new_span_id() -> str:
    return uuid.uuid4().hex[:16]


def _new_trace_id() -> str:
    return uuid.uuid4().hex


def _parse_traceparent(traceparent: str | None) -> tuple[str, str] | None:
    if not traceparent:
        return None
    parts = traceparent.split("-")
    if len(parts) != 4:
        return None
    version, trace_id, parent_span_id, flags = parts
    if version != "00" or len(trace_id) != 32 or len(parent_span_id) != 16 or len(flags) != 2:
        return None
    return trace_id, parent_span_id


def _build_traceparent(trace_id: str, span_id: str, flags: str = "01") -> str:
    return f"00-{trace_id}-{span_id}-{flags}"


def _save_span(
    trace_id: str,
    span_id: str,
    parent_span_id: str | None,
    name: str,
    start_ms: float,
    end_ms: float,
) -> None:
    duration_ms = end_ms - start_ms
    with _db_lock:
        conn = sqlite3.connect(TRACE_DB_PATH)
        try:
            conn.execute(
                """
                INSERT INTO spans (
                    trace_id, span_id, parent_span_id, name, worker_id, start_ms, end_ms, duration_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace_id,
                    span_id,
                    parent_span_id,
                    name,
                    WORKER_ID,
                    start_ms,
                    end_ms,
                    duration_ms,
                ),
            )
            conn.commit()
        finally:
            conn.close()


@contextmanager
def _span(trace_id: str, parent_span_id: str | None, name: str):
    span_id = _new_span_id()
    start_ms = time.time() * 1000.0
    try:
        yield span_id
    finally:
        end_ms = time.time() * 1000.0
        _save_span(trace_id, span_id, parent_span_id, name, start_ms, end_ms)


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


_init_trace_store()


@app.post("/ingest", response_model=IngestResponse)
def ingest(
    payload: IngestRequest,
    response: Response,
    traceparent: str | None = Header(default=None),
) -> IngestResponse:
    incoming = _parse_traceparent(traceparent)
    trace_id = incoming[0] if incoming else _new_trace_id()
    parent_span = incoming[1] if incoming else None
    root_span = _new_span_id()
    response.headers["traceparent"] = _build_traceparent(trace_id, root_span)

    start_ms = time.time() * 1000.0
    with _span(trace_id, parent_span, "ingest"):
        with _span(trace_id, root_span, "ingest.chunk"):
            chunks = _split_chunks(payload.text)

        if chunks:
            with _span(trace_id, root_span, "ingest.embed"):
                embeddings = model.encode(chunks, convert_to_numpy=False)

            metadatas = []
            ids = []
            for idx, chunk in enumerate(chunks):
                data = dict(payload.metadata)
                data["document_id"] = payload.document_id
                data["chunk_index"] = idx
                metadatas.append(data)
                ids.append(f"{payload.document_id}:{idx}")

            with _span(trace_id, root_span, "ingest.store"):
                collection.upsert(
                    ids=ids,
                    embeddings=embeddings,
                    documents=chunks,
                    metadatas=metadatas,
                )

    latency_ms = (time.time() * 1000.0) - start_ms
    return IngestResponse(chunks_stored=len(chunks), latency_ms=latency_ms)


@app.post("/query", response_model=QueryResponse)
def query(
    payload: QueryRequest,
    response: Response,
    traceparent: str | None = Header(default=None),
) -> QueryResponse:
    incoming = _parse_traceparent(traceparent)
    trace_id = incoming[0] if incoming else _new_trace_id()
    parent_span = incoming[1] if incoming else None
    root_span = _new_span_id()
    response.headers["traceparent"] = _build_traceparent(trace_id, root_span)

    start_ms = time.time() * 1000.0
    with _span(trace_id, parent_span, "query"):
        with _span(trace_id, root_span, "query.embed"):
            query_embedding = model.encode([payload.query], convert_to_numpy=False)[0]

        with _span(trace_id, root_span, "query.search"):
            found = collection.query(
                query_embeddings=[query_embedding],
                n_results=payload.top_k,
                include=["documents", "metadatas", "distances"],
            )

    docs = found.get("documents", [[]])[0]
    metas = found.get("metadatas", [[]])[0]
    dists = found.get("distances", [[]])[0]

    results = [
        QueryResult(
            text=doc,
            score=1.0 / (1.0 + float(dist)),
            metadata=meta or {},
        )
        for doc, meta, dist in zip(docs, metas, dists)
    ]

    latency_ms = (time.time() * 1000.0) - start_ms
    return QueryResponse(results=results, latency_ms=latency_ms)


@app.get("/health", response_model=HealthResponse)
def health(
    response: Response,
    traceparent: str | None = Header(default=None),
) -> HealthResponse:
    incoming = _parse_traceparent(traceparent)
    trace_id = incoming[0] if incoming else _new_trace_id()
    parent_span = incoming[1] if incoming else None
    root_span = _new_span_id()
    response.headers["traceparent"] = _build_traceparent(trace_id, root_span)

    with _span(trace_id, parent_span, "health"):
        with _span(trace_id, root_span, "health.count"):
            documents_indexed = int(collection.count())

        memory_usage_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)

    return HealthResponse(
        worker_id=WORKER_ID,
        documents_indexed=documents_indexed,
        memory_usage_mb=round(memory_usage_mb, 2),
    )
