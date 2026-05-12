import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock
from typing import Any, Mapping


@dataclass
class TraceContext:
    trace_id: str
    span_id: str | None
    trace_flags: str = "01"


@dataclass
class Span:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    service_name: str
    trace_flags: str = "01"


class TraceLMTracer:
    def __init__(self, service_name: str, db_path: str | None = None) -> None:
        self.service_name = service_name
        self.db_path = db_path or os.getenv("TRACE_DB_PATH", "traces.db")
        self._lock = Lock()
        self._init_store()

    def _init_store(self) -> None:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS spans (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trace_id TEXT NOT NULL,
                        span_id TEXT NOT NULL,
                        parent_span_id TEXT,
                        name TEXT NOT NULL,
                        service_name TEXT NOT NULL,
                        start_ms REAL NOT NULL,
                        end_ms REAL NOT NULL,
                        duration_ms REAL NOT NULL,
                        attributes_json TEXT NOT NULL
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()

    @staticmethod
    def _new_trace_id() -> str:
        return uuid.uuid4().hex

    @staticmethod
    def _new_span_id() -> str:
        return uuid.uuid4().hex[:16]

    def create_span(self, name: str, parent_context: TraceContext | None = None) -> Span:
        trace_id = parent_context.trace_id if parent_context else self._new_trace_id()
        trace_flags = parent_context.trace_flags if parent_context else "01"
        parent_span_id = parent_context.span_id if parent_context else None
        return Span(
            trace_id=trace_id,
            span_id=self._new_span_id(),
            parent_span_id=parent_span_id,
            name=name,
            service_name=self.service_name,
            trace_flags=trace_flags,
        )

    def inject_headers(self, span: Span) -> dict[str, str]:
        return {"traceparent": f"00-{span.trace_id}-{span.span_id}-{span.trace_flags}"}

    def extract_context(self, headers: Mapping[str, str] | None) -> TraceContext | None:
        if not headers:
            return None
        raw = headers.get("traceparent") or headers.get("Traceparent")
        if not raw:
            return None
        parts = raw.split("-")
        if len(parts) != 4:
            return None
        version, trace_id, span_id, flags = parts
        if version != "00" or len(trace_id) != 32 or len(span_id) != 16 or len(flags) != 2:
            return None
        return TraceContext(trace_id=trace_id, span_id=span_id, trace_flags=flags)

    @contextmanager
    def span(self, span: Span, attributes: dict[str, Any] | None = None):
        attrs: dict[str, Any] = attributes.copy() if attributes else {}
        start_ms = time.time() * 1000.0
        try:
            yield attrs
        finally:
            end_ms = time.time() * 1000.0
            self._save_span(span, start_ms, end_ms, attrs)

    def _save_span(self, span: Span, start_ms: float, end_ms: float, attributes: dict[str, Any]) -> None:
        payload = json.dumps(attributes, ensure_ascii=True)
        duration_ms = end_ms - start_ms
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO spans (
                        trace_id, span_id, parent_span_id, name, service_name, start_ms, end_ms, duration_ms, attributes_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        span.trace_id,
                        span.span_id,
                        span.parent_span_id,
                        span.name,
                        span.service_name,
                        start_ms,
                        end_ms,
                        duration_ms,
                        payload,
                    ),
                )
                conn.commit()
            finally:
                conn.close()


def init_tracer(service_name: str, db_path: str | None = None) -> TraceLMTracer:
    return TraceLMTracer(service_name=service_name, db_path=db_path)


def get_logger(name: str, request_id: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] [request_id=%(request_id)s] %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    if request_id is not None:
        adapter = logging.LoggerAdapter(logger, {"request_id": request_id})
        return adapter  # type: ignore[return-value]

    return logging.LoggerAdapter(logger, {"request_id": "-"})  # type: ignore[return-value]
