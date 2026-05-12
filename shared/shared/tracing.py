import logging
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock
from typing import Any, Mapping

from tracelm.context import generate_trace_id
from tracelm.distributed.tracecontext import build_traceparent, parse_traceparent
from tracelm.span import Span
from tracelm.storage import sqlite_store
from tracelm.storage.sqlite_store import save_trace
from tracelm.trace import Trace


@dataclass
class TraceContext:
    trace_id: str
    span_id: str | None


class TraceLMTracer:
    def __init__(self, service_name: str, db_path: str | None = None) -> None:
        self.service_name = service_name
        self.db_path = db_path or os.getenv("TRACE_DB_PATH", "tracelm_traces.db")
        sqlite_store.DB_FILE = self.db_path
        sqlite_store.init_db()
        self._lock = Lock()
        self._traces: dict[str, Trace] = {}
        self._open_counts: dict[str, int] = {}

    def create_span(self, name: str, parent_context: TraceContext | None = None) -> Span:
        with self._lock:
            if parent_context:
                trace_id = parent_context.trace_id
                parent_id = parent_context.span_id
            else:
                trace_id = generate_trace_id()
                parent_id = None

            trace = self._traces.get(trace_id)
            if trace is None:
                trace = Trace(trace_id=trace_id)
                self._traces[trace_id] = trace
                self._open_counts[trace_id] = 0

            span = Span(trace_id=trace_id, parent_id=parent_id, name=name)
            span.metadata["service_name"] = self.service_name
            trace.add_span(span)
            return span

    def inject_headers(self, span: Span) -> dict[str, str]:
        return {"traceparent": build_traceparent(span.trace_id, span.span_id)}

    def extract_context(self, headers: Mapping[str, str] | None) -> TraceContext | None:
        if not headers:
            return None
        value = headers.get("traceparent") or headers.get("Traceparent")
        if not value:
            return None
        parsed = parse_traceparent(value)
        if parsed is None:
            return None
        trace_id, parent_span_id = parsed
        return TraceContext(trace_id=trace_id, span_id=parent_span_id)

    @contextmanager
    def span(self, span: Span, attributes: dict[str, Any] | None = None):
        attrs = attributes or {}
        for k, v in attrs.items():
            span.metadata[k] = v

        with self._lock:
            self._open_counts[span.trace_id] = self._open_counts.get(span.trace_id, 0) + 1

        try:
            yield span.metadata
        except Exception as exc:
            span.error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            span.finish()
            should_flush = False
            with self._lock:
                self._open_counts[span.trace_id] = max(0, self._open_counts.get(span.trace_id, 1) - 1)
                if self._open_counts[span.trace_id] == 0:
                    should_flush = True

            if should_flush:
                with self._lock:
                    trace = self._traces.pop(span.trace_id, None)
                    self._open_counts.pop(span.trace_id, None)
                if trace is not None:
                    trace.validate()
                    save_trace(trace)

    def fetch_recent_traces(self, limit: int = 100) -> list[dict[str, Any]]:
        sqlite_store.init_db()
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT data FROM traces ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()

        import json

        return [json.loads(row[0]) for row in rows]


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
