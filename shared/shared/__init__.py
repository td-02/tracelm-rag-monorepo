from .models import JobRequest, JobResult
from .hashing import hash_payload, ConsistentHashRing

try:
    from .tracing import get_logger, init_tracer, TraceContext, Span
except ModuleNotFoundError:
    def init_tracer(*args, **kwargs):
        raise ModuleNotFoundError("tracelm is required for tracing features")

    class TraceContext:  # type: ignore[no-redef]
        pass

    class Span:  # type: ignore[no-redef]
        pass

    def get_logger(name: str, request_id: str | None = None):
        import logging
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler())
            logger.setLevel(logging.INFO)
        return logger

__all__ = [
    "JobRequest",
    "JobResult",
    "hash_payload",
    "ConsistentHashRing",
    "get_logger",
    "init_tracer",
    "TraceContext",
    "Span",
]
