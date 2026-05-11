import logging
from typing import Optional


def get_logger(name: str, request_id: Optional[str] = None) -> logging.Logger:
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