from fastapi import FastAPI
from pydantic import BaseModel
import redis
import requests

from shared import JobRequest, hash_payload, get_logger

app = FastAPI(title="coordinator")
logger = get_logger("coordinator")
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


class EnqueueResponse(BaseModel):
    accepted: bool
    worker_url: str
    payload_hash: str


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "coordinator"}


@app.post("/enqueue", response_model=EnqueueResponse)
def enqueue(job: JobRequest) -> EnqueueResponse:
    queue_len = redis_client.incr("round_robin")
    worker_id = (queue_len % 3) + 1
    worker_url = f"http://worker{worker_id}:8000/process"

    payload_hash = hash_payload(job.model_dump_json())
    logger.info("Dispatching job", extra={"request_id": job.job_id})

    requests.post(worker_url, json=job.model_dump(), timeout=5)
    return EnqueueResponse(accepted=True, worker_url=worker_url, payload_hash=payload_hash)