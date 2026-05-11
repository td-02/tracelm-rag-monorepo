from fastapi import FastAPI
from chromadb import HttpClient

from shared import JobRequest, JobResult, hash_payload, get_logger

app = FastAPI(title="worker")
logger = get_logger("worker")
chroma = HttpClient(host="chromadb", port=8000)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "worker"}


@app.post("/process", response_model=JobResult)
def process(job: JobRequest) -> JobResult:
    payload_hash = hash_payload(job.model_dump_json())
    logger.info("Processing job", extra={"request_id": job.job_id})

    # Keep a tiny heartbeat call so ChromaDB is actually integrated.
    chroma.heartbeat()

    return JobResult(
        job_id=job.job_id,
        status="processed",
        output={"payload_hash": payload_hash},
    )