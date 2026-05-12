import asyncio
import csv
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx
from datasets import load_dataset
from tqdm import tqdm

COORDINATOR_INGEST_URL = "http://localhost:8000/ingest"
CONCURRENCY = 20
FAILED_CSV = Path("failed_ingestions.csv")


def _prepare_documents() -> list[dict[str, Any]]:
    ds = load_dataset("wikipedia", "20220301.en", split="train[:20000]")
    docs: list[dict[str, Any]] = []
    for i, row in enumerate(ds):
        text = (row.get("text") or "").strip()
        title = (row.get("title") or "").strip()
        if not text:
            continue

        doc_id = str(row.get("id") or f"wiki-{i}")
        docs.append(
            {
                "document_id": doc_id,
                "text": text,
                "metadata": {
                    "source": "wikipedia",
                    "title": title,
                    "dataset": "20220301.en",
                },
            }
        )
    return docs


async def _ingest_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    payload: dict[str, Any],
) -> dict[str, Any]:
    async with semaphore:
        t0 = time.perf_counter()
        try:
            response = await client.post(COORDINATOR_INGEST_URL, json=payload)
            latency_ms = (time.perf_counter() - t0) * 1000.0
            response.raise_for_status()
            data = response.json()
            return {
                "ok": True,
                "document_id": payload["document_id"],
                "worker_id": data.get("worker_id", "unknown"),
                "latency_ms": float(data.get("total_latency_ms", latency_ms)),
            }
        except Exception as exc:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            return {
                "ok": False,
                "document_id": payload["document_id"],
                "error": str(exc),
                "latency_ms": latency_ms,
            }


async def main() -> None:
    started = time.perf_counter()
    documents = _prepare_documents()

    if FAILED_CSV.exists():
        FAILED_CSV.unlink()

    semaphore = asyncio.Semaphore(CONCURRENCY)
    worker_counts: Counter[str] = Counter()
    success_count = 0
    failed_count = 0
    latency_sum_ms = 0.0

    async with httpx.AsyncClient(timeout=120.0) as client:
        tasks = [
            asyncio.create_task(_ingest_one(client, semaphore, doc))
            for doc in documents
        ]

        with FAILED_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["document_id", "error"])
            writer.writeheader()

            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Ingesting Wikipedia"):
                result = await coro
                latency_sum_ms += result.get("latency_ms", 0.0)

                if result["ok"]:
                    success_count += 1
                    worker_counts[result["worker_id"]] += 1
                else:
                    failed_count += 1
                    writer.writerow(
                        {
                            "document_id": result["document_id"],
                            "error": result["error"],
                        }
                    )

    elapsed_s = time.perf_counter() - started
    avg_latency_ms = latency_sum_ms / len(documents) if documents else 0.0

    print("\nIngestion Summary")
    print(f"Total attempted: {len(documents)}")
    print(f"Total ingested: {success_count}")
    print(f"Total failed: {failed_count}")
    print(f"Per-worker distribution: {dict(worker_counts)}")
    print(f"Total time (s): {elapsed_s:.2f}")
    print(f"Average latency per document (ms): {avg_latency_ms:.2f}")
    print(f"Failed CSV: {FAILED_CSV.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
