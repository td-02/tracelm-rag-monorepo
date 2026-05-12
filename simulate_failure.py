import time
from typing import Any

import docker
import requests

COORDINATOR_URL = "http://localhost:8001"
QUERY_ENDPOINT = f"{COORDINATOR_URL}/query"
STATUS_ENDPOINT = f"{COORDINATOR_URL}/cluster/status"


def _find_worker2_container(client: docker.DockerClient):
    candidates = []
    for container in client.containers.list(all=True):
        name = container.name.lower()
        if "worker2" in name or "worker_2" in name:
            candidates.append(container)
    if not candidates:
        raise RuntimeError("worker_2/worker2 container not found")
    return candidates[0]


def _send_query(i: int) -> dict[str, Any]:
    payload = {"query": f"failure-test-query-{i}", "top_k": 3}
    response = requests.post(QUERY_ENDPOINT, json=payload, timeout=20)
    response.raise_for_status()
    return response.json()


def main() -> None:
    client = docker.from_env()
    container = _find_worker2_container(client)
    print(f"Stopping container: {container.name}")
    container.kill()

    responses: list[dict[str, Any]] = []
    for i in range(10):
        data = _send_query(i)
        responses.append(data)
        print(f"Query {i+1}/10 succeeded")
        time.sleep(0.4)

    print(f"Restarting container: {container.name}")
    container.start()

    time.sleep(5)
    status = requests.get(STATUS_ENDPOINT, timeout=20)
    status.raise_for_status()
    cluster = status.json()

    worker2_state = None
    for worker_id, info in cluster.get("workers", {}).items():
        if worker_id in {"worker2", "worker_2"}:
            worker2_state = info
            break

    excluded_count = 0
    for item in responses:
        per_worker_latency = item.get("per_worker_latency", {})
        if "worker2" not in per_worker_latency and "worker_2" not in per_worker_latency:
            excluded_count += 1

    if excluded_count != len(responses):
        raise AssertionError("degraded worker was not excluded from all query responses")

    print("All queries succeeded while degraded worker was excluded.")
    print(f"worker2 post-restart status: {worker2_state}")


if __name__ == "__main__":
    main()
