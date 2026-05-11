# Python Monorepo (Coordinator + Workers)

This monorepo contains:

- `coordinator/`: FastAPI service that dispatches jobs to workers.
- `worker/`: FastAPI service that processes jobs.
- `shared/`: Shared Pydantic v2 models and common utilities.
- `docker-compose.yml`: Coordinator + 3 workers + Redis + ChromaDB.

## Requirements

- Python 3.11
- Docker + Docker Compose

## Project Structure

```text
python-monorepo/
+-- coordinator/
¦   +-- app/main.py
¦   +-- Dockerfile
¦   +-- requirements.txt
+-- worker/
¦   +-- app/main.py
¦   +-- Dockerfile
¦   +-- requirements.txt
+-- shared/
¦   +-- shared/
¦   ¦   +-- __init__.py
¦   ¦   +-- hashing.py
¦   ¦   +-- models.py
¦   ¦   +-- tracing.py
¦   +-- setup.py
+-- docker-compose.yml
+-- README.md
```

## Run with Docker Compose

```bash
docker compose up --build
```

Coordinator will be available at:

- `http://localhost:8001`
- Health check: `GET /health`
- Enqueue endpoint: `POST /enqueue`

### Example request

```bash
curl -X POST http://localhost:8001/enqueue \
  -H "Content-Type: application/json" \
  -d '{"job_id":"job-1","payload":{"message":"hello"}}'
```

## Notes

- All models are implemented with **Pydantic v2**.
- Coordinator uses Redis for simple round-robin worker selection.
- Workers call ChromaDB heartbeat to validate dependency connectivity.