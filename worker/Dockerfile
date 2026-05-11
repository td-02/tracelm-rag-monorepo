FROM python:3.11-slim

WORKDIR /app

COPY worker/requirements.txt /app/worker/requirements.txt
COPY shared /app/shared
RUN pip install --no-cache-dir -r /app/worker/requirements.txt && \
    pip install --no-cache-dir -e /app/shared

COPY worker /app/worker

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]