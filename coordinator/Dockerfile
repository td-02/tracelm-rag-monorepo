FROM python:3.11-slim

WORKDIR /app

COPY coordinator/requirements.txt /app/coordinator/requirements.txt
COPY shared /app/shared
RUN pip install --no-cache-dir -r /app/coordinator/requirements.txt && \
    pip install --no-cache-dir -e /app/shared

COPY coordinator /app/coordinator

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]