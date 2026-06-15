FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests confluent-kafka pydantic python-dotenv

COPY src/ingestion ./src/ingestion

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "ingestion.gbfs_poller"]
