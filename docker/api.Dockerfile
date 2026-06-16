FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi \
    "uvicorn[standard]" \
    psycopg2-binary \
    "deltalake[pyarrow]" \
    pandas \
    h3 \
    pydantic

# Only the serving package + the dependency-free H3 constant module --
# avoids pulling pyspark into this image (see serving.api.h3_aggregation).
COPY src/serving ./src/serving
COPY src/streaming/__init__.py ./src/streaming/__init__.py
COPY src/streaming/h3_config.py ./src/streaming/h3_config.py

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "serving.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
