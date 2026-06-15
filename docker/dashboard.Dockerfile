FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit \
    pydeck \
    pandas \
    h3 \
    requests

# Only the dashboard package + the dependency-free H3 constant module --
# avoids pulling pyspark into this image (see serving.dashboard.app).
COPY src/serving ./src/serving
COPY src/streaming/__init__.py ./src/streaming/__init__.py
COPY src/streaming/h3_config.py ./src/streaming/h3_config.py

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

EXPOSE 8501

CMD ["streamlit", "run", "src/serving/dashboard/app.py", "--server.address=0.0.0.0"]
