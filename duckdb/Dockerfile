FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        less \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt /tmp/requirements-duckdb.txt
COPY duckdb_shell.py /usr/local/bin/duckdb_shell.py

RUN pip install --no-cache-dir -r /tmp/requirements-duckdb.txt

ENV DUCKDB_PATH=/data/analytics.duckdb \
    AWS_DEFAULT_REGION=us-east-1 \
    AWS_S3_USE_SSL=true

ENTRYPOINT ["python", "/usr/local/bin/duckdb_shell.py"]
