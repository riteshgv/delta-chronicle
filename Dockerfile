# ─────────────────────────────────────────────────────────────
# delta-chronicle Dockerfile
# Base: Official Delta Lake image (Spark 3.5 + Delta 3.x + Python 3.10)
# ─────────────────────────────────────────────────────────────

FROM deltaio/delta-docker:latest

# Set working directory inside container
WORKDIR /app

# ── System dependencies ───────────────────────────────────────
USER root
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ───────────────────────────────────────
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# ── Copy project source ───────────────────────────────────────
COPY delta_chronicle/ ./delta_chronicle/
COPY tests/           ./tests/
COPY demo/            ./demo/
COPY setup.py         .
COPY pyproject.toml   .
COPY README.md        .

# ── Install delta-chronicle as editable package ───────────────
RUN pip3 install -e .

# ── Spark config for local mode ───────────────────────────────
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=python3

# ── Default command: run tests ────────────────────────────────
CMD ["python3", "-m", "pytest", "tests/unit/", "-v", "--tb=short"]