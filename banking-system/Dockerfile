# Banking System — Multi-architecture Docker image
#
# Supports: linux/amd64, linux/arm64 (Apple Silicon)
# Base: Python 3.13 slim (Debian Bookworm)
#
# Build locally:
#   docker compose build
#   (or: docker build -t freight-lane-banking-system:latest ./banking-system)

FROM python:3.13-slim-bookworm

# Prevent Python from writing .pyc files and enable unbuffered output (for real-time logs)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install build dependencies for confluent-kafka (librdkafka) — available on both amd64 and arm64
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy the full project (source + metadata) then install in one step
COPY pyproject.toml ./
COPY src/ ./src/

RUN pip install --no-cache-dir .

# Remove build toolchain to shrink the image
RUN apt-get purge -y --auto-remove gcc && \
    rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["banking-system"]
CMD ["--help"]
