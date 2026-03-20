# Production Dockerfile for pycypher (multi-stage)
#
# Stage 1: Install dependencies with build tools (compilers, headers).
# Stage 2: Copy only the virtual environment into a slim runtime image.
#
# This produces a significantly smaller image (~400MB vs ~1.2GB) by excluding
# build-essential, git, and other build-only tooling from the final layer.
#
# Build:
#   docker build -t pycypher:latest .
#
# Run health server (container liveness probe):
#   docker run -p 8079:8079 pycypher:latest nmetl health-server --bind 0.0.0.0
#
# Note: CI (.github/workflows/ci.yml) uses Python 3.14t (free-threaded) for
# testing. This Dockerfile uses standard 3.14 for production stability.

# ── Stage 1: build ──────────────────────────────────────────────────────────
# SECURITY NOTE: When Python 3.14 reaches GA, pin base images to SHA digests
# (e.g. python:3.14@sha256:abc...) for reproducible, tamper-proof builds.
# While 3.14 is pre-release, version tags are acceptable for tracking updates.
FROM python:3.14 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Build-only system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv (prebuilt binary — faster than pip install)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Copy dependency manifests first (layer cache optimization)
COPY pyproject.toml uv.lock ./
COPY packages/pycypher/pyproject.toml packages/pycypher/pyproject.toml
COPY packages/shared/pyproject.toml packages/shared/pyproject.toml
COPY packages/fastopendata/pyproject.toml packages/fastopendata/pyproject.toml

# Copy source code
COPY packages/ packages/

# Install into a virtual environment
RUN uv sync --frozen --no-dev

# ── Stage 2: runtime ───────────────────────────────────────────────────────
FROM python:3.14-slim AS runtime

ENV PYTHONUNBUFFERED=1
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Minimal runtime dependencies only
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the virtual environment from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/packages /app/packages

# Put venv on PATH so `nmetl` is directly callable
ENV PATH="/app/.venv/bin:$PATH"

# Security: non-root user
ARG USERNAME=appuser
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME
USER $USERNAME

# Health check using the nmetl health command (exit code 0/1/2)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD ["nmetl", "health"]

# Default: start health server for container probe integration
EXPOSE 8079
CMD ["nmetl", "health-server", "--bind", "0.0.0.0"]
