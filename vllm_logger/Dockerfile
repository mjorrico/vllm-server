FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git ffmpeg poppler-utils curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

# Ensure uv installs to /app/.venv
RUN --mount=type=cache,target=/tmp/uv-cache \
    UV_PROJECT_ENVIRONMENT=/app/.venv uv sync --frozen

COPY . .

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

ENV CONTAINER_HOME=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV UV_CACHE_DIR=/tmp/uv-cache
ENV UV_LINK_MODE=copy

WORKDIR $CONTAINER_HOME

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    poppler-utils \
    curl \
    ca-certificates \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/* && \
    rm -rf /tmp/*

COPY --from=builder /app /app

# Create logs directory at root level (where rp_auth expects it) and app-level directories
RUN mkdir -p /logs logs temp && \
    chown -R appuser:appuser /logs logs temp && \
    chown -R appuser:appuser $CONTAINER_HOME && \
    chmod -R 755 /logs logs temp && \
    chmod -R 775 $CONTAINER_HOME 
# RUN mkdir -p logs temp course-data && \
#     chown -R appuser:appuser $CONTAINER_HOME && \
#     chmod -R 755 $CONTAINER_HOME && \
#     chown -R appuser:appuser / && \
#     chmod -R 755 / && \
#     chmod -R 775 temp && \
#     chmod -R 775 course-data

USER appuser

EXPOSE 8000

# CMD ["uv", "run", "gunicorn", "app.api.main:app", \
# Use the uv-installed environment directly instead of uv run
CMD [".venv/bin/python", "-m", "gunicorn", "api.main:app", \
    "--workers", "2", \
    "--worker-class", "uvicorn.workers.UvicornWorker", \
    "--bind", "0.0.0.0:8000", \
    "--worker-connections", "1000", \
    "--max-requests", "1000", \
    "--max-requests-jitter", "100", \
    "--preload", \
    "--timeout", "180", \
    "--keep-alive", "5", \
    "--access-logfile", "-", \
    "--error-logfile", "-", \
    "--log-level", "info"]
