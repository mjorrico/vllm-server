FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set environment variables for optimization
ENV CONTAINER_HOME=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV UV_CACHE_DIR=/tmp/uv-cache
ENV UV_LINK_MODE=copy

WORKDIR $CONTAINER_HOME

# Create non-root user for security
# RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install only essential system dependencies for web API
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    docker.io \
    docker-compose \
    poppler-utils \
    curl \
    ca-certificates \
    # gcc \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/* && \
    rm -rf /tmp/*


# Copy pyproject.toml first for better layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies with production optimizations
RUN --mount=type=cache,target=/tmp/uv-cache \
    uv sync --frozen --no-dev

# Copy the rest of the application code
COPY . .

# Create necessary directories and set permissions
RUN mkdir -p logs temp 
# && \
# chown -R appuser:appuser /app && \
# chmod -R 755 /app && \
# chmod -R 775 temp

# Switch to non-root user
# USER appuser

# Health check for the web API
# HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
#     CMD curl -f http://localhost:8000/health || exit 1

# Expose the port the app runs on
EXPOSE 8000

# Optimized command for production web server
# Points to app/api/main.py (module app.api.main)
CMD [".venv/bin/python", "-m", "gunicorn", "app.api.main:app", \
    "--workers", "2", \
    "--worker-class", "uvicorn.workers.UvicornWorker", \
    "--bind", "0.0.0.0:8000", \
    "--worker-connections", "1000", \
    "--max-requests", "1000", \
    "--max-requests-jitter", "100", \
    "--preload", \
    "--timeout", "360", \
    "--keep-alive", "360", \
    "--access-logfile", "-", \
    "--error-logfile", "-", \
    "--log-level", "info"]