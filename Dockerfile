############################
# Builder stage
############################
FROM python:3.12-slim AS builder

ARG POETRY_VERSION=1.6.1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=${POETRY_VERSION} \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VENV_IN_PROJECT=1

# System dependencies required for building (gcc, headers) & curl for potential build scripts
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Only copy dependency definition & metadata first for layer caching
COPY pyproject.toml poetry.lock* README.md ./

RUN pip install --no-cache-dir poetry==${POETRY_VERSION} \
    && poetry install --only=main --no-root

# Copy application source
COPY app/ ./app/

# Install project itself (its own package)
RUN poetry install --only-root \
    && find . -type d -name "__pycache__" -prune -exec rm -rf {} +

############################
# Runtime stage
############################
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1


# Minimal runtime deps (curl for HEALTHCHECK)
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install runtime Python dependencies with pip (globally) to ensure console scripts like uvicorn are on PATH.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code & (optionally) the builder artifacts (virtualenv) for any packages not pinned in requirements.txt
COPY --from=builder /app/app ./app

# Copy docs directory for embedded.html (used by /docs endpoint)
COPY docs/ ./docs/

# Create non-root user
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

EXPOSE 8000

LABEL org.opencontainers.image.source="https://github.com/CBIIT/ccdi-dcc-federation-service" \
      org.opencontainers.image.title="CCDI DCC Federation Service" \
      org.opencontainers.image.description="REST API for querying CCDI graph database" \
      org.opencontainers.image.version="0.1.0"

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# Use uvicorn directly from the in-project Poetry venv for faster startup
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
