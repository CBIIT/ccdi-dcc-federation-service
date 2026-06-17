############################
# Builder stage
############################
# Pin trixie: libssl3t64 / openssl-provider-legacy match this series (unpinned slim may switch Debian).
FROM python:3.12-slim-trixie AS builder

ARG POETRY_VERSION=1.6.1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=${POETRY_VERSION} \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VENV_IN_PROJECT=1

# System dependencies required for building (gcc, headers) & curl for potential build scripts
# DSA-6335: openssl 3.5.6-1~deb13u2+ from trixie-security (CVE-2026-34182 and related OpenSSL CVEs).
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && apt-get install -y --no-install-recommends --only-upgrade \
        openssl libssl3t64 openssl-provider-legacy \
        libgnutls30t64 \
        libcap2 \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W openssl)" ge 3.5.6-1~deb13u2 \
    || (echo "FATAL: openssl < 3.5.6-1~deb13u2 (DSA-6335). apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && rm -rf /var/lib/apt/lists/*


# Patch pip to fix security vulnerabilities (builder only; runtime removes pip after install).
RUN python -m pip install --no-cache-dir --upgrade "pip==26.1.2"

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
FROM python:3.12-slim-trixie AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1


# Minimal runtime deps (curl for HEALTHCHECK)
# DSA-6335: openssl 3.5.6-1~deb13u2+ from trixie-security (CVE-2026-34182 and related OpenSSL CVEs).
# Remove perl-base after all apt installs — unused by this Python service; clears open perl CVEs on
# Debian Trixie until a patched package ships. See docs/container-image-security.md.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && apt-get install -y --no-install-recommends --only-upgrade \
        openssl libssl3t64 openssl-provider-legacy \
        libgnutls30t64 \
        libcap2 \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W openssl)" ge 3.5.6-1~deb13u2 \
    || (echo "FATAL: openssl < 3.5.6-1~deb13u2 (DSA-6335). apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && apt-get remove -y --allow-remove-essential --purge perl-base \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install runtime deps, then remove pip — not needed at runtime and clears PRISMA-2022-0168 /
# other pip CVEs on scanners (base image ships pip 24.1.1; upgrade alone still leaves pip in SBOM).
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --upgrade "pip==26.1.2" \
    && python -m pip install --no-cache-dir -r requirements.txt \
    && python -m pip uninstall -y pip \
    && rm -rf /root/.cache/pip

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

# Uvicorn is installed from requirements.txt and invoked on PATH (runtime does not use the builder Poetry venv).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
