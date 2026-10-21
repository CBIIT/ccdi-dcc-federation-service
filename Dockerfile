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
# DSA-6531: openssl 3.5.7-1~deb13u3+ (CVE-2026-84782/77696/75806/75805/54872/35189; supersedes DSA-6465).
# DSA-6442: util-linux 2.41.5-0+deb13u1+ (CVE-2026-13595/27456 and related libblkid issues).
# DSA-6530: libpcre2-8-0 10.46-1~deb13u3+ (CVE-2026-103111; also CVE-2026-89157/89161).
# glibc 2.41-12+deb13u4+, curl 8.14.1-2+deb13u7+, libssh2 1.11.1-1+deb13u2+ (trixie, not trixie-security),
# gzip 1.13-1+deb13u1+. glibc/curl floors clear the CVEs Debian has patched; some remain with no DSA.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && apt-get install -y --no-install-recommends --only-upgrade \
        openssl libssl3t64 openssl-provider-legacy \
        libgnutls30t64 \
        libcap2 \
        util-linux libblkid1 libuuid1 libmount1 libsmartcols1 \
        bsdutils mount login \
        libc6 libc-bin \
        curl libcurl4t64 \
        libpcre2-8-0 \
        libssh2-1t64 \
        gzip \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W openssl)" ge 3.5.7-1~deb13u3 \
    || (echo "FATAL: openssl < 3.5.7-1~deb13u3 (DSA-6531). apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W libblkid1)" ge 2.41.5-0+deb13u1 \
    || (echo "FATAL: libblkid1 < 2.41.5-0+deb13u1 (DSA-6442 / CVE-2026-13595). apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W libpcre2-8-0)" ge 10.46-1~deb13u3 \
    || (echo "FATAL: libpcre2-8-0 < 10.46-1~deb13u3 (DSA-6530). apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W libc6)" ge 2.41-12+deb13u4 \
    || (echo "FATAL: libc6 < 2.41-12+deb13u4. apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W curl)" ge 8.14.1-2+deb13u7 \
    || (echo "FATAL: curl < 8.14.1-2+deb13u7. apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W libssh2-1t64)" ge 1.11.1-1+deb13u2 \
    || (echo "FATAL: libssh2-1t64 < 1.11.1-1+deb13u2. apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W gzip)" ge 1.13-1+deb13u1 \
    || (echo "FATAL: gzip < 1.13-1+deb13u1. apt-get update may be stale — rebuild with --no-cache." && exit 1) \
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
# DSA-6531: openssl 3.5.7-1~deb13u3+ (CVE-2026-84782/77696/75806/75805/54872/35189; supersedes DSA-6465).
# DSA-6442: util-linux 2.41.5-0+deb13u1+ (CVE-2026-13595/27456 and related libblkid issues).
# DSA-6530: libpcre2-8-0 10.46-1~deb13u3+ (CVE-2026-103111; also CVE-2026-89157/89161).
# glibc 2.41-12+deb13u4+, curl 8.14.1-2+deb13u7+, libssh2 1.11.1-1+deb13u2+ (trixie, not trixie-security),
# gzip 1.13-1+deb13u1+. glibc/curl floors clear the CVEs Debian has patched; some remain with no DSA.
# Remove perl-base after all apt installs — unused by this Python service; clears open perl CVEs on
# Debian Trixie until a patched package ships. See docs/container-image-security.md.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && apt-get install -y --no-install-recommends --only-upgrade \
        openssl libssl3t64 openssl-provider-legacy \
        libgnutls30t64 \
        libcap2 \
        util-linux libblkid1 libuuid1 libmount1 libsmartcols1 \
        bsdutils mount login \
        libc6 libc-bin \
        curl libcurl4t64 \
        libpcre2-8-0 \
        libssh2-1t64 \
        gzip \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W openssl)" ge 3.5.7-1~deb13u3 \
    || (echo "FATAL: openssl < 3.5.7-1~deb13u3 (DSA-6531). apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W libblkid1)" ge 2.41.5-0+deb13u1 \
    || (echo "FATAL: libblkid1 < 2.41.5-0+deb13u1 (DSA-6442 / CVE-2026-13595). apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W libpcre2-8-0)" ge 10.46-1~deb13u3 \
    || (echo "FATAL: libpcre2-8-0 < 10.46-1~deb13u3 (DSA-6530). apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W libc6)" ge 2.41-12+deb13u4 \
    || (echo "FATAL: libc6 < 2.41-12+deb13u4. apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W curl)" ge 8.14.1-2+deb13u7 \
    || (echo "FATAL: curl < 8.14.1-2+deb13u7. apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W libssh2-1t64)" ge 1.11.1-1+deb13u2 \
    || (echo "FATAL: libssh2-1t64 < 1.11.1-1+deb13u2. apt-get update may be stale — rebuild with --no-cache." && exit 1) \
    && dpkg --compare-versions "$(dpkg-query -f '${Version}' -W gzip)" ge 1.13-1+deb13u1 \
    || (echo "FATAL: gzip < 1.13-1+deb13u1. apt-get update may be stale — rebuild with --no-cache." && exit 1) \
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
