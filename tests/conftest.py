import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.main import setup_exception_handlers, setup_routers
from app.api.v1 import deps as api_deps
from app.core.config import Settings


class DummyAllowlist:
    def is_field_allowed(self, entity_type: str, field: str) -> bool:  # pragma: no cover
        return True


class _DummyResult:
    """Async result stub for neo4j AsyncSession.run()."""

    def __init__(self, rows=None):
        self._rows = list(rows or [])

    def __aiter__(self):
        async def _gen():
            for r in self._rows:
                yield r

        return _gen()

    async def consume(self):
        return None


class DummySession:
    """AsyncSession stub for endpoints that run direct Cypher in the endpoint module."""

    async def run(self, *args, **kwargs):
        # return no rows by default
        return _DummyResult([])


@pytest.fixture()
def app() -> FastAPI:
    """
    Unit-test FastAPI app.

    We intentionally do NOT use app.main.create_app() to avoid starting lifespan
    (Memgraph/Redis) during unit tests.
    """
    app = FastAPI()
    setup_exception_handlers(app)
    setup_routers(app)

    async def _fake_db_session():
        # Yield a dummy session; some endpoints run Cypher directly (namespace/org),
        # and contract tests should not require a live DB.
        yield DummySession()

    def _fake_settings() -> Settings:
        return Settings()

    def _fake_allowlist() -> DummyAllowlist:
        return DummyAllowlist()

    async def _no_rate_limit():
        return None

    app.dependency_overrides[api_deps.get_database_session] = _fake_db_session
    app.dependency_overrides[api_deps.get_app_settings] = _fake_settings
    app.dependency_overrides[api_deps.get_allowlist] = _fake_allowlist
    app.dependency_overrides[api_deps.check_rate_limit] = _no_rate_limit

    return app


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


