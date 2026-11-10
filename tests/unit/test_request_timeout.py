"""Request timeout middleware returns the errors envelope before a proxy 504."""

import asyncio
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.request_timeout import RequestTimeoutMiddleware


def _app(timeout_seconds: float) -> FastAPI:
    app = FastAPI()

    @app.get("/slow")
    async def slow():
        await asyncio.sleep(30)
        return {"ok": True}

    @app.get("/fast")
    async def fast():
        return {"ok": True}

    app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=timeout_seconds)
    return app


@pytest.mark.unit
def test_slow_request_returns_timeout_envelope():
    client = TestClient(_app(0.05))
    response = client.get("/slow")
    assert response.status_code == 404
    assert response.json() == {
        "errors": [
            {
                "kind": "NotFound",
                "method": "GET",
                "route": "/api",
                "message": "Request Timeout.",
            }
        ]
    }


@pytest.mark.unit
def test_fast_request_is_unchanged():
    client = TestClient(_app(5))
    response = client.get("/fast")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


@pytest.mark.unit
def test_timeout_response_does_not_wait_for_cleanup():
    app = FastAPI()

    @app.get("/slow")
    async def slow():
        try:
            await asyncio.sleep(30)
        finally:
            await asyncio.sleep(1)

    app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=0.05)
    started = time.monotonic()
    response = TestClient(app).get("/slow")
    elapsed = time.monotonic() - started
    assert response.status_code == 404
    assert elapsed < 0.5


@pytest.mark.unit
def test_zero_timeout_disables_the_limit():
    app = FastAPI()

    @app.get("/slow")
    async def slow():
        await asyncio.sleep(0.05)
        return {"ok": True}

    app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=0)
    response = TestClient(app).get("/slow")
    assert response.status_code == 200
    assert response.json() == {"ok": True}
