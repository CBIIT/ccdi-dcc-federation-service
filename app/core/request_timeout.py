"""Stop a request that is still running at the app timeout and return an error.

The upstream proxy returns 504 when a request outlives its idle limit. This
middleware answers first, at REQUEST_TIMEOUT_SECONDS (default 100), so the
caller gets the errors envelope instead of a gateway timeout.

The reply is sent before the inner app is cancelled. Cleanup, including
closing the database session, continues after the response. Session close
does not by itself stop a Memgraph query: the driver reads the rest of the
result while closing. Stopping the query on the server is a separate step.
"""

import asyncio
import json

from app.core.logging import get_logger

logger = get_logger(__name__)

TIMEOUT_MESSAGE = "Request Timeout."


def timeout_error_body(method: str) -> bytes:
    """Errors envelope for a request that exceeded the app timeout."""
    payload = {
        "errors": [
            {
                "kind": "NotFound",
                "method": method,
                "route": "/api",
                "message": TIMEOUT_MESSAGE,
            }
        ]
    }
    return json.dumps(payload).encode()


async def _retire(task: asyncio.Task) -> None:
    """Cancel the inner app and consume its outcome so it is not left pending."""
    if task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.warning(
            "Request raised after timeout cancellation",
            error_type=type(exc).__name__,
        )


class RequestTimeoutMiddleware:
    """ASGI middleware. timeout_seconds <= 0 disables the limit."""

    def __init__(self, app, timeout_seconds: float = 100):
        self.app = app
        self.timeout_seconds = timeout_seconds

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not self.timeout_seconds or self.timeout_seconds <= 0:
            await self.app(scope, receive, send)
            return

        response_started = False
        timed_out = False
        send_lock = asyncio.Lock()

        async def send_wrapper(message):
            nonlocal response_started
            async with send_lock:
                if timed_out:
                    return
                if message["type"] == "http.response.start":
                    response_started = True
                await send(message)

        task = asyncio.create_task(self.app(scope, receive, send_wrapper))
        try:
            await asyncio.wait({task}, timeout=self.timeout_seconds)
        except asyncio.CancelledError:
            await _retire(task)
            raise

        if task.done():
            exc = task.exception()
            if exc is not None:
                raise exc
            return

        async with send_lock:
            timed_out = True
            started = response_started
        if started:
            # Headers are already out. Uvicorn closes the connection here.
            logger.warning(
                "Request exceeded timeout after the response had started",
                method=scope.get("method"),
                path=scope.get("path"),
                timeout_seconds=self.timeout_seconds,
            )
        else:
            logger.warning(
                "Request exceeded timeout; returning timeout error",
                method=scope.get("method"),
                path=scope.get("path"),
                timeout_seconds=self.timeout_seconds,
            )
            body = timeout_error_body(scope.get("method") or "GET")
            await send(
                {
                    "type": "http.response.start",
                    "status": 404,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode()),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body})
        # Answer the client before waiting on cleanup. The driver may still
        # drain the query while the session closes.
        asyncio.create_task(_retire(task))
