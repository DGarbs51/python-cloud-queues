"""ASGI entrypoint: uvicorn asgi:app --host :: --port $PORT"""

from __future__ import annotations

import asyncio
import time

from laravel_cloud_logging import asgi_middleware

import app as app_module
import checks

app_module.started("uvicorn")


async def app(scope: dict, receive, send) -> None:
    if scope["type"] == "lifespan":
        while (message := await receive())["type"] != "lifespan.shutdown":
            await send({"type": "lifespan.startup.complete"})
        await send({"type": "lifespan.shutdown.complete"})
        return
    if scope["type"] == "websocket":
        await _websocket(scope, receive, send)
        return
    if scope["type"] != "http":
        return
    # The address nginx connected from: ::1 when it reaches the app over IPv6, 127.0.0.1 over IPv4.
    checks.PEER.set((scope.get("client") or ("",))[0])
    body, too_large = b"", False
    while True:
        message = await receive()
        body += message.get("body", b"")
        if len(body) > app_module.MAX_BODY:
            too_large = True
            break
        if not message.get("more_body"):
            break
    if too_large:
        status, headers, payload = app_module.json_response(400, {"error": "body exceeds 64 KiB"})
    else:
        path = scope["path"] + ("?" + scope["query_string"].decode("latin-1") if scope.get("query_string") else "")
        headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in scope["headers"]}
        if scope["method"] == "GET" and scope["path"] == "/api/checks":
            # The one check that must run on the event loop: 50 concurrent 0.2 s sleeps take ~0.2 s unless it is blocked.
            started = time.monotonic()
            await asyncio.gather(*(asyncio.sleep(0.2) for _ in range(50)))
            checks.LOOP_SECONDS.set(time.monotonic() - started)
        # handle() is synchronous (Redis); run it off the event loop so slow routes never stall others.
        status, headers, payload = await asyncio.to_thread(app_module.handle, scope["method"], path, headers, body)
    await send({"type": "http.response.start", "status": status,
                "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers]})
    await send({"type": "http.response.body", "body": payload})


async def _websocket(scope: dict, receive, send) -> None:
    """GET /ws/echo: echo text messages until the client closes. Used by the WebSocket check."""
    if (await receive())["type"] != "websocket.connect":
        return
    if scope["path"] != "/ws/echo":
        await send({"type": "websocket.close", "code": 1008})
        return
    await send({"type": "websocket.accept"})
    while (message := await receive())["type"] == "websocket.receive":
        await send({"type": "websocket.send", "text": message.get("text") or ""})


# Adds Cloud-Request-ID to every log line written during the request.
app = asgi_middleware(app)
