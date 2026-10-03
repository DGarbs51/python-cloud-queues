"""ASGI entrypoint: uvicorn asgi:app --host :: --port $PORT"""

from __future__ import annotations

import asyncio

from laravel_cloud_logging import asgi_middleware

import app as app_module

app_module.started("uvicorn")


async def app(scope: dict, receive, send) -> None:
    if scope["type"] == "lifespan":
        while (message := await receive())["type"] != "lifespan.shutdown":
            await send({"type": "lifespan.startup.complete"})
        await send({"type": "lifespan.shutdown.complete"})
        return
    if scope["type"] != "http":
        return
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
        # handle() is synchronous (Redis); run it off the event loop so slow routes never stall others.
        status, headers, payload = await asyncio.to_thread(app_module.handle, scope["method"], path, headers, body)
    await send({"type": "http.response.start", "status": status,
                "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers]})
    await send({"type": "http.response.body", "body": payload})


# Adds Cloud-Request-ID to every log line written during the request.
app = asgi_middleware(app)
