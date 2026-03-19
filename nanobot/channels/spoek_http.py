"""Spoek HTTP channel — receives messages via Unix socket, returns responses synchronously.

This channel replaces nanobot's native per-bot Telegram integration for the Spoek
deployment model, where one shared @SpoekBot handles all groups and the FastAPI
dispatcher routes messages to per-Spoek nanobot processes via Unix socket.

Flow:
    FastAPI dispatcher  →  POST /message (Unix socket)
                        ←  {"response": "..."}
                        →  send reply via shared Telegram bot token
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel


class SpoekHttpConfig:
    def __init__(self, config: dict):
        self.enabled: bool = config.get("enabled", False)
        self.socket_path: str = config.get("socketPath", "/tmp/spoek.sock")
        self.tcp_port: int = int(config.get("tcpPort", 18791))
        self.allow_from: list[str] = config.get("allowFrom", ["*"])
        self.timeout: float = float(config.get("timeout", 60.0))


class SpoekHttpChannel(BaseChannel):
    """HTTP channel over Unix socket for the Spoek dispatcher.

    The Spoek API posts messages via ``POST /message`` and blocks waiting for
    the agent's response.  This channel bridges that request-reply pattern into
    nanobot's async bus without adding new server-side dependencies (pure
    asyncio streams).

    Concurrency note: each HTTP request gets a unique ``_request_id`` injected
    into the InboundMessage metadata.  The agent loop passes that metadata
    through to the OutboundMessage, so ``send()`` can resolve exactly the right
    waiting future even if multiple requests arrive concurrently.
    """

    name = "spoek_http"
    display_name = "Spoek HTTP"

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = SpoekHttpConfig(config)
        super().__init__(config, bus)
        # request_id → asyncio.Future[str]
        self._pending: dict[str, asyncio.Future] = {}
        self._server: asyncio.Server | None = None

    # ------------------------------------------------------------------
    # BaseChannel interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True

        if sys.platform == "win32":
            port = self.config.tcp_port
            self._server = await asyncio.start_server(
                self._handle_connection, host="127.0.0.1", port=port
            )
            logger.info("spoek_http: listening on tcp://127.0.0.1:{} (Windows fallback)", port)
        else:
            socket_path = self.config.socket_path
            # Remove a stale socket file left by a previous process
            try:
                Path(socket_path).unlink()
            except FileNotFoundError:
                pass
            self._server = await asyncio.start_unix_server(
                self._handle_connection, path=socket_path
            )
            logger.info("spoek_http: listening on unix:{}", socket_path)

        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        self._running = False
        if self._server:
            self._server.close()

    async def send(self, msg: OutboundMessage) -> None:
        """Called by ChannelManager when the agent has a final response.

        Progress/streaming messages are silently dropped — we only resolve the
        waiting HTTP request on the final turn response.
        """
        if msg.metadata.get("_progress"):
            return

        request_id = msg.metadata.get("_request_id")
        if not request_id:
            logger.warning("spoek_http: OutboundMessage missing _request_id, dropping")
            return

        future = self._pending.pop(request_id, None)
        if future and not future.done():
            future.set_result(msg.content or "")
        else:
            logger.warning(
                "spoek_http: no pending future for request_id={}", request_id
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            raw = await reader.read(65536)
            if not raw:
                return

            request = raw.decode("utf-8", errors="replace")

            # Parse the HTTP request line (e.g. "POST /message HTTP/1.1")
            line_end = request.find("\r\n")
            if line_end < 0:
                self._write_http(writer, 400, {"error": "bad request"})
                return

            parts = request[:line_end].split(" ", 2)
            if len(parts) < 2:
                self._write_http(writer, 400, {"error": "bad request"})
                return
            method, path = parts[0], parts[1]

            # Locate the body (after the blank line separating headers from body)
            body_start = request.find("\r\n\r\n")
            body = request[body_start + 4:] if body_start >= 0 else ""

            if method == "GET" and path == "/health":
                self._write_http(writer, 200, {"status": "ok"})
                return

            if method == "POST" and path == "/message":
                await self._handle_message_request(writer, body)
                return

            self._write_http(writer, 404, {"error": "not found"})

        except Exception as exc:
            logger.error("spoek_http: connection error: {}", exc)
        finally:
            try:
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _handle_message_request(
        self, writer: asyncio.StreamWriter, body: str
    ) -> None:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            self._write_http(writer, 400, {"error": "invalid json"})
            return

        request_id = str(uuid.uuid4())
        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[request_id] = future

        await self._handle_message(
            sender_id=str(payload.get("from_user_id", "unknown")),
            chat_id=str(payload.get("chat_id", "")),
            content=str(payload.get("text", "")),
            metadata={
                "from_user_name": payload.get("from_user_name", ""),
                "from_user_id": str(payload.get("from_user_id", "")),
                "_request_id": request_id,
            },
        )

        try:
            response_text = await asyncio.wait_for(
                future, timeout=self.config.timeout
            )
            self._write_http(writer, 200, {"response": response_text})
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            logger.warning(
                "spoek_http: agent timeout for request_id={}", request_id
            )
            self._write_http(
                writer, 200,
                {"response": "Het duurt even langer dan verwacht — ik ben nog bezig."},
            )

    @staticmethod
    def _write_http(writer: asyncio.StreamWriter, status: int, body: dict) -> None:
        payload = json.dumps(body).encode("utf-8")
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found"}.get(status, "Error")
        header = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8")
        writer.write(header + payload)

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return {
            "enabled": False,
            "socketPath": "/tmp/spoek.sock",
            "allowFrom": ["*"],
            "timeout": 60.0,
        }
