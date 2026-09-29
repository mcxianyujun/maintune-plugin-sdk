"""Cross-platform stdio JSON-RPC runner for isolated Python plugins."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import os
import re
import sys
import threading
import traceback
from pathlib import Path
from typing import Any, TextIO

from .api import PluginAPI, PluginContext
from .protocol import MAX_MESSAGE_BYTES, PROTOCOL


class _RedactingStream:
    def __init__(self, destination: TextIO, peer: _Peer):
        self.destination = destination
        self.peer = peer
        self.buffered = ""
        self.lock = threading.Lock()

    def write(self, value: str) -> int:
        with self.lock:
            self.buffered += str(value)
            while "\n" in self.buffered:
                line, self.buffered = self.buffered.split("\n", 1)
                self.destination.write(self.peer.redact(line + "\n"))
            if len(self.buffered) > 8192:
                self.destination.write(self.peer.redact(self.buffered))
                self.buffered = ""
        return len(value)

    def flush(self) -> None:
        with self.lock:
            if self.buffered:
                self.destination.write(self.peer.redact(self.buffered))
                self.buffered = ""
            self.destination.flush()

    @property
    def encoding(self) -> str | None:
        return self.destination.encoding

    def isatty(self) -> bool:
        return self.destination.isatty()

    def fileno(self) -> int:
        return self.destination.fileno()


class _Peer:
    def __init__(self, stdout: TextIO | None = None):
        self.stdout = stdout or sys.stdout
        self.writer_lock = asyncio.Lock()
        self.pending: dict[str, asyncio.Future[Any]] = {}
        self.sequence = 0
        self.context: PluginContext | None = None
        self.api = PluginAPI()
        self.module: Any = None
        self.stopped = asyncio.Event()
        self.secrets: list[str] = []
        self.invocations: dict[str, PluginContext] = {}

    def redact(self, value: str) -> str:
        for secret in self.secrets:
            value = value.replace(secret, "[REDACTED]")
        return re.sub(
            r"(?i)(token|password|secret|api[_-]?key)\s*[:=]\s*\S+",
            r"\1=[REDACTED]", value,
        )

    def clean(self, value: str) -> str:
        return self.redact(value)[:1000]

    async def send(self, message: dict[str, Any]) -> None:
        encoded = json.dumps(message, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"
        if len(encoded.encode("utf-8")) > MAX_MESSAGE_BYTES:
            raise ValueError("Plugin protocol message exceeds 1 MiB")
        async with self.writer_lock:
            self.stdout.write(encoded)
            self.stdout.flush()

    async def call_core(self, method: str, params: dict[str, Any]) -> Any:
        self.sequence += 1
        identifier = f"core-{self.sequence}"
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.pending[identifier] = future
        try:
            await self.send({
                "jsonrpc": "2.0", "id": identifier, "method": "core.call",
                "params": {"method": method, "params": params},
            })
            return await asyncio.wait_for(future, timeout=30)
        finally:
            self.pending.pop(identifier, None)

    async def respond(self, identifier: str, *, result: Any = None, error: Exception | None = None) -> None:
        if error is not None:
            await self.send({
                "jsonrpc": "2.0", "id": identifier,
                "error": {"code": "PLUGIN_ERROR", "type": type(error).__name__,
                          "message": self.clean(str(error) or type(error).__name__)},
            })
        else:
            await self.send({"jsonrpc": "2.0", "id": identifier, "result": result})

    async def initialize(self, params: dict[str, Any]) -> dict[str, Any]:
        if self.context is not None:
            raise ValueError("Plugin has already started")
        if params.get("protocol") != PROTOCOL:
            raise ValueError("Unsupported Plugin API protocol")
        raw_context = params.get("context") or {}
        config = raw_context.get("config") or {}
        if not isinstance(config, dict):
            raise ValueError("Plugin config must be an object")
        secret_fields = raw_context.get("secret_fields") or {}
        if isinstance(secret_fields, dict):
            secret_names = {name for name, enabled in secret_fields.items() if enabled}
        elif isinstance(secret_fields, (list, tuple)):
            secret_names = set(secret_fields)
        else:
            raise ValueError("secret_fields must be an object or list")
        self.secrets = [value for name, value in config.items()
                        if name in secret_names and isinstance(value, str) and value]

        plugin_src, entrypoint = Path(params["plugin_src"]), str(params["entrypoint"])
        if not plugin_src.is_dir() or not entrypoint:
            raise ValueError("Plugin source or entrypoint is invalid")
        data_dir = Path(raw_context["data_dir"])
        data_dir.mkdir(parents=True, exist_ok=True)
        sys.path.insert(0, str(plugin_src))
        self.module = importlib.import_module(entrypoint)
        register = getattr(self.module, "register", None)
        if not callable(register):
            raise ValueError("Plugin entrypoint must define register(api)")
        registration = register(self.api)
        if inspect.isawaitable(registration):
            await registration
        self.context = PluginContext(
            plugin_id=str(raw_context["plugin_id"]),
            plugin_version=str(raw_context["plugin_version"]),
            data_dir=data_dir,
            config=config,
            _core_call=self.call_core,
        )
        return {"protocol": PROTOCOL, "registrations": self.api.registrations()}

    async def handle(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        identifier = str(message["id"]) if message.get("id") is not None else ""
        params = message.get("params") or {}
        try:
            if not isinstance(params, dict):
                raise ValueError("Plugin API params must be an object")
            if method == "lifecycle.start":
                result = await self.initialize(params)
            elif method == "health":
                result = {"ok": self.context is not None, "protocol": PROTOCOL}
            elif method == "lifecycle.stop":
                stop = getattr(self.module, "stop", None)
                if callable(stop):
                    result = stop(self.context)
                    if inspect.isawaitable(result):
                        await result
                result = {"stopped": True}
            elif method == "extension.cancel":
                invocation = self.invocations.get(str(params.get("invocation_id", "")))
                if invocation is not None:
                    invocation._cancelled.set()
                result = {"notified": invocation is not None}
            elif method == "config.migrate":
                if self.context is None:
                    raise RuntimeError("Plugin has not started")
                previous = params.get("old_config") or {}
                if not isinstance(previous, dict):
                    raise ValueError("Previous plugin config must be an object")
                migrate = getattr(self.module, "migrate_config", None)
                result = migrate(previous, str(params.get("old_version", "")), str(params.get("new_version", ""))) if callable(migrate) else previous
                if inspect.isawaitable(result):
                    result = await result
                if not isinstance(result, dict):
                    raise ValueError("Plugin config migration must return an object")
            elif method == "lifecycle.upgrade":
                if self.context is None:
                    raise RuntimeError("Plugin has not started")
                upgrade = getattr(self.module, "on_upgrade", None)
                result = upgrade(self.context, str(params.get("old_version", "")), str(params.get("new_version", ""))) if callable(upgrade) else None
                if inspect.isawaitable(result):
                    result = await result
                result = {"upgraded": True}
            elif method == "extension.invoke":
                if self.context is None:
                    raise RuntimeError("Plugin has not started")
                invocation_id = str(params.get("invocation_id") or identifier)
                if not invocation_id or invocation_id in self.invocations:
                    raise ValueError("Invocation ID is missing or already active")
                call_context = PluginContext(
                    plugin_id=self.context.plugin_id,
                    plugin_version=self.context.plugin_version,
                    data_dir=self.context.data_dir,
                    config=self.context.config,
                    invocation_id=invocation_id,
                    _core_call=self.call_core,
                )
                self.invocations[invocation_id] = call_context
                try:
                    payload = params.get("input") or {}
                    if not isinstance(payload, dict):
                        raise ValueError("Extension input must be an object")
                    result = await self.api.invoke(str(params["kind"]), str(params["name"]), call_context, payload)
                finally:
                    self.invocations.pop(invocation_id, None)
            else:
                raise ValueError("Unsupported Plugin API v2 method")
            if identifier:
                await self.respond(identifier, result=result)
        except Exception as error:
            traceback.print_exc(file=sys.stderr)
            if identifier:
                try:
                    await self.respond(identifier, error=error)
                except Exception:
                    traceback.print_exc(file=sys.stderr)
        finally:
            if method == "lifecycle.stop":
                self.stopped.set()

    def accept_response(self, message: dict[str, Any]) -> None:
        future = self.pending.get(str(message.get("id", "")))
        if future is None or future.done():
            return
        if "error" in message:
            detail = message.get("error") or {}
            message_text = detail.get("message", "Core API error") if isinstance(detail, dict) else "Core API error"
            future.set_exception(RuntimeError(self.clean(str(message_text))))
        else:
            future.set_result(message.get("result"))


async def _serve() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
    peer = _Peer()
    original_stderr = sys.stderr
    redacted_stderr = _RedactingStream(original_stderr, peer)
    # Plugin print/log output uses stderr; the original stdout is JSON-RPC only.
    sys.stdout = redacted_stderr
    sys.stderr = redacted_stderr
    loop = asyncio.get_running_loop()
    incoming: asyncio.Queue[bytes] = asyncio.Queue()

    def read_stdin() -> None:
        buffered = bytearray()
        descriptor = sys.stdin.fileno()
        while True:
            try:
                chunk = os.read(descriptor, 4096)
            except Exception:
                chunk = b""
            if not chunk:
                try:
                    loop.call_soon_threadsafe(incoming.put_nowait, b"")
                except RuntimeError:
                    pass
                return
            buffered.extend(chunk)
            if len(buffered) > MAX_MESSAGE_BYTES and b"\n" not in buffered:
                try:
                    loop.call_soon_threadsafe(incoming.put_nowait, bytes(buffered))
                except RuntimeError:
                    pass
                return
            while b"\n" in buffered:
                line, _, remainder = buffered.partition(b"\n")
                buffered = bytearray(remainder)
                line += b"\n"
                if len(line) > MAX_MESSAGE_BYTES:
                    try:
                        loop.call_soon_threadsafe(incoming.put_nowait, line)
                    except RuntimeError:
                        pass
                    return
                try:
                    loop.call_soon_threadsafe(incoming.put_nowait, line)
                except RuntimeError:
                    return

    threading.Thread(target=read_stdin, name="maintune-plugin-stdin", daemon=True).start()
    active: set[asyncio.Task[None]] = set()
    try:
        while not peer.stopped.is_set():
            read_task = asyncio.create_task(incoming.get())
            stop_task = asyncio.create_task(peer.stopped.wait())
            done, waiting = await asyncio.wait({read_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
            for task in waiting:
                task.cancel()
            await asyncio.gather(*waiting, return_exceptions=True)
            if peer.stopped.is_set():
                break
            line = read_task.result()
            if not line or len(line) > MAX_MESSAGE_BYTES:
                break
            try:
                message = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                continue
            if "method" in message:
                task = asyncio.create_task(peer.handle(message))
                active.add(task)
                task.add_done_callback(active.discard)
            elif "id" in message:
                peer.accept_response(message)
    finally:
        for task in active:
            task.cancel()
        await asyncio.gather(*active, return_exceptions=True)
        redacted_stderr.flush()


def run_stdio() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    run_stdio()
