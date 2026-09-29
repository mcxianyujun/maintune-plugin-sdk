"""Public SDK tests that run without importing Maintune Core."""

from __future__ import annotations

import asyncio
import json
import os
import queue
import subprocess
import sys
import threading
from pathlib import Path

import pytest


from maintune_plugin_sdk import PluginAPI, PluginContext, PluginRegistrationError, schema_for_callable


def test_tool_schema_is_compatible_with_the_supported_core_subset() -> None:
    def lookup(context: PluginContext, query: str, limit: int = 5, region: str | None = None) -> str:
        return query

    schema = schema_for_callable(lookup)
    assert schema == {
        "type": "object",
        "properties": {
            "query": {"type": "string"},
            "limit": {"type": "integer", "default": 5},
            "region": {"type": "string"},
        },
        "required": ["query"],
        "additionalProperties": False,
    }
    assert all(isinstance(value["type"], str) for value in schema["properties"].values())

    def imprecise(items: list) -> None:
        pass

    with pytest.raises(PluginRegistrationError, match="item annotation"):
        schema_for_callable(imprecise)


def test_registration_and_dispatch_are_core_independent(tmp_path: Path) -> None:
    api = PluginAPI()
    context = PluginContext("sample", "1.0", tmp_path, {"bridge_token": "a-private-value"})
    assert "a-private-value" not in repr(context)

    def multiply(value: int, factor: int = 2) -> int:
        return value * factor

    api.register_tool("multiply", multiply, description="Multiply a value", recommended_agents=["worker", "worker"])
    api.register_hook("task.started", lambda payload: {"seen": payload})
    api.register_service("ping", lambda payload: payload)
    api.register_model_provider("sample", lambda payload: payload, config_schema={"type": "object"}, models=["sample-model"])
    api.register_sandbox_provider("local", lambda payload: payload, config_schema={"type": "object"})

    registration = api.registrations()
    assert [item["kind"] for item in registration] == ["tool", "hook", "service", "provider", "provider"]
    assert registration[0]["recommended_agents"] == ["worker"]
    assert [item["provider_kind"] for item in registration[-2:]] == ["model", "sandbox"]
    registration[0]["description"] = "changed"
    assert api.registrations()[0]["description"] == "Multiply a value"
    assert asyncio.run(api.invoke("tool", "multiply", context, {"value": 3})) == 6
    assert asyncio.run(api.invoke("service", "ping", context, {"ok": True})) == {"ok": True}

    with pytest.raises(PluginRegistrationError, match="Duplicate"):
        api.register_tool("multiply", multiply, description="duplicate")
    with pytest.raises(PluginRegistrationError, match="object"):
        api.register_tool("invalid", multiply, description="invalid", input_schema={})


def test_stdio_json_rpc_parallel_calls_cancellation_and_secret_redaction(tmp_path: Path) -> None:
    plugin_src = tmp_path / "plugin"
    plugin_src.mkdir()
    (plugin_src / "example.py").write_text(
        """\
from maintune_plugin_sdk import PluginContext

def register(api):
    api.register_service("read", read)
    api.register_service("leak", leak)
    api.register_tool("wait", wait, description="Wait for an advisory cancellation")

async def read(context: PluginContext, payload: dict):
    return await context.call_core("task.read", payload)

async def wait(context: PluginContext, label: str):
    await context.call_core("test.ready", {"label": label})
    await context.wait_cancelled()
    return {"cancelled": context.cancelled, "invocation_id": context.invocation_id}

def leak(context: PluginContext, payload: dict):
    print(context.config["bridge_token"])
    raise RuntimeError("failed with " + context.config["bridge_token"])
""",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    process = subprocess.Popen(
        [sys.executable, "-m", "maintune_plugin_sdk.runner"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        bufsize=1,
        env=environment,
    )
    assert process.stdin is not None and process.stdout is not None and process.stderr is not None
    received: queue.Queue[dict] = queue.Queue()

    def read_responses() -> None:
        for line in process.stdout:
            received.put(json.loads(line))

    threading.Thread(target=read_responses, daemon=True).start()

    def send(identifier: str | None, method: str, params: dict) -> None:
        message = {"jsonrpc": "2.0", "method": method, "params": params}
        if identifier is not None:
            message["id"] = identifier
        process.stdin.write(json.dumps(message) + "\n")
        process.stdin.flush()

    def receive() -> dict:
        return received.get(timeout=5)

    try:
        send("start", "lifecycle.start", {
            "protocol": "maintune.plugin.v2",
            "plugin_src": str(plugin_src),
            "entrypoint": "example",
            "context": {
                "plugin_id": "sample", "plugin_version": "1.0", "data_dir": str(tmp_path / "data"),
                "config": {"bridge_token": "VERY_PRIVATE_TEST_TOKEN"},
                "secret_fields": {"bridge_token": True},
            },
        })
        started = receive()
        assert started["id"] == "start"
        assert {item["name"] for item in started["result"]["registrations"]} == {"read", "leak", "wait"}

        for identifier in ("one", "two"):
            send(identifier, "extension.invoke", {
                "kind": "service", "name": "read", "invocation_id": identifier,
                "input": {"label": identifier},
            })
        requests = [receive(), receive()]
        assert all(item["method"] == "core.call" for item in requests)
        # Resolve in reverse order to prove responses are mapped by JSON-RPC ID.
        for item in reversed(requests):
            process.stdin.write(json.dumps({
                "jsonrpc": "2.0", "id": item["id"],
                "result": item["params"]["params"]["label"],
            }) + "\n")
            process.stdin.flush()
        results = [receive(), receive()]
        assert {item["id"]: item["result"] for item in results} == {"one": "one", "two": "two"}

        send("wait", "extension.invoke", {
            "kind": "tool", "name": "wait", "invocation_id": "fixed-invocation",
            "input": {"label": "ready"},
        })
        ready = receive()
        assert ready["method"] == "core.call" and ready["params"]["method"] == "test.ready"
        process.stdin.write(json.dumps({"jsonrpc": "2.0", "id": ready["id"], "result": True}) + "\n")
        process.stdin.flush()
        send(None, "extension.cancel", {"invocation_id": "fixed-invocation"})
        assert receive() == {
            "jsonrpc": "2.0", "id": "wait",
            "result": {"cancelled": True, "invocation_id": "fixed-invocation"},
        }

        send("leak", "extension.invoke", {"kind": "service", "name": "leak", "input": {}})
        failure = receive()
        assert failure["id"] == "leak" and failure["error"]["type"] == "RuntimeError"
        assert "VERY_PRIVATE_TEST_TOKEN" not in json.dumps(failure)

        send("stop", "lifecycle.stop", {})
        assert receive()["result"] == {"stopped": True}
        assert process.wait(timeout=5) == 0
        assert "VERY_PRIVATE_TEST_TOKEN" not in process.stderr.read()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
