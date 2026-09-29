from __future__ import annotations

import asyncio
import copy
import inspect
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from .schema import PluginRegistrationError, schema_for_callable

_NAME = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")


@dataclass(frozen=True)
class PluginContext:
    plugin_id: str
    plugin_version: str
    data_dir: Path
    config: Mapping[str, Any] = field(repr=False)
    invocation_id: str = ""
    _core_call: Callable[[str, dict[str, Any]], Any] | None = field(default=None, repr=False, compare=False)
    _cancelled: asyncio.Event = field(default_factory=asyncio.Event, repr=False, compare=False)

    async def call_core(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Call a documented Maintune API method; never exposes host credentials."""
        if self._core_call is None:
            raise RuntimeError("Core API is unavailable in this context")
        result = self._core_call(method, params or {})
        if inspect.isawaitable(result):
            return await result
        return result

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    async def wait_cancelled(self) -> None:
        """Wait for Core's advisory cancellation notification."""
        await self._cancelled.wait()

    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger("maintune.plugin." + self.plugin_id)


@dataclass
class _Registration:
    kind: str
    name: str
    handler: Callable[..., Any]
    metadata: dict[str, Any]
    wants_context: bool


def _wants_context(handler: Callable[..., Any]) -> bool:
    try:
        parameters = tuple(inspect.signature(handler).parameters.values())
    except (TypeError, ValueError) as error:
        raise PluginRegistrationError("Extension handler signature could not be inspected") from error
    return bool(parameters and parameters[0].name in {"context", "ctx"})


class PluginAPI:
    """Collect runtime registrations and dispatch Core invocations to handlers."""

    def __init__(self):
        self._registrations: dict[tuple[str, str], _Registration] = {}

    def _add(self, kind: str, name: str, handler: Callable[..., Any], **metadata: Any) -> None:
        if not _NAME.fullmatch(name) or not callable(handler):
            raise PluginRegistrationError("Invalid extension name or handler")
        key = kind, name
        if key in self._registrations:
            raise PluginRegistrationError(f"Duplicate {kind} registration: {name}")
        self._registrations[key] = _Registration(kind, name, handler, copy.deepcopy(metadata), _wants_context(handler))

    def register_hook(self, name: str, handler: Callable[..., Any], *, priority: int = 100) -> None:
        if not -1000 <= priority <= 1000:
            raise PluginRegistrationError("Hook priority must be between -1000 and 1000")
        self._add("hook", name, handler, priority=priority)

    def on_task_finally(self, handler: Callable[..., Any]) -> None:
        self.register_hook("task.finally", handler)

    def register_tool(
        self,
        name: str,
        handler: Callable[..., Any],
        *,
        description: str,
        input_schema: dict[str, Any] | None = None,
        recommended_agents: list[str] | tuple[str, ...] = (),
    ) -> None:
        if not description.strip() or len(description) > 1000:
            raise PluginRegistrationError("Tool description must contain 1 to 1000 characters")
        schema = schema_for_callable(handler) if input_schema is None else input_schema
        if not isinstance(schema, dict) or schema.get("type") != "object":
            raise PluginRegistrationError("Tool input schema must be an object")
        self._add(
            "tool", name, handler,
            description=description,
            input_schema=schema,
            recommended_agents=list(dict.fromkeys(recommended_agents)),
        )

    def register_service(
        self,
        name: str,
        handler: Callable[..., Any],
        *,
        description: str = "",
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        version: str | None = None,
    ) -> None:
        self._add(
            "service", name, handler,
            description=description[:1000], input_schema=input_schema, output_schema=output_schema, version=version,
        )

    def register_provider(self, kind: str, name: str, handler: Callable[..., Any], *, config_schema: dict[str, Any], models: list[str] | None = None) -> None:
        if kind not in {"model", "sandbox"}:
            raise PluginRegistrationError("Provider kind must be 'model' or 'sandbox'")
        self._add("provider", name, handler, provider_kind=kind, config_schema=config_schema, models=models or [])

    def register_route(self, name: str, handler: Callable[..., Any], *, methods: tuple[str, ...] = ("GET",), access: str = "authenticated") -> None:
        """Expose a static path under this plugin's /api/plugins/<id>/ namespace."""
        normalized = [method.upper() for method in methods]
        if not normalized or any(method not in {"GET", "POST", "PUT", "PATCH", "DELETE"} for method in normalized):
            raise PluginRegistrationError("Unsupported plugin HTTP method")
        if access not in {"authenticated", "external"}:
            raise PluginRegistrationError("Plugin route access must be authenticated or external")
        self._add("route", name, handler, methods=list(dict.fromkeys(normalized)), access=access)

    def register_model_provider(self, name: str, handler: Callable[..., Any], *, config_schema: dict[str, Any], models: list[str]) -> None:
        self.register_provider("model", name, handler, config_schema=config_schema, models=models)

    def register_sandbox_provider(self, name: str, handler: Callable[..., Any], *, config_schema: dict[str, Any]) -> None:
        self.register_provider("sandbox", name, handler, config_schema=config_schema)

    def registrations(self) -> list[dict[str, Any]]:
        return [
            {"kind": item.kind, "name": item.name, **copy.deepcopy(item.metadata)}
            for item in self._registrations.values()
        ]

    async def invoke(self, kind: str, name: str, context: PluginContext, payload: dict[str, Any]) -> Any:
        try:
            registration = self._registrations[(kind, name)]
        except KeyError as error:
            raise PluginRegistrationError("Extension handler is not registered") from error
        if kind == "tool":
            result = registration.handler(context, **payload) if registration.wants_context else registration.handler(**payload)
        else:
            result = registration.handler(context, payload) if registration.wants_context else registration.handler(payload)
        return await result if inspect.isawaitable(result) else result
