"""Language-neutral JSON-RPC envelope names used by Plugin API v2."""

from typing import Any, Literal, TypedDict


PROTOCOL = "maintune.plugin.v2"
MAX_MESSAGE_BYTES = 1024 * 1024


class RpcRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    method: str
    params: dict[str, Any]


class RpcResponse(TypedDict, total=False):
    jsonrpc: Literal["2.0"]
    id: str
    result: Any
    error: dict[str, Any]
