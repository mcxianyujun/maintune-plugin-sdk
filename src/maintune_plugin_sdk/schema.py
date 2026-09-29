from __future__ import annotations

import inspect
import json
import types
from typing import Any, Literal, Union, get_args, get_origin, get_type_hints


class PluginRegistrationError(ValueError):
    pass


def _type_schema(annotation: Any) -> dict[str, Any]:
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Literal:
        values = list(args)
        if not values or len({type(value) for value in values}) != 1:
            raise PluginRegistrationError("Tool Literal annotations must use one primitive type")
        primitive = {str: "string", int: "integer", float: "number", bool: "boolean"}.get(type(values[0]))
        if not primitive:
            raise PluginRegistrationError("Unsupported Tool Literal annotation")
        return {"type": primitive, "enum": values}
    if annotation is list:
        raise PluginRegistrationError("Tool lists need an item annotation or an explicit schema")
    if origin is list:
        if len(args) != 1:
            raise PluginRegistrationError("Tool lists need an item annotation or an explicit schema")
        return {"type": "array", "items": _type_schema(args[0])}
    if origin is dict:
        if args and args[0] is not str:
            raise PluginRegistrationError("Tool dictionary keys must be strings")
        return {"type": "object", "additionalProperties": True}
    if origin in (Union, types.UnionType):
        raise PluginRegistrationError("Tool unions need an explicit schema; optional values need a default of None")
    primitive = {str: "string", int: "integer", float: "number", bool: "boolean"}.get(annotation)
    if primitive:
        return {"type": primitive}
    if annotation is Any or annotation is inspect.Signature.empty:
        raise PluginRegistrationError("Every Tool parameter needs a supported type annotation")
    raise PluginRegistrationError(f"Unsupported Tool parameter annotation: {annotation!r}")


def schema_for_callable(function: Any) -> dict[str, Any]:
    """Build the deliberately small JSON Schema object supported by Maintune."""
    try:
        signature = inspect.signature(function)
        hints = get_type_hints(function)
    except (TypeError, ValueError, NameError) as error:
        raise PluginRegistrationError("Tool signature could not be inspected") from error
    parameters = list(signature.parameters.values())
    if parameters and parameters[0].name in {"context", "ctx"}:
        parameters = parameters[1:]
    properties: dict[str, Any] = {}
    required: list[str] = []
    for parameter in parameters:
        if parameter.kind not in (parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY):
            raise PluginRegistrationError("Tool parameters must be named keyword arguments")
        annotation = hints.get(parameter.name, parameter.annotation)
        origin = get_origin(annotation)
        args = get_args(annotation)
        optional = origin in (Union, types.UnionType) and len(args) == 2 and type(None) in args
        if optional:
            if parameter.default is not None:
                raise PluginRegistrationError("Optional Tool parameters must default to None or use an explicit schema")
            annotation = next(item for item in args if item is not type(None))
        schema = _type_schema(annotation)
        if parameter.default is not inspect.Signature.empty:
            if parameter.default is not None:
                try:
                    json.dumps(parameter.default, allow_nan=False)
                except (TypeError, ValueError) as error:
                    raise PluginRegistrationError("Tool default must be JSON serializable") from error
                schema["default"] = parameter.default
        else:
            required.append(parameter.name)
        properties[parameter.name] = schema
    return {"type": "object", "properties": properties, "required": required, "additionalProperties": False}
