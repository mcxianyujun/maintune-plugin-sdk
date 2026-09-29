"""Public, Core-independent SDK for Maintune Plugin API v2."""

from .api import PluginAPI, PluginContext, PluginRegistrationError
from .schema import schema_for_callable

__all__ = ["PluginAPI", "PluginContext", "PluginRegistrationError", "schema_for_callable"]
__version__ = "2.0.0"
