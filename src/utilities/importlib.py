from __future__ import annotations

from importlib import import_module
from importlib.util import find_spec


def is_valid_import(module: str, /, *, name: str | None = None) -> bool:
    """Check if an import is valid."""
    spec = find_spec(module)
    if spec is None:
        return False
    if name is None:
        return True
    mod = import_module(module)
    return hasattr(mod, name)


__all__ = ["is_valid_import"]
