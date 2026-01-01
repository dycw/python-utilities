from __future__ import annotations

import importlib.resources
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING

from utilities.errors import ImpossibleCaseError

if TYPE_CHECKING:
    from importlib.resources import Anchor


def files(*, anchor: Anchor | None = None) -> Path:
    """Get the path for an anchor."""
    path = importlib.resources.files(anchor)
    if isinstance(path, Path):
        return path
    raise ImpossibleCaseError(case=[f"{path=}"])  # pragma: no cover


def is_valid_import(module: str, /, *, name: str | None = None) -> bool:
    """Check if an import is valid."""
    spec = find_spec(module)
    if spec is None:
        return False
    if name is None:
        return True
    mod = import_module(module)
    return hasattr(mod, name)


__all__ = ["files", "is_valid_import"]
