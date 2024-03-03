from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pathvalidate import ValidationError, sanitize_filepath, validate_filepath

if TYPE_CHECKING:
    from utilities.types import PathLike


def valid_path(*parts: PathLike, sanitize: bool = False) -> Path:
    """Build & validate a path; sanitize if necessary."""
    path = Path(*parts)
    try:
        validate_filepath(path, platform="auto")
    except ValidationError:
        if sanitize:
            return sanitize_filepath(path, platform="auto")
        raise
    return path


def valid_path_cwd(*parts: PathLike, sanitize: bool = False) -> Path:
    """Build & validate a path from the current working directory."""
    return valid_path(Path.cwd(), *parts, sanitize=sanitize)


def valid_path_home(*parts: PathLike, sanitize: bool = False) -> Path:
    """Build & validate a path from home."""
    return valid_path(Path.home(), *parts, sanitize=sanitize)


__all__ = ["valid_path", "valid_path_cwd", "valid_path_home"]
