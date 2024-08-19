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


__all__ = ["valid_path"]
