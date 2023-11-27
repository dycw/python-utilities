from __future__ import annotations

from pathlib import Path

from pathvalidate import ValidationError, sanitize_filepath, validate_filepath

from utilities.pathlib import PathLike


def ensure_path(path: PathLike, /, *, sanitize: bool = False) -> Path:
    """Validate a path, and if allowed, sanitize upon error."""
    path = Path(path)
    try:
        validate_filepath(path, platform="auto")
    except ValidationError:
        if sanitize:
            return sanitize_filepath(path)
        raise
    return path


__all__ = ["ensure_path"]
