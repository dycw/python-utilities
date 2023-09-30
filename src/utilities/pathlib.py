from __future__ import annotations

from pathlib import Path

from utilities.re import extract_group

PathLike = Path | str


def ensure_suffix(path: PathLike, suffix: str, /) -> Path:
    """Ensure a path has the required suffix."""
    as_path = Path(path)
    parts = as_path.name.split(".")
    clean_suffix = extract_group(r"^\.(\w+)$", suffix)
    if parts[-1] != clean_suffix:
        parts.append(clean_suffix)
    return as_path.with_name(".".join(parts))
