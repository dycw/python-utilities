from pathlib import Path
from typing import Union

from beartype import beartype

from utilities.re import extract_group


PathLike = Union[Path, str]


@beartype
def ensure_suffix(path: PathLike, suffix: str, /) -> Path:
    """Ensure a path has the required suffix."""

    as_path = Path(path)
    parts = as_path.name.split(".")
    clean_suffix = extract_group(r"^\.(\w+)$", suffix)
    if parts[-1] != clean_suffix:
        parts.append(clean_suffix)
    return as_path.with_name(".".join(parts))
