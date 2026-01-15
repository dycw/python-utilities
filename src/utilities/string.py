from __future__ import annotations

import os
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Any, assert_never

if TYPE_CHECKING:
    from utilities.types import PathLike, StrDict


def substitute(
    path_or_text: PathLike, /, *, environ: bool = False, **kwargs: Any
) -> str:
    """Substitute from a Path or string."""
    match path_or_text:
        case Path() as path:
            return substitute(path.read_text(), environ=environ, **kwargs)
        case str() as text:
            kwargs_use: StrDict = (os.environ if environ else {}) | kwargs
            return Template(text).substitute(**kwargs_use)
        case never:
            assert_never(never)


__all__ = ["substitute"]
