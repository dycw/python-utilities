from __future__ import annotations

from os import environ
from pathlib import Path
from string import Template
from typing import Any, assert_never


def substitute_environ(path_or_text: Path | str, /, **kwargs: Any) -> str:
    """Substitute the environment variables in a file."""
    match path_or_text:
        case Path() as path:
            return substitute_environ(path.read_text(), **kwargs)
        case str() as text:
            return Template(text).substitute(environ, **kwargs)
        case _ as never:
            assert_never(never)


__all__ = ["substitute_environ"]
