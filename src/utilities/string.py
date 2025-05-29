from __future__ import annotations

from os import environ
from pathlib import Path
from string import Template
from typing import assert_never


def substitute_environ(path_or_text: Path | str, /) -> str:
    """Substitute the environment variables in a file."""
    match path_or_text:
        case Path() as path:
            with path.open() as fh:
                return substitute_environ(fh.read())
        case str() as text:
            return Template(text).substitute(environ)
        case _ as never:
            assert_never(never)


__all__ = ["substitute_environ"]
