from __future__ import annotations

from contextlib import suppress
from subprocess import CalledProcessError, check_call
from typing import TYPE_CHECKING

from astor import to_source

from utilities.tempfile import TemporaryDirectory

if TYPE_CHECKING:
    from ast import Module


def module_to_source(module: Module, /) -> str:
    """Write a module as a `ruff`-formatted string."""
    src = to_source(module)
    with TemporaryDirectory() as temp:
        path = temp.joinpath("temp.py")
        with path.open(mode="w") as fh:
            _ = fh.write(src)
        with suppress(CalledProcessError, FileNotFoundError):
            _ = check_call(["ruff", "format", str(path)])
        with path.open() as fh:
            return fh.read()


__all__ = ["module_to_source"]
