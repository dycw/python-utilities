from __future__ import annotations

from dataclasses import dataclass
from os import environ, name
from re import search
from typing import Literal, override

from shellingham import ShellDetectionFailure, detect_shell

from utilities.iterables import OneEmptyError, one
from utilities.typing import get_args

type Shell = Literal["bash", "fish", "posix", "zsh"]


def get_shell() -> Shell:
    """Get the shell."""
    try:
        shell, _ = detect_shell()
    except ShellDetectionFailure:  # pragma: no cover
        if name == "posix":
            shell = environ["SHELL"]
        elif name == "nt":
            shell = environ["COMSPEC"]
        else:
            raise _GetShellOSError(name=name) from None
    shells: tuple[Shell, ...] = get_args(Shell)
    matches: dict[Shell, bool] = {s: search(shell, s) is not None for s in shells}
    try:
        return one(k for k, v in matches.items() if v is not None)
    except OneEmptyError:  # pragma: no cover
        raise _GetShellUnsupportedError(shell=shell) from None


@dataclass(kw_only=True, slots=True)
class GetShellError(Exception):
    name: str


@dataclass(kw_only=True, slots=True)
class _GetShellUnsupportedError(Exception):
    shell: str

    @override
    def __str__(self) -> str:
        return f"Invalid shell; got {self.shell!r}"  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class _GetShellOSError(GetShellError):
    name: str

    @override
    def __str__(self) -> str:
        return f"Invalid OS; got {self.name!r}"  # pragma: no cover


SHELL = get_shell()


__all__ = ["SHELL", "GetShellError", "get_shell"]
