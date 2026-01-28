from __future__ import annotations

from dataclasses import dataclass
from os import environ, name
from pathlib import Path
from typing import Literal, override

from rich.pretty import pretty_repr
from shellingham import ShellDetectionFailure, detect_shell

from utilities.core import OneEmptyError, one
from utilities.typing import get_args

type Shell = Literal["bash", "fish", "posix", "sh", "zsh"]


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
    matches: list[Shell] = [s for s in shells if _get_shell_match(shell, s)]
    try:
        return one(matches)
    except OneEmptyError:  # pragma: no cover
        raise _GetShellUnsupportedError(shell=shell) from None


def _get_shell_match(shell: str, candidate: Shell, /) -> bool:
    *_, name = Path(shell).parts
    return name == candidate


@dataclass(kw_only=True, slots=True)
class GetShellError(Exception):
    name: str


@dataclass(kw_only=True, slots=True)
class _GetShellUnsupportedError(Exception):
    shell: str

    @override
    def __str__(self) -> str:
        return f"Invalid shell; got {pretty_repr(self.shell)}"  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class _GetShellOSError(GetShellError):
    name: str

    @override
    def __str__(self) -> str:
        return f"Invalid OS; got {pretty_repr(self.name)}"  # pragma: no cover


SHELL = get_shell()


__all__ = ["SHELL", "GetShellError", "get_shell"]
