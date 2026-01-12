from __future__ import annotations

from dataclasses import dataclass
from os import environ, name
from typing import Literal, override

from shellingham import ShellDetectionFailure, detect_shell

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
    shells: list[Shell] = list(get_args(Shell))
    if shell in shells:
        return shell
    raise _GetShellUnsupportedError(shell=shell)  # pragma: no cover


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
