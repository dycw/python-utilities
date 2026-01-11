from __future__ import annotations

from dataclasses import dataclass
from os import environ, name
from typing import Literal, override

from shellingham import ShellDetectionFailure, detect_shell

Shell = Literal["bash", "zsh", "fish"]


def get_shell() -> Shell:
    """Get the shell."""
    try:
        shell, _ = detect_shell()
    except ShellDetectionFailure:
        if name == "posix":
            shell = environ["SHELL"]
        if name == "nt":
            shell = environ["COMSPEC"]
        raise _GetShellOSError(name=name) from None
    if shell == "bash":
        return "bash"
    if shell == "zsh":
        return "zsh"
    if shell == "fish":
        return "fish"
    raise _GetShellUnsupportedError(shell=shell)


@dataclass(kw_only=True, slots=True)
class GetShellError(Exception):
    name: str

    @override
    def __str__(self) -> str:
        return f"Invalid OS; got {self.name!r}"


@dataclass(kw_only=True, slots=True)
class _GetShellUnsupportedError(Exception):
    shell: str

    @override
    def __str__(self) -> str:
        return f"Invalid shell; got {self.shell!r}"


@dataclass(kw_only=True, slots=True)
class _GetShellOSError(GetShellError):
    name: str

    @override
    def __str__(self) -> str:
        return f"Invalid OS; got {self.name!r}"


SHELL = get_shell()


__all__ = ["SHELL", "GetShellError", "get_shell"]
