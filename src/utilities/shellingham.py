from __future__ import annotations

from dataclasses import dataclass
from os import environ, name
from typing import override

import shellingham
from shellingham import ShellDetectionFailure


def detect_shell() -> str:
    try:
        (shell,) = shellingham.detect_shell()
    except ShellDetectionFailure:
        if name == "posix":
            return environ["SHELL"]
        if name == "nt":
            return environ["COMSPEC"]
        raise DetectShellError(name=name) from None
    else:
        return shell


@dataclass(kw_only=True, slots=True)
class DetectShellError(Exception):
    name: str

    @override
    def __str__(self) -> str:
        return f"Invalid OS; got {self.name!r}"


__all__ = ["DetectShellError", "detect_shell"]
