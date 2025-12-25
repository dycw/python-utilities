from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import override


def which(cmd: str, /) -> Path:
    path = shutil.which(cmd)
    if path is None:
        raise WhichError(cmd=cmd)
    return Path(path)


@dataclass(kw_only=True, slots=True)
class WhichError(Exception):
    cmd: str

    @override
    def __str__(self) -> str:
        return f"{self.cmd!r} not found"


__all__ = ["WhichError", "which"]
