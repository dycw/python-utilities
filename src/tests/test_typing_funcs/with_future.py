from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import datetime as dt
    from pathlib import Path


@dataclass(kw_only=True, slots=True)
class DataClassWithLiteral:
    truth: Literal["true", "false"]


@dataclass(kw_only=True, slots=True)
class DataClassWithLiteralNullable:
    truth: Literal["true", "false"] | None = None


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClassWithPath:
    path: Path


@dataclass(kw_only=True, slots=True)
class DataClassWithTimeDelta:
    timedelta: dt.timedelta
