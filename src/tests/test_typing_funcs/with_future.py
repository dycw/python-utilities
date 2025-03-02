from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime as dt
    from pathlib import Path
    from typing import Literal
    from uuid import UUID

    from utilities.sentinel import Sentinel


@dataclass(kw_only=True, slots=True)
class DataClassNestedWithFutureInnerThenOuterInner:
    int_: int


@dataclass(kw_only=True, slots=True)
class DataClassNestedWithFutureInnerThenOuterOuter:
    inner: DataClassNestedWithFutureInnerThenOuterInner


@dataclass(kw_only=True, slots=True)
class DataClassNestedWithFutureOuterThenInnerOuter:
    inner: DataClassNestedWithFutureOuterThenInnerInner


@dataclass(kw_only=True, slots=True)
class DataClassNestedWithFutureOuterThenInnerInner:
    int_: int


@dataclass(kw_only=True, slots=True)
class DataClassWithDate:
    date: dt.date


@dataclass(kw_only=True, slots=True)
class DataClassWithInt:
    int_: int


@dataclass(kw_only=True, slots=True)
class DataClassWithIntNullable:
    int_: int | None = None


@dataclass(kw_only=True, slots=True)
class DataClassWithListInts:
    ints: list[int]


@dataclass(kw_only=True, slots=True)
class DataClassWithLiteral:
    truth: Literal["true", "false"]


@dataclass(kw_only=True, slots=True)
class DataClassWithLiteralNullable:
    truth: Literal["true", "false"] | None = None


@dataclass(kw_only=True, slots=True)
class DataClassWithNone:
    none: None


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClassWithPath:
    path: Path


@dataclass(kw_only=True, slots=True)
class DataClassWithSentinel:
    sentinel: Sentinel


@dataclass(kw_only=True, slots=True)
class DataClassWithStr:
    str_: str


@dataclass(kw_only=True, slots=True)
class DataClassWithTimeDelta:
    timedelta: dt.timedelta


@dataclass(kw_only=True, slots=True)
class DataClassWithUUID:
    uuid: UUID
