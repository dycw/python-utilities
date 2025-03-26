from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, override

if TYPE_CHECKING:
    import datetime as dt
    from pathlib import Path
    from typing import Literal
    from uuid import UUID

    from utilities.sentinel import Sentinel


@dataclass(kw_only=True, slots=True)
class DataClassDefaultInInitParent:
    int_: int


@dataclass(kw_only=True, slots=True)
class DataClassDefaultInInitChild(DataClassDefaultInInitParent):
    def __init__(self) -> None:
        DataClassDefaultInInitParent.__init__(self, int_=0)


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
class DataClassWithCustomEquality:
    int_: int = 0

    @override
    def __eq__(self, other: object) -> bool:
        return self is other

    @override
    def __hash__(self) -> int:
        return id(self)


@dataclass(kw_only=True, slots=True)
class DataClassWithInt:
    int_: int


@dataclass(kw_only=True, slots=True)
class DataClassWithIntDefault:
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
