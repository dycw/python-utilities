from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, override

if TYPE_CHECKING:
    import datetime as dt
    from pathlib import Path
    from typing import Literal
    from uuid import UUID

    from utilities.sentinel import Sentinel


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassDefaultInInitParent:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassDefaultInInitChild(DataClassDefaultInInitParent):
    def __init__(self) -> None:
        DataClassDefaultInInitParent.__init__(self, int_=0)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassNestedWithFutureInnerThenOuterInner:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassNestedWithFutureInnerThenOuterOuter:
    inner: DataClassNestedWithFutureInnerThenOuterInner


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassNestedWithFutureOuterThenInnerOuter:
    inner: DataClassNestedWithFutureOuterThenInnerInner


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassNestedWithFutureOuterThenInnerInner:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithDate:
    date: dt.date


@dataclass(order=True, kw_only=True)
class DataClassWithCustomEquality:
    int_: int = 0

    @override
    def __eq__(self, other: object) -> bool:
        return self is other

    @override
    def __hash__(self) -> int:
        return id(self)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithInt:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithIntDefault:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithIntNullable:
    int_: int | None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithListInts:
    ints: list[int]


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithLiteral:
    truth: Literal["true", "false"]


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithLiteralNullable:
    truth: Literal["true", "false"] | None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithNone:
    none: None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithPath:
    path: Path


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithSentinel:
    sentinel: Sentinel


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithStr:
    str_: str


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithTimeDelta:
    timedelta: dt.timedelta


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithUUID:
    uuid: UUID
