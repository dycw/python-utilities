from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, override

if TYPE_CHECKING:
    import datetime as dt
    from pathlib import Path
    from typing import Literal
    from uuid import UUID

    from utilities.sentinel import Sentinel


@dataclass(order=True, kw_only=True)
class DataClassFutureCustomEquality:
    int_: int = 0

    @override
    def __eq__(self, other: object) -> bool:
        return self is other

    @override
    def __hash__(self) -> int:
        return id(self)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureDate:
    date: dt.date


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureInt:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureDefaultInInitParent:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureDefaultInInitChild(DataClassFutureDefaultInInitParent):
    def __init__(self) -> None:
        DataClassFutureDefaultInInitParent.__init__(self, int_=0)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureIntDefault:
    int_: int = 0


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithIntNullable:
    int_: int | None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureListInts:
    ints: list[int]


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureListIntsDefault:
    ints: list[int] = field(default_factory=list)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithLiteral:
    truth: Literal["true", "false"]


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithLiteralNullable:
    truth: Literal["true", "false"] | None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureNestedInnerFirstInner:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureNestedInnerFirstOuter:
    inner: DataClassFutureNestedInnerFirstInner


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureNestedOuterFirstOuter:
    inner: DataClassFutureNestedOuterFirstInner


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureNestedOuterFirstInner:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureNone:
    none: None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureNoneDefault:
    none: None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFuturePath:
    path: Path


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureSentinel:
    sentinel: Sentinel


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureStr:
    str_: str


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassWithTimeDelta:
    timedelta: dt.timedelta


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureUUID:
    uuid: UUID
