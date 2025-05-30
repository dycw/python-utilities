from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, override

if TYPE_CHECKING:
    import datetime as dt
    from pathlib import Path
    from uuid import UUID

    from utilities.sentinel import Sentinel


TrueOrFalseFutureLit = Literal["true", "false"]
type TrueOrFalseFutureTypeLit = Literal["true", "false"]


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
class DataClassFutureDefaultInInitParent:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureDefaultInInitChild(DataClassFutureDefaultInInitParent):
    def __init__(self) -> None:
        DataClassFutureDefaultInInitParent.__init__(self, int_=0)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureInt:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureIntDefault:
    int_: int = 0


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureIntEven:
    even_int: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureIntOdd:
    odd_int: int


DataClassFutureIntEvenOrOddUnion = DataClassFutureIntEven | DataClassFutureIntOdd
type DataClassFutureIntEvenOrOddTypeUnion = (
    DataClassFutureIntEven | DataClassFutureIntOdd
)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureIntNullable:
    int_: int | None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureIntLowerAndUpper:
    int_: int
    INT_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureIntOneAndTwo:
    int1: int
    int2: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureListInts:
    ints: list[int]


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureListIntsDefault:
    ints: list[int] = field(default_factory=list)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureLiteral:
    truth: TrueOrFalseFutureLit


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureLiteralNullable:
    truth: TrueOrFalseFutureLit | None = None


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
class DataClassFutureTimeDelta:
    timedelta: dt.timedelta


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureTimeDeltaNullable:
    timedelta: dt.timedelta | None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureTypeLiteral:
    truth: TrueOrFalseFutureTypeLit


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureTypeLiteralNullable:
    truth: TrueOrFalseFutureTypeLit | None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureUUID:
    uuid: UUID
