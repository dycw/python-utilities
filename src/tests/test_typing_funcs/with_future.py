from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, override

if TYPE_CHECKING:
    from pathlib import Path
    from uuid import UUID

    import whenever
    from whenever import (
        Date,
        DateDelta,
        DateTimeDelta,
        PlainDateTime,
        Time,
        TimeDelta,
        ZonedDateTime,
    )

    from utilities.sentinel import Sentinel
    from utilities.whenever import DatePeriod, TimePeriod, ZonedDateTimePeriod


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
    date1: Date
    date2: whenever.Date


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureDateDelta:
    delta1: DateDelta
    delta2: whenever.DateDelta


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureDatePeriod:
    period: DatePeriod


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureDateTimeDelta:
    delta1: DateTimeDelta
    delta2: whenever.DateTimeDelta


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
class DataClassFuturePlainDateTime:
    date_time1: PlainDateTime
    date_time2: whenever.PlainDateTime


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureSentinel:
    sentinel: Sentinel


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureStr:
    str_: str


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureTime:
    time1: Time
    time2: whenever.Time


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureTimeDelta:
    delta1: TimeDelta
    delta2: whenever.TimeDelta


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureTimePeriod:
    period: TimePeriod


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureTimeDeltaNullable:
    delta1: TimeDelta | None = None
    delta2: whenever.TimeDelta | None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureTypeLiteral:
    truth: TrueOrFalseFutureTypeLit


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureTypeLiteralNullable:
    truth: TrueOrFalseFutureTypeLit | None = None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureUUID:
    uuid: UUID


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureZonedDateTime:
    date_time1: ZonedDateTime
    date_time2: whenever.ZonedDateTime


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassFutureZonedDateTimePeriod:
    period: ZonedDateTimePeriod


class TypedDictFutureIntFloat(TypedDict):
    int_: int
    float_: float


class TypedDictFutureIntFloatOptional(TypedDict):
    int_: int
    float_: NotRequired[float]
