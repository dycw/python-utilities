from __future__ import annotations

import datetime as dt
from collections.abc import Awaitable, Callable, Coroutine, Hashable, Iterable, Mapping
from enum import Enum
from logging import Logger
from pathlib import Path
from random import Random
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    Literal,
    Protocol,
    TypeAlias,
    TypeVar,
    runtime_checkable,
)
from zoneinfo import ZoneInfo

_T = TypeVar("_T")
_TEnum = TypeVar("_TEnum", bound=Enum)
_THashable = TypeVar("_THashable", bound=Hashable)
_T_contra = TypeVar("_T_contra", contravariant=True)


# basic
Number: TypeAlias = int | float
Duration: TypeAlias = Number | dt.timedelta
StrMapping: TypeAlias = Mapping[str, Any]
TupleOrStrMapping: TypeAlias = tuple[Any, ...] | StrMapping
MaybeType: TypeAlias = _T | type[_T]


# asyncio
Coroutine1: TypeAlias = Coroutine[Any, Any, _T]
MaybeAwaitable: TypeAlias = _T | Awaitable[_T]
MaybeCoroutine1: TypeAlias = _T | Coroutine1[_T]


# concurrent
Parallelism: TypeAlias = Literal["processes", "threads"]


# dataclasses
@runtime_checkable
class Dataclass(Protocol):
    """Protocol for `dataclass` classes."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


# datetime
DateOrDateTime: TypeAlias = dt.date | dt.datetime


# enum
EnumOrStr: TypeAlias = _TEnum | str


# iterables
MaybeIterable: TypeAlias = _T | Iterable[_T]
IterableHashable: TypeAlias = tuple[_THashable, ...] | frozenset[_THashable]
MaybeIterableHashable: TypeAlias = _THashable | IterableHashable[_THashable]


# logging
LogLevel: TypeAlias = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LoggerOrName: TypeAlias = Logger | str


# operator


class SupportsDunderLT(Protocol[_T_contra]):
    def __lt__(self, other: _T_contra, /) -> bool: ...  # pragma: no cover


class SupportsDunderGT(Protocol[_T_contra]):
    def __gt__(self, other: _T_contra, /) -> bool: ...  # pragma: no cover


SupportsRichComparison = SupportsDunderLT[Any] | SupportsDunderGT[Any]


# pathlib
PathLike: TypeAlias = Path | str
PathLikeOrCallable: TypeAlias = PathLike | Callable[[], PathLike]


# random
Seed: TypeAlias = int | float | str | bytes | bytearray | Random


# traceback
ExcInfo: TypeAlias = tuple[type[BaseException], BaseException, TracebackType]
OptExcInfo: TypeAlias = ExcInfo | tuple[None, None, None]


# zoneinfo
ZoneInfoLike: TypeAlias = ZoneInfo | str
LocalOrZoneInfoLike: TypeAlias = Literal["local"] | ZoneInfoLike


__all__ = [
    "Coroutine1",
    "Dataclass",
    "DateOrDateTime",
    "Duration",
    "EnumOrStr",
    "ExcInfo",
    "IterableHashable",
    "LocalOrZoneInfoLike",
    "LogLevel",
    "LoggerOrName",
    "MaybeAwaitable",
    "MaybeCoroutine1",
    "MaybeIterable",
    "MaybeIterableHashable",
    "MaybeType",
    "Number",
    "OptExcInfo",
    "Parallelism",
    "PathLike",
    "PathLikeOrCallable",
    "Seed",
    "StrMapping",
    "SupportsDunderGT",
    "SupportsDunderLT",
    "SupportsRichComparison",
    "TupleOrStrMapping",
    "ZoneInfoLike",
]
