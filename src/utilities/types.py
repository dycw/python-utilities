from __future__ import annotations

import datetime as dt
from collections.abc import Awaitable, Callable, Coroutine, Hashable, Iterable, Mapping
from enum import Enum
from logging import Logger
from pathlib import Path
from random import Random
from types import TracebackType
from typing import Any, ClassVar, Literal, Protocol, TypeVar, runtime_checkable
from zoneinfo import ZoneInfo

_T_contra = TypeVar("_T_contra", contravariant=True)


# basic
type Number = int | float
type Duration = Number | dt.timedelta
type StrMapping = Mapping[str, Any]
type TupleOrStrMapping = tuple[Any, ...] | StrMapping
type MaybeType[_T] = _T | type[_T]


# asyncio
type Coroutine1[_T] = Coroutine[Any, Any, _T]
type MaybeAwaitable[_T] = _T | Awaitable[_T]
type MaybeCoroutine1[_T] = _T | Coroutine1[_T]


# concurrent
type Parallelism = Literal["processes", "threads"]


# dataclasses
@runtime_checkable
class Dataclass(Protocol):
    """Protocol for `dataclass` classes."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


# datetime
type DateOrDateTime = dt.date | dt.datetime


# enum
type EnumOrStr[_TEnum: Enum] = _TEnum | str


# iterables
type MaybeIterable[_T] = _T | Iterable[_T]
type IterableHashable[_THashable: Hashable] = (
    tuple[_THashable, ...] | frozenset[_THashable]
)
type MaybeIterableHashable[_THashable: Hashable] = (
    _THashable | IterableHashable[_THashable]
)


# logging
type LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
type LoggerOrName = Logger | str


# operator


class SupportsDunderLT(Protocol[_T_contra]):
    def __lt__(self, other: _T_contra, /) -> bool: ...  # pragma: no cover


class SupportsDunderGT(Protocol[_T_contra]):
    def __gt__(self, other: _T_contra, /) -> bool: ...  # pragma: no cover


SupportsRichComparison = SupportsDunderLT[Any] | SupportsDunderGT[Any]


# pathlib
type PathLike = Path | str
type PathLikeOrCallable = PathLike | Callable[[], PathLike]


# random
type Seed = int | float | str | bytes | bytearray | Random


# traceback
type ExcInfo = tuple[type[BaseException], BaseException, TracebackType]
type OptExcInfo = ExcInfo | tuple[None, None, None]


# zoneinfo
type ZoneInfoLike = ZoneInfo | str
type LocalOrZoneInfoLike = Literal["local"] | ZoneInfoLike


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
