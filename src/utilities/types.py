from __future__ import annotations

import datetime as dt
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from enum import Enum
from logging import Logger
from pathlib import Path
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

_T = TypeVar("_T")
_TEnum = TypeVar("_TEnum", bound=Enum)
_T_contra = TypeVar("_T_contra", contravariant=True)


# basic
Number: TypeAlias = int | float
Duration: TypeAlias = Number | dt.timedelta
StrMapping: TypeAlias = Mapping[str, Any]
TupleOrStrMapping: TypeAlias = tuple[Any, ...] | StrMapping


# asyncio
Coroutine1: TypeAlias = Coroutine[Any, Any, _T]
MaybeAwaitable: TypeAlias = _T | Awaitable[_T]
MaybeCoroutine1: TypeAlias = _T | Coroutine1[_T]


# enum
EnumOrStr: TypeAlias = _TEnum | str


# logging
LogLevel: TypeAlias = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LoggerOrName: TypeAlias = Logger | str


# pathlib
PathLike: TypeAlias = Path | str
PathLikeOrCallable: TypeAlias = PathLike | Callable[[], PathLike]


# traceback
ExcInfo: TypeAlias = tuple[type[BaseException], BaseException, TracebackType]
OptExcInfo: TypeAlias = ExcInfo | tuple[None, None, None]


# dataclasses
@runtime_checkable
class Dataclass(Protocol):
    """Protocol for `dataclass` classes."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


# math


class SupportsDunderLT(Protocol[_T_contra]):
    def __lt__(self, other: _T_contra, /) -> bool: ...  # pragma: no cover


class SupportsDunderGT(Protocol[_T_contra]):
    def __gt__(self, other: _T_contra, /) -> bool: ...  # pragma: no cover


SupportsRichComparison = SupportsDunderLT[Any] | SupportsDunderGT[Any]


__all__ = [
    "Coroutine1",
    "Dataclass",
    "Duration",
    "EnumOrStr",
    "ExcInfo",
    "LogLevel",
    "LoggerOrName",
    "MaybeAwaitable",
    "MaybeCoroutine1",
    "Number",
    "OptExcInfo",
    "PathLike",
    "PathLikeOrCallable",
    "StrMapping",
    "SupportsDunderGT",
    "SupportsDunderLT",
    "SupportsRichComparison",
    "TupleOrStrMapping",
]
