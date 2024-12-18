from __future__ import annotations

import datetime as dt
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from pathlib import Path
from typing import Any, ClassVar, Protocol, TypeAlias, TypeVar, runtime_checkable

_T = TypeVar("_T")


# basic
Number: TypeAlias = int | float
Duration: TypeAlias = Number | dt.timedelta
StrMapping: TypeAlias = Mapping[str, Any]
TupleOrStrMapping: TypeAlias = tuple[Any, ...] | StrMapping


# async
Coroutine1: TypeAlias = Coroutine[Any, Any, _T]
MaybeAwaitable: TypeAlias = _T | Awaitable[_T]
MaybeCoroutine1: TypeAlias = _T | Coroutine1[_T]


# pathlib
PathLike: TypeAlias = Path | str
PathLikeOrCallable: TypeAlias = PathLike | Callable[[], PathLike]


@runtime_checkable
class Dataclass(Protocol):
    """Protocol for `dataclass` classes."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


__all__ = [
    "Coroutine1",
    "Dataclass",
    "Duration",
    "MaybeAwaitable",
    "MaybeCoroutine1",
    "Number",
    "PathLike",
    "PathLikeOrCallable",
    "StrMapping",
    "TupleOrStrMapping",
]
