from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from memory_profiler import memory_usage
from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from collections.abc import Callable

_P = ParamSpec("_P")
_T = TypeVar("_T")


@dataclass(frozen=True)
class Output(Generic[_T]):
    """A function output, and its memory usage."""

    value: _T
    memory: float


def memory_profiled(func: Callable[_P, _T], /) -> Callable[_P, Output[_T]]:
    """Call a function, but also profile its maximum memory usage."""

    @wraps(func)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> Output[_T]:
        memory, value = memory_usage(
            cast(Any, (func, args, kwargs)), max_usage=True, retval=True
        )
        return Output(value=value, memory=memory)

    return wrapped


__all__ = ["Output", "memory_profiled"]
