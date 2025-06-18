from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, cast

from memory_profiler import memory_usage

if TYPE_CHECKING:
    from collections.abc import Callable


def memory_profiled[**P, T](func: Callable[P, T], /) -> Callable[P, Output[T]]:
    """Call a function, but also profile its maximum memory usage."""

    @wraps(func)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> Output[T]:
        memory, value = memory_usage(
            cast("Any", (func, args, kwargs)), max_usage=True, retval=True
        )
        return Output(value=value, memory=memory)

    return wrapped


@dataclass(kw_only=True, slots=True)
class Output[T]:
    """A function output, and its memory usage."""

    value: T
    memory: float


__all__ = ["Output", "memory_profiled"]
