from __future__ import annotations

from dataclasses import dataclass
from functools import partial, wraps
from types import (
    BuiltinFunctionType,
    FunctionType,
    MethodDescriptorType,
    MethodType,
    MethodWrapperType,
    WrapperDescriptorType,
)
from typing import TYPE_CHECKING, Any, TypeVar, overload

from typing_extensions import ParamSpec, override

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

_P = ParamSpec("_P")
_T = TypeVar("_T")
_U = TypeVar("_U")
_V = TypeVar("_V")


def ensure_not_none(obj: _T | None, /) -> _T:
    """Ensure an object is not None."""
    if obj is None:
        raise EnsureNotNoneError(obj=obj)
    return obj


@dataclass(kw_only=True)
class EnsureNotNoneError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must not be None"


def first(pair: tuple[_T, Any], /) -> _T:
    """Get the first element in a pair."""
    return pair[0]


@overload
def get_class(obj: type[_T], /) -> type[_T]: ...
@overload
def get_class(obj: _T, /) -> type[_T]: ...
def get_class(obj: _T | type[_T], /) -> type[_T]:
    """Get the class of an object, unless it is already a class."""
    return obj if isinstance(obj, type) else type(obj)


def get_class_name(obj: Any, /) -> str:
    """Get the name of the class of an object, unless it is already a class."""
    return get_class(obj).__name__


def get_func_name(obj: Callable[..., Any], /) -> str:
    """Get the name of a callable."""
    if isinstance(obj, BuiltinFunctionType | FunctionType | MethodType):
        return obj.__name__
    if isinstance(
        obj, MethodDescriptorType | MethodWrapperType | WrapperDescriptorType
    ):
        return obj.__qualname__
    if isinstance(obj, partial):
        return get_func_name(obj.func)
    return get_class_name(obj)


def identity(obj: _T, /) -> _T:
    """Return the object itself."""
    return obj


def is_none(obj: Any, /) -> bool:
    """Check if an object is `None`."""
    return obj is None


def is_not_none(obj: Any, /) -> bool:
    """Check if an object is not `None`."""
    return obj is not None


def not_func(func: Callable[_P, bool], /) -> Callable[_P, bool]:
    """Lift a boolean-valued function to return its conjugation."""

    @wraps(func)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> bool:
        return not func(*args, **kwargs)

    return wrapped


def second(pair: tuple[Any, _U], /) -> _U:
    """Get the second element in a pair."""
    return pair[1]


def send_and_next(value: _U, generator: Generator[_T | None, _U | None, Any], /) -> _T:
    """Send a value to a generator, and then yield the output."""
    result = ensure_not_none(generator.send(value))
    _ = next(generator)
    return result


def start_generator_coroutine(
    func: Callable[_P, Generator[_T, _U, _V]], /
) -> Callable[_P, Generator[_T, _U, _V]]:
    """Instantiate and then start a generator-coroutine."""

    @wraps(func)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> Generator[_T, _U, _V]:
        coro = func(*args, **kwargs)
        _ = next(coro)
        return coro

    return wrapped


__all__ = [
    "EnsureNotNoneError",
    "ensure_not_none",
    "first",
    "get_class",
    "get_class_name",
    "get_func_name",
    "identity",
    "is_none",
    "is_not_none",
    "not_func",
    "second",
    "send_and_next",
    "start_generator_coroutine",
]
