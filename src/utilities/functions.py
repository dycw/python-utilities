from __future__ import annotations

from functools import partial
from types import (
    BuiltinFunctionType,
    FunctionType,
    MethodDescriptorType,
    MethodType,
    MethodWrapperType,
    WrapperDescriptorType,
)
from typing import TYPE_CHECKING, Any, TypeVar, overload

if TYPE_CHECKING:
    from collections.abc import Callable


_T = TypeVar("_T")
_U = TypeVar("_U")


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


def if_not_none(x: _T | None, y: _U, /) -> _T | _U:
    """Return the first value if it is not None, else the second value."""
    return x if x is not None else y


def second(pair: tuple[Any, _U], /) -> _U:
    """Get the second element in a pair."""
    return pair[1]


__all__ = [
    "first",
    "get_class",
    "get_class_name",
    "get_func_name",
    "identity",
    "if_not_none",
    "second",
]
