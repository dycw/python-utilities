from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, dataclass_transform, overload

from pyrsistent import PRecord as _PRecord
from pyrsistent import field as _field
from pyrsistent._field_common import (
    PFIELD_NO_FACTORY,
    PFIELD_NO_INITIAL,
    PFIELD_NO_INVARIANT,
    PFIELD_NO_SERIALIZER,
    PFIELD_NO_TYPE,
)

if TYPE_CHECKING:
    from collections.abc import Callable


_T = TypeVar("_T")
_U = TypeVar("_U")


@overload
def field(
    *,
    type: type[_T],
    invariant: Callable[[Any], tuple[bool, Any]] = ...,
    default: Any = ...,
    mandatory: bool = ...,
    factory: Callable[[_U], _U] = ...,
    serializer: Callable[[Any, Any], Any] = ...,
) -> _T: ...
@overload
def field(
    *,
    type: tuple[type[_T]],
    invariant: Callable[[Any], tuple[bool, Any]] = ...,
    default: Any = ...,
    mandatory: bool = ...,
    factory: Callable[[_U], _U] = ...,
    serializer: Callable[[Any, Any], Any] = ...,
) -> _T: ...
@overload
def field(
    *,
    type: tuple[type[_T], type[_U]],
    invariant: Callable[[Any], tuple[bool, Any]] = ...,
    default: Any = ...,
    mandatory: bool = ...,
    factory: Callable[[_U], _U] = ...,
    serializer: Callable[[Any, Any], Any] = ...,
) -> _T | _U: ...
@overload
def field(
    *,
    type: tuple[Any, ...] = ...,
    invariant: Callable[[Any], tuple[bool, Any]] = ...,
    default: Any = ...,
    mandatory: bool = ...,
    factory: Callable[[_U], _U] = ...,
    serializer: Callable[[Any, Any], Any] = ...,
) -> Any: ...
def field(
    *,
    type: type[_T] | tuple[type[_T], ...] = PFIELD_NO_TYPE,  # noqa: A002
    invariant: Callable[[Any], tuple[bool, Any]] = PFIELD_NO_INVARIANT,
    default: Any = PFIELD_NO_INITIAL,
    mandatory: bool = False,
    factory: Callable[[_U], _U] = PFIELD_NO_FACTORY,
    serializer: Callable[[Any, Any], Any] = PFIELD_NO_SERIALIZER,
) -> Any:
    """Field specification factory for :py:class:`PRecord`."""
    return _field(
        type=type,
        invariant=invariant,
        initial=default,
        mandatory=mandatory,
        factory=factory,
        serializer=serializer,
    )


@dataclass_transform(kw_only_default=True, field_specifiers=(field,))
class PRecord(_PRecord): ...


__all__ = ["PRecord", "field"]
