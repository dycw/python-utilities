from __future__ import annotations

from dataclasses import MISSING, dataclass, fields, is_dataclass, replace
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    TypeGuard,
    TypeVar,
    get_type_hints,
    runtime_checkable,
)

from typing_extensions import Protocol, override

from utilities.sentinel import Sentinel

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from utilities.types import StrMapping


@runtime_checkable
class Dataclass(Protocol):
    """Protocol for `dataclass` classes."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


def asdict_without_defaults(
    obj: Dataclass,
    /,
    *,
    final: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
    recursive: bool = False,
) -> StrMapping:
    """Cast a dataclass as a dictionary, without its defaults."""
    out: dict[str, Any] = {}
    for field in fields(obj):
        name = field.name
        value = getattr(obj, name)
        if (
            ((field.default is MISSING) and (field.default_factory is MISSING))
            or (
                (field.default is not MISSING)
                and (field.default_factory is MISSING)
                and (value != field.default)
            )
            or (
                (field.default is MISSING)
                and (field.default_factory is not MISSING)
                and (value != field.default_factory())
            )
        ):
            if recursive and is_dataclass_instance(value):
                value_as_dict = asdict_without_defaults(
                    value, final=final, recursive=recursive
                )
            else:
                value_as_dict = value
            out[name] = value_as_dict
    return out if final is None else final(type(obj), out)


def get_dataclass_class(obj: Dataclass | type[Dataclass], /) -> type[Dataclass]:
    """Get the underlying dataclass, if possible."""
    if is_dataclass_class(obj):
        return obj
    if is_dataclass_instance(obj):
        return type(obj)
    raise GetDataClassClassError(obj=obj)


@dataclass(kw_only=True, slots=True)
class GetDataClassClassError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object must be a dataclass instance or class; got {self.obj}"


def get_dataclass_fields(
    cls: type[Dataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> StrMapping:
    """Get the fields of a dataclass."""
    return get_type_hints(
        cls,
        globalns=globals() if globalns is None else dict(globalns),
        localns=locals() if localns is None else dict(localns),
    )


def is_dataclass_class(obj: Any, /) -> TypeGuard[type[Dataclass]]:
    """Check if an object is a dataclass."""
    return isinstance(obj, type) and is_dataclass(obj)


def is_dataclass_instance(obj: Any, /) -> TypeGuard[Dataclass]:
    """Check if an object is an instance of a dataclass."""
    return (not isinstance(obj, type)) and is_dataclass(obj)


_T = TypeVar("_T", bound=Dataclass)


def replace_non_sentinel(obj: _T, **kwargs: Any) -> _T:
    """Replace attributes on a dataclass, filtering out sentinel values."""
    return replace(
        obj, **{k: v for k, v in kwargs.items() if not isinstance(v, Sentinel)}
    )


def yield_field_names(obj: Dataclass | type[Dataclass], /) -> Iterator[str]:
    """Yield the field names of a dataclass."""
    for field in fields(obj):
        yield field.name


__all__ = [
    "Dataclass",
    "GetDataClassClassError",
    "asdict_without_defaults",
    "get_dataclass_class",
    "get_dataclass_fields",
    "is_dataclass_class",
    "is_dataclass_instance",
    "replace_non_sentinel",
    "yield_field_names",
]
