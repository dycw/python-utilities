from __future__ import annotations

from dataclasses import MISSING, Field, dataclass, fields, is_dataclass, replace
from operator import eq
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    TypeGuard,
    TypeVar,
    overload,
    runtime_checkable,
)

from typing_extensions import Protocol, override

from utilities.errors import ImpossibleCaseError
from utilities.functions import get_class_name
from utilities.sentinel import Sentinel
from utilities.typing import get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from utilities.types import StrMapping


@runtime_checkable
class Dataclass(Protocol):
    """Protocol for `dataclass` classes."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


def asdict_without_defaults(
    obj: Dataclass,
    /,
    *,
    comparisons: Mapping[type[Any], Callable[[Any, Any], bool]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    final: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
    recursive: bool = False,
) -> StrMapping:
    """Cast a dataclass as a dictionary, without its defaults."""
    out: dict[str, Any] = {}
    for field in fields(obj):
        name = field.name
        value = getattr(obj, name)
        if _is_not_default_value(
            obj,
            field,
            value,
            comparisons=comparisons,
            globalns=globalns,
            localns=localns,
        ):
            if recursive and is_dataclass_instance(value):
                value_as_dict = asdict_without_defaults(
                    value,
                    comparisons=comparisons,
                    final=final,
                    recursive=recursive,
                    globalns=globalns,
                    localns=localns,
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


def is_dataclass_class(obj: Any, /) -> TypeGuard[type[Dataclass]]:
    """Check if an object is a dataclass."""
    return isinstance(obj, type) and is_dataclass(obj)


def is_dataclass_instance(obj: Any, /) -> TypeGuard[Dataclass]:
    """Check if an object is an instance of a dataclass."""
    return (not isinstance(obj, type)) and is_dataclass(obj)


_T = TypeVar("_T", bound=Dataclass)


@overload
def replace_non_sentinel(
    obj: Any, /, *, in_place: Literal[True], **kwargs: Any
) -> None: ...
@overload
def replace_non_sentinel(
    obj: _T, /, *, in_place: Literal[False] = False, **kwargs: Any
) -> _T: ...
@overload
def replace_non_sentinel(
    obj: _T, /, *, in_place: bool = False, **kwargs: Any
) -> _T | None: ...
def replace_non_sentinel(
    obj: _T, /, *, in_place: bool = False, **kwargs: Any
) -> _T | None:
    """Replace attributes on a dataclass, filtering out sentinel values."""
    if in_place:
        for k, v in kwargs.items():
            if not isinstance(v, Sentinel):
                setattr(obj, k, v)
        return None
    return replace(
        obj, **{k: v for k, v in kwargs.items() if not isinstance(v, Sentinel)}
    )


def repr_without_defaults(
    obj: Dataclass,
    /,
    *,
    comparisons: Mapping[type[Any], Callable[[Any, Any], bool]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    recursive: bool = False,
) -> str:
    """Repr a dataclass, without its defaults."""
    out: dict[str, str] = {}
    for field in fields(obj):
        name = field.name
        value = getattr(obj, name)
        if (
            _is_not_default_value(
                obj,
                field,
                value,
                comparisons=comparisons,
                globalns=globalns,
                localns=localns,
            )
            and field.repr
        ):
            if recursive and is_dataclass_instance(value):
                repr_as_dict = repr_without_defaults(
                    value,
                    comparisons=comparisons,
                    globalns=globalns,
                    localns=localns,
                    recursive=recursive,
                )
            else:
                repr_as_dict = repr(value)
            out[name] = repr_as_dict
    cls = get_class_name(obj)
    joined = ", ".join(f"{k}={v}" for k, v in out.items())
    return f"{cls}({joined})"


def yield_field_names(obj: Dataclass | type[Dataclass], /) -> Iterator[str]:
    """Yield the field names of a dataclass."""
    for field in fields(obj):
        yield field.name


def _is_not_default_value(
    cls: Dataclass | type[Dataclass],
    field: Field,
    value: Any,
    /,
    *,
    comparisons: Mapping[type[Any], Callable[[Any, Any], bool]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> bool:
    if (field.default is MISSING) and (field.default_factory is MISSING):
        return True
    if (field.default is not MISSING) and (field.default_factory is MISSING):
        return bool(value != field.default)
    if (field.default is MISSING) and (field.default_factory is not MISSING):
        if comparisons is None:
            cmp = eq
        else:
            hints = get_type_hints(cls, globalns=globalns, localns=localns)
            type_ = hints[field.name]
            cmp = comparisons.get(type_, eq)
        try:
            return not cmp(value, field.default_factory())
        except TypeError:
            return True
    raise ImpossibleCaseError(  # pragma: no cover
        case=[f"{field.default_factory=}", f"{field.default_factory=}"]
    )


__all__ = [
    "Dataclass",
    "GetDataClassClassError",
    "asdict_without_defaults",
    "get_dataclass_class",
    "is_dataclass_class",
    "is_dataclass_instance",
    "replace_non_sentinel",
    "repr_without_defaults",
    "yield_field_names",
]
