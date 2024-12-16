from __future__ import annotations

from dataclasses import MISSING, Field, fields, replace
from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload

from utilities.errors import ImpossibleCaseError
from utilities.functions import get_class_name
from utilities.operator import is_equal
from utilities.sentinel import Sentinel
from utilities.types import Dataclass, StrMapping, is_dataclass_instance
from utilities.typing import get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping


_T = TypeVar("_T")
_TDataclass = TypeVar("_TDataclass", bound=Dataclass)


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


@overload
def replace_non_sentinel(
    obj: Any, /, *, in_place: Literal[True], **kwargs: Any
) -> None: ...
@overload
def replace_non_sentinel(
    obj: _TDataclass, /, *, in_place: Literal[False] = False, **kwargs: Any
) -> _TDataclass: ...
@overload
def replace_non_sentinel(
    obj: _TDataclass, /, *, in_place: bool = False, **kwargs: Any
) -> _TDataclass | None: ...
def replace_non_sentinel(
    obj: _TDataclass, /, *, in_place: bool = False, **kwargs: Any
) -> _TDataclass | None:
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
    ignore: Iterable[str] | None = None,
    comparisons: Mapping[type[Any], Callable[[Any, Any], bool]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    recursive: bool = False,
) -> str:
    """Repr a dataclass, without its defaults."""
    ignore_use: set[str] = set() if ignore is None else set(ignore)
    out: dict[str, str] = {}
    for field in fields(obj):
        name = field.name
        value = getattr(obj, name)
        if (name not in ignore_use) and (
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
                    ignore=ignore,
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
    comparisons: Mapping[type[_T], Callable[[_T, _T], bool]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> bool:
    if (field.default is MISSING) and (field.default_factory is MISSING):
        return True
    if (field.default is not MISSING) and (field.default_factory is MISSING):
        expected = field.default
    elif (field.default is MISSING) and (field.default_factory is not MISSING):
        expected = field.default_factory()
    else:  # pragma: no cover
        raise ImpossibleCaseError(
            case=[f"{field.default_factory=}", f"{field.default_factory=}"]
        )
    if comparisons is None:
        extra: Mapping[type[_T], Callable[[_T, _T], bool]] | None = None
    else:
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        type_ = hints[field.name]
        try:
            extra = {type_: comparisons[type_]}
        except KeyError:
            extra = None
    return not is_equal(value, expected, extra=extra)


__all__ = [
    "Dataclass",
    "asdict_without_defaults",
    "replace_non_sentinel",
    "repr_without_defaults",
    "yield_field_names",
]
