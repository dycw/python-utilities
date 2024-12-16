from __future__ import annotations

from dataclasses import MISSING, Field, dataclass, field, fields, replace
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, overload

from typing_extensions import override

from utilities.errors import ImpossibleCaseError
from utilities.functions import get_class_name
from utilities.operator import is_equal
from utilities.sentinel import Sentinel, sentinel
from utilities.types import (
    Dataclass,
    StrMapping,
    is_dataclass_class,
    is_dataclass_instance,
)
from utilities.typing import get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping


_T = TypeVar("_T")


def asdict_without_defaults(
    obj: Dataclass,
    /,
    *,
    comparisons: Mapping[type[_T], Callable[[_T, _T], bool]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    final: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
    recursive: bool = False,
) -> StrMapping:
    """Cast a dataclass as a dictionary, without its defaults."""
    out: dict[str, Any] = {}
    for fld in yield_fields(obj, globalns=globalns, localns=localns):
        name = fld.name
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


_TDataclass = TypeVar("_TDataclass", bound=Dataclass)


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
    for fld in yield_fields(obj):
        name = fld.name
        value = getattr(obj, name)
        if (name not in ignore_use) and (
            _is_not_default_value(
                obj,
                fld,
                value,
                comparisons=comparisons,
                globalns=globalns,
                localns=localns,
            )
            and fld.repr
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


@dataclass(kw_only=True, slots=True)
class _YieldFieldsInstance(Generic[_T]):
    name: str
    value: _T
    type_: type[_T]
    default: _T = sentinel
    default_factory: Callable[[], _T] = sentinel
    repr: bool = True
    hash_: bool | None = None
    init: bool = True
    compare: bool = True
    metadata: StrMapping = field(default_factory=dict)
    kw_only: bool | Sentinel = sentinel

    def is_equal_to_default(
        self,
        *,
        rel_tol: float | None = None,
        abs_tol: float | None = None,
        extra: Mapping[type[_T], Callable[[_T, _T], bool]] | None = None,
    ) -> bool:
        if isinstance(self.default, Sentinel) and isinstance(
            self.default_factory, Sentinel
        ):
            return False
        if (not isinstance(self.default, Sentinel)) and isinstance(
            self.default_factory, Sentinel
        ):
            expected = self.default
        elif isinstance(self.default, Sentinel) and (
            not isinstance(self.default_factory, Sentinel)
        ):
            expected = self.default_factory()
        else:  # pragma: no cover
            raise ImpossibleCaseError(
                case=[f"{self.default=}", f"{self.default_factory=}"]
            )
        return is_equal(
            self.value, expected, rel_tol=rel_tol, abs_tol=abs_tol, extra=extra
        )


@dataclass(kw_only=True, slots=True)
class _YieldFieldsClass(Generic[_T]):
    name: str
    type_: type[_T]
    default: _T = sentinel
    default_factory: Callable[[], _T] = sentinel
    repr: bool = True
    hash_: bool | None = None
    init: bool = True
    compare: bool = True
    metadata: StrMapping = field(default_factory=dict)
    kw_only: bool | Sentinel = sentinel


@overload
def yield_fields(
    obj: Dataclass,
    /,
    *,
    globalns: StrMapping | None = ...,
    localns: StrMapping | None = ...,
) -> Iterator[_YieldFieldsInstance[Any]]: ...
@overload
def yield_fields(
    obj: type[Dataclass],
    /,
    *,
    globalns: StrMapping | None = ...,
    localns: StrMapping | None = ...,
) -> Iterator[_YieldFieldsClass[Any]]: ...
def yield_fields(
    obj: Dataclass | type[Dataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> Iterator[_YieldFieldsInstance[Any]] | Iterator[_YieldFieldsClass[Any]]:
    """Yield the fields of a dataclass."""
    if is_dataclass_instance(obj):
        for field in yield_fields(type(obj)):
            yield _YieldFieldsInstance(
                name=field.name,
                value=getattr(obj, field.name),
                type_=field.type_,
                default=field.default,
                default_factory=field.default_factory,
                init=field.init,
                repr=field.repr,
                hash_=field.hash_,
                compare=field.compare,
                metadata=field.metadata,
                kw_only=field.kw_only,
            )
    elif is_dataclass_class(obj):
        for field in fields(obj):
            if isinstance(field.type, type):
                type_ = field.type
            else:
                hints = get_type_hints(obj, globalns=globalns, localns=localns)
                type_ = hints.get(field.name, field.type)
            yield (
                _YieldFieldsClass(
                    name=field.name,
                    type_=type_,
                    default=sentinel if field.default is MISSING else field.default,
                    default_factory=sentinel
                    if field.default_factory is MISSING
                    else field.default_factory,
                    init=field.init,
                    repr=field.repr,
                    hash_=field.hash,
                    compare=field.compare,
                    metadata=dict(field.metadata),
                    kw_only=sentinel if field.kw_only is MISSING else field.kw_only,
                )
            )
    else:
        raise YieldFieldsError(obj=obj)


@dataclass(kw_only=True, slots=True)
class YieldFieldsError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object must be a dataclass instance or class; got {self.obj}"


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
    "GetDataClassClassError",
    "YieldFieldsError",
    "asdict_without_defaults",
    "replace_non_sentinel",
    "repr_without_defaults",
    "yield_fields",
]
