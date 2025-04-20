from __future__ import annotations

from collections.abc import Mapping
from dataclasses import MISSING, dataclass, field, fields, replace
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    assert_never,
    overload,
    override,
)

from utilities.errors import ImpossibleCaseError
from utilities.functions import (
    get_class_name,
    is_dataclass_class,
    is_dataclass_instance,
)
from utilities.iterables import OneStrEmptyError, OneStrNonUniqueError, one_str
from utilities.operator import is_equal
from utilities.parse import ParseTextError, parse_text
from utilities.reprlib import get_repr
from utilities.sentinel import Sentinel, sentinel
from utilities.types import TDataclass
from utilities.typing import get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from utilities.types import Dataclass, StrMapping


_T = TypeVar("_T")
_U = TypeVar("_U")


##


def dataclass_repr(
    obj: Dataclass,
    /,
    *,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    extra: Mapping[type[_T], Callable[[_T, _T], bool]] | None = None,
    defaults: bool = False,
    recursive: bool = False,
) -> str:
    """Repr a dataclass, without its defaults."""
    out: dict[str, str] = {}
    for fld in yield_fields(obj, globalns=globalns, localns=localns):
        if (
            fld.keep(
                include=include,
                exclude=exclude,
                rel_tol=rel_tol,
                abs_tol=abs_tol,
                extra=extra,
                defaults=defaults,
            )
            and fld.repr
        ):
            if recursive:
                if is_dataclass_instance(fld.value):
                    repr_ = dataclass_repr(
                        fld.value,
                        include=include,
                        exclude=exclude,
                        globalns=globalns,
                        localns=localns,
                        rel_tol=rel_tol,
                        abs_tol=abs_tol,
                        extra=extra,
                        defaults=defaults,
                        recursive=recursive,
                    )
                elif isinstance(fld.value, list):
                    repr_ = [
                        dataclass_repr(
                            v,
                            include=include,
                            exclude=exclude,
                            globalns=globalns,
                            localns=localns,
                            rel_tol=rel_tol,
                            abs_tol=abs_tol,
                            extra=extra,
                            defaults=defaults,
                            recursive=recursive,
                        )
                        if is_dataclass_instance(v)
                        else repr(v)
                        for v in fld.value
                    ]
                    repr_ = f"[{', '.join(repr_)}]"
                else:
                    repr_ = repr(fld.value)
            else:
                repr_ = repr(fld.value)
            out[fld.name] = repr_
    cls = get_class_name(obj)
    joined = ", ".join(f"{k}={v}" for k, v in out.items())
    return f"{cls}({joined})"


##


def dataclass_to_dict(
    obj: Dataclass,
    /,
    *,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    extra: Mapping[type[_T], Callable[[_T, _T], bool]] | None = None,
    defaults: bool = False,
    final: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
    recursive: bool = False,
) -> StrMapping:
    """Convert a dataclass to a dictionary."""
    out: StrMapping = {}
    for fld in yield_fields(obj, globalns=globalns, localns=localns):
        if fld.keep(
            include=include,
            exclude=exclude,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
            extra=extra,
            defaults=defaults,
        ):
            if recursive:
                if is_dataclass_instance(fld.value):
                    value = dataclass_to_dict(
                        fld.value,
                        globalns=globalns,
                        localns=localns,
                        rel_tol=rel_tol,
                        abs_tol=abs_tol,
                        extra=extra,
                        defaults=defaults,
                        final=final,
                        recursive=recursive,
                    )
                elif isinstance(fld.value, list):
                    value = [
                        dataclass_to_dict(
                            v,
                            globalns=globalns,
                            localns=localns,
                            rel_tol=rel_tol,
                            abs_tol=abs_tol,
                            extra=extra,
                            defaults=defaults,
                            final=final,
                            recursive=recursive,
                        )
                        if is_dataclass_instance(v)
                        else v
                        for v in fld.value
                    ]
                else:
                    value = fld.value
            else:
                value = fld.value
            out[fld.name] = value
    return out if final is None else final(type(obj), out)


##


def mapping_to_dataclass(
    cls: type[TDataclass],
    mapping: StrMapping,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    case_sensitive: bool = False,
    post: Callable[[_YieldFieldsClass[Any], Any], Any] | None = None,
) -> TDataclass:
    """Construct a dataclass from a mapping."""
    fields = yield_fields(cls, globalns=globalns, localns=localns)
    mapping_use = {
        f.name: _mapping_to_dataclass_one(
            f, mapping, case_sensitive=case_sensitive, post=post
        )
        for f in fields
    }
    return cls(**mapping_use)


def _mapping_to_dataclass_one(
    field: _YieldFieldsClass[Any],
    mapping: StrMapping,
    /,
    *,
    case_sensitive: bool = False,
    post: Callable[[_YieldFieldsClass[Any], Any], Any] | None = None,
) -> Any:
    try:
        key = one_str(mapping, field.name, case_sensitive=case_sensitive)
    except OneStrEmptyError:
        if not isinstance(field.default, Sentinel):
            value = field.default
        elif not isinstance(field.default_factory, Sentinel):
            value = field.default_factory()
        else:
            raise _MappingToDataclassEmptyError(
                mapping=mapping, field=field.name, case_sensitive=case_sensitive
            ) from None
    except OneStrNonUniqueError as error:
        raise _MappingToDataclassCaseInsensitiveNonUniqueError(
            mapping=mapping, field=field.name, first=error.first, second=error.second
        ) from None
    else:
        value = mapping[key]
    if post is not None:
        value = post(field, value)
    return value


@dataclass(kw_only=True, slots=True)
class MappingToDataclassError(Exception):
    mapping: StrMapping
    field: str


@dataclass(kw_only=True, slots=True)
class _MappingToDataclassEmptyError(MappingToDataclassError):
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        desc = f"Mapping {get_repr(self.mapping)} does not contain {self.field!r}"
        return desc if self.case_sensitive else f"{desc} (modulo case)"


@dataclass(kw_only=True, slots=True)
class _MappingToDataclassCaseInsensitiveNonUniqueError(MappingToDataclassError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        return f"Mapping {get_repr(self.mapping)} must contain {self.field!r} exactly once (modulo case); got {self.first!r}, {self.second!r} and perhaps more"


##


@overload
def replace_non_sentinel(
    obj: Any, /, *, in_place: Literal[True], **kwargs: Any
) -> None: ...
@overload
def replace_non_sentinel(
    obj: TDataclass, /, *, in_place: Literal[False] = False, **kwargs: Any
) -> TDataclass: ...
@overload
def replace_non_sentinel(
    obj: TDataclass, /, *, in_place: bool = False, **kwargs: Any
) -> TDataclass | None: ...
def replace_non_sentinel(
    obj: TDataclass, /, *, in_place: bool = False, **kwargs: Any
) -> TDataclass | None:
    """Replace attributes on a dataclass, filtering out sentinel values."""
    if in_place:
        for k, v in kwargs.items():
            if not isinstance(v, Sentinel):
                setattr(obj, k, v)
        return None
    return replace(
        obj, **{k: v for k, v in kwargs.items() if not isinstance(v, Sentinel)}
    )


##


def text_to_dataclass(
    text_or_mapping: str | Mapping[str, str],
    cls: type[TDataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    case_sensitive: bool = False,
) -> TDataclass:
    """Construct a dataclass from a string or a mapping or strings."""
    fields = list(yield_fields(cls, globalns=globalns, localns=localns))
    match text_or_mapping:
        case str() as text:
            text_mapping = _text_to_dataclass_split_text(text, cls)
        case Mapping() as text_mapping:
            ...
        case _ as never:
            assert_never(never)
    value_mapping = dict(
        _text_to_dataclass_get_and_parse(
            fields, key, value, cls, case_sensitive=case_sensitive
        )
        for key, value in text_mapping.items()
    )
    return mapping_to_dataclass(
        cls,
        value_mapping,
        globalns=globalns,
        localns=localns,
        case_sensitive=case_sensitive,
    )


def _text_to_dataclass_split_text(
    text: str, cls: type[TDataclass], /
) -> Mapping[str, str]:
    pairs = (t for t in text.split(",") if t != "")
    return dict(_text_to_dataclass_split_key_value_pair(pair, cls) for pair in pairs)


def _text_to_dataclass_split_key_value_pair(
    text: str, cls: type[Dataclass], /
) -> tuple[str, str]:
    try:
        key, value = text.split("=")
    except ValueError:
        raise _TextToDataClassSplitKeyValuePairError(cls=cls, text=text) from None
    return key, value


def _text_to_dataclass_get_and_parse(
    fields: Iterable[_YieldFieldsClass[Any]],
    key: str,
    value: str,
    cls: type[Dataclass],
    /,
    *,
    case_sensitive: bool = False,
) -> tuple[str, Any]:
    mapping = {f.name: f for f in fields}
    try:
        name = one_str(mapping, key, head=True, case_sensitive=case_sensitive)
    except OneStrEmptyError:
        raise _TextToDataClassGetFieldEmptyError(
            cls=cls, key=key, case_sensitive=case_sensitive
        ) from None
    except OneStrNonUniqueError as error:
        raise _TextToDataClassGetFieldNonUniqueError(
            cls=cls,
            key=key,
            case_sensitive=case_sensitive,
            first=error.first,
            second=error.second,
        ) from None
    field = mapping[name]
    try:
        parsed = parse_text(field.type_, value, case_sensitive=case_sensitive)
    except ParseTextError:
        raise _TextToDataClassParseValueError(
            cls=cls, field=field, text=value
        ) from None
    return key, parsed


@dataclass(kw_only=True, slots=True)
class TextToDataClassError(Exception, Generic[TDataclass]):
    cls: type[TDataclass]


@dataclass(kw_only=True, slots=True)
class _TextToDataClassSplitKeyValuePairError(TextToDataClassError):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to construct {get_class_name(self.cls)!r}; failed to split key-value pair {self.text!r}"


@dataclass(kw_only=True, slots=True)
class _TextToDataClassGetFieldEmptyError(TextToDataClassError[TDataclass]):
    key: str
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        desc = f"Dataclass {get_class_name(self.cls)!r} does not contain any field starting with {self.key!r}"
        return desc if self.case_sensitive else f"{desc} (modulo case)"


@dataclass(kw_only=True, slots=True)
class _TextToDataClassGetFieldNonUniqueError(TextToDataClassError[TDataclass]):
    key: str
    case_sensitive: bool = False
    first: str
    second: str

    @override
    def __str__(self) -> str:
        head = f"Dataclass {get_class_name(self.cls)!r} must contain exactly one field starting with {self.key!r}"
        mid = "" if self.case_sensitive else " (modulo case)"
        return f"{head}{mid}; got {self.first!r}, {self.second!r} and perhaps more"


@dataclass(kw_only=True, slots=True)
class _TextToDataClassParseValueError(TextToDataClassError[TDataclass]):
    field: _YieldFieldsClass[Any]
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to construct {get_class_name(self.cls)!r}; unable to parse field {self.field.name!r} of type {self.field.type_!r}; got {self.text!r}"


##


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
        for field in yield_fields(type(obj), globalns=globalns, localns=localns):
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
        hints = get_type_hints(obj, globalns=globalns, localns=localns)
        for field in fields(obj):
            if isinstance(field.type, type):
                type_ = field.type
            else:
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


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class _YieldFieldsInstance(Generic[_T]):
    name: str
    value: _T = field(hash=False)
    type_: Any = field(hash=False)
    default: _T | Sentinel = field(default=sentinel, hash=False)
    default_factory: Callable[[], _T] | Sentinel = field(default=sentinel, hash=False)
    repr: bool = True
    hash_: bool | None = None
    init: bool = True
    compare: bool = True
    metadata: StrMapping = field(default_factory=dict, hash=False)
    kw_only: bool | Sentinel = sentinel

    def equals_default(
        self,
        *,
        rel_tol: float | None = None,
        abs_tol: float | None = None,
        extra: Mapping[type[_U], Callable[[_U, _U], bool]] | None = None,
    ) -> bool:
        """Check if the field value equals its default."""
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

    def keep(
        self,
        *,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] | None = None,
        rel_tol: float | None = None,
        abs_tol: float | None = None,
        extra: Mapping[type[_U], Callable[[_U, _U], bool]] | None = None,
        defaults: bool = False,
    ) -> bool:
        """Whether to include a field."""
        if (include is not None) and (self.name not in include):
            return False
        if (exclude is not None) and (self.name in exclude):
            return False
        equal = self.equals_default(rel_tol=rel_tol, abs_tol=abs_tol, extra=extra)
        return (defaults and equal) or not equal


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class _YieldFieldsClass(Generic[_T]):
    name: str
    type_: Any = field(hash=False)
    default: _T | Sentinel = field(default=sentinel, hash=False)
    default_factory: Callable[[], _T] | Sentinel = field(default=sentinel, hash=False)
    repr: bool = True
    hash_: bool | None = None
    init: bool = True
    compare: bool = True
    metadata: StrMapping = field(default_factory=dict, hash=False)
    kw_only: bool | Sentinel = sentinel


@dataclass(kw_only=True, slots=True)
class YieldFieldsError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object must be a dataclass instance or class; got {self.obj}"


##

__all__ = [
    "MappingToDataclassError",
    "TextToDataClassError",
    "YieldFieldsError",
    "dataclass_repr",
    "dataclass_to_dict",
    "mapping_to_dataclass",
    "replace_non_sentinel",
    "text_to_dataclass",
    "yield_fields",
]
