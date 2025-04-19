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
from utilities.reprlib import get_repr
from utilities.sentinel import Sentinel, sentinel
from utilities.typing import get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from utilities.types import Dataclass, StrMapping, TDataclass


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
        if not self.case_sensitive:
            desc += " (modulo case)"
        return desc


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
    case_sensitive: bool = True,
) -> TDataclass:
    """Construct a dataclass from a string or a mapping or strings."""
    match text_or_mapping:
        case str() as text:
            mapping = _text_to_dataclass_parse_text(
                text,
                cls,
                globalns=globalns,
                localns=localns,
                case_sensitive=case_sensitive,
            )
        case Mapping() as mapping:
            pass
        case _ as never:
            assert_never(never)
    return mapping_to_dataclass(
        cls, mapping, globalns=globalns, localns=localns, case_sensitive=case_sensitive
    )


@dataclass(kw_only=True, slots=True)
class TextToDataClassError(Exception): ...


def _text_to_dataclass_parse_text(
    text: str,
    cls: type[TDataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    case_sensitive: bool = True,
) -> StrMapping:
    list(yield_fields(cls, globalns=globalns, localns=localns))
    [t for t in text.split(",") if t != ""]
    settings = cls.settings_cls()(**settings_kwargs)
    return cls(**strategy_kwargs, settings=settings)


def _text_to_dataclass_parse_key_value_pair(
    text: str, fields: Iterable[_YieldFieldsClass[Any]], /
) -> tuple[_YieldFieldsClass[Any], Any]:
    try:
        key, value = text.split("=")
    except ValueError:
        raise _TextToDataClassParseKeyValuePairSplitError(text=text) from None
    {f.name: f for f in fields}
    try:
        key = one(n for n in strategy_keys if key in n)
    except OneEmptyError:
        try:
            key = one(n for n in settings_keys if key in n)
        except OneEmptyError:
            raise _Strategy_FromStr_InvalidKeyError(*[f"{key=}"]) from None
        except OneNonUniqueError as error:
            raise _Strategy_FromStr_AmbiguousKeyError(*[
                f"{key=}",
                f"{error.first=}",
                f"{error.second=}",
            ]) from None
        else:
            settings_kwargs[key] = cls.settings_cls().from_str(key, value)
    except OneNonUniqueError as error:
        raise _Strategy_FromStr_AmbiguousKeyError(*[
            f"{key=}",
            f"{error.first=}",
            f"{error.second=}",
        ]) from None
    else:
        strategy_kwargs[key] = cls._from_str_one(key, value)


@dataclass(kw_only=True, slots=True)
class _TextToDataClassParseKeyValuePairSplitError(Exception):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to split key-value pair {self.text!r}"


def _text_to_dataclass_core(
    field: _YieldFieldsClass[Any], value: Any, /, *, path: Path, values: StrMapping
) -> Any:
    type_ = field.type_
    if type_ is str:
        return value
    if type_ is bool:
        if value == "0" or search("false", value, flags=IGNORECASE):
            return False
        if value == "1" or search("true", value, flags=IGNORECASE):
            return True
        raise _LoadSettingsInvalidBoolError(
            path=path, values=values, field=field.name, value=value
        )
    if type_ is float:
        try:
            return float(value)
        except ValueError:
            raise _LoadSettingsInvalidFloatError(
                path=path, values=values, field=field.name, value=value
            ) from None
    if type_ is int:
        try:
            return int(value)
        except ValueError:
            raise _LoadSettingsInvalidIntError(
                path=path, values=values, field=field.name, value=value
            ) from None
    if type_ is Path:
        return Path(value).expanduser()
    if type_ is dt.date:
        from utilities.whenever import ParseDateError, parse_date

        try:
            return parse_date(value)
        except ParseDateError:
            raise _LoadSettingsInvalidDateError(
                path=path, values=values, field=field.name, value=value
            ) from None
    if type_ is dt.timedelta:
        from utilities.whenever import ParseTimedeltaError, parse_timedelta

        try:
            return parse_timedelta(value)
        except ParseTimedeltaError:
            raise _LoadSettingsInvalidTimeDeltaError(
                path=path, values=values, field=field.name, value=value
            ) from None
    if isinstance(type_, type) and issubclass(type_, Enum):
        try:
            return ensure_enum(value, type_)
        except EnsureEnumError:
            raise _LoadSettingsInvalidEnumError(
                path=path, values=values, field=field.name, type_=type_, value=value
            ) from None
    if is_literal_type(type_):
        return one_str(get_args(type_), value, case_sensitive=False)
    if is_optional_type(type_) and (one(get_args(type_)) is int):
        if (value is None) or (value == "") or search("none", value, flags=IGNORECASE):
            return None
        try:
            return int(value)
        except ValueError:
            raise _LoadSettingsInvalidNullableIntError(
                path=path, values=values, field=field.name, value=value
            ) from None
    raise _LoadSettingsTypeError(path=path, field=field.name, type=type_)


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


@dataclass(kw_only=True, slots=True)
class _YieldFieldsInstance(Generic[_T]):
    name: str
    value: _T
    type_: Any
    default: _T | Sentinel = sentinel
    default_factory: Callable[[], _T] | Sentinel = sentinel
    repr: bool = True
    hash_: bool | None = None
    init: bool = True
    compare: bool = True
    metadata: StrMapping = field(default_factory=dict)
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


@dataclass(kw_only=True, slots=True)
class _YieldFieldsClass(Generic[_T]):
    name: str
    type_: Any
    default: _T | Sentinel = sentinel
    default_factory: Callable[[], _T] | Sentinel = sentinel
    repr: bool = True
    hash_: bool | None = None
    init: bool = True
    compare: bool = True
    metadata: StrMapping = field(default_factory=dict)
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
