from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
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
from utilities.parse import ParseObjectError, parse_object, serialize_object
from utilities.re import ExtractGroupError, extract_group
from utilities.sentinel import Sentinel, sentinel
from utilities.text import (
    BRACKETS,
    LIST_SEPARATOR,
    PAIR_SEPARATOR,
    _SplitKeyValuePairsDuplicateKeysError,
    _SplitKeyValuePairsSplitError,
    split_key_value_pairs,
)
from utilities.types import (
    ParseObjectExtra,
    SerializeObjectExtra,
    StrStrMapping,
    TDataclass,
)
from utilities.typing import get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from collections.abc import Set as AbstractSet

    from utilities.types import Dataclass, StrMapping


_T = TypeVar("_T")
_U = TypeVar("_U")


##


def dataclass_repr(
    obj: Dataclass,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    extra: Mapping[type[_T], Callable[[_T, _T], bool]] | None = None,
    defaults: bool = False,
    recursive: bool = False,
) -> str:
    """Repr a dataclass, without its defaults."""
    out: dict[str, str] = {}
    for fld in yield_fields(
        obj, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
    ):
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
                        globalns=globalns,
                        localns=localns,
                        warn_name_errors=warn_name_errors,
                        include=include,
                        exclude=exclude,
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
                            globalns=globalns,
                            localns=localns,
                            warn_name_errors=warn_name_errors,
                            include=include,
                            exclude=exclude,
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
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    extra: Mapping[type[_T], Callable[[_T, _T], bool]] | None = None,
    defaults: bool = False,
    final: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
    recursive: bool = False,
) -> StrMapping:
    """Convert a dataclass to a dictionary."""
    out: StrMapping = {}
    for fld in yield_fields(
        obj, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
    ):
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
                        warn_name_errors=warn_name_errors,
                        include=include,
                        exclude=exclude,
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
                            warn_name_errors=warn_name_errors,
                            include=include,
                            exclude=exclude,
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
    fields: Iterable[_YieldFieldsClass[Any]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    head: bool = False,
    case_sensitive: bool = False,
    allow_extra: bool = False,
) -> TDataclass:
    """Construct a dataclass from a mapping."""
    if fields is None:
        fields_use = list(
            yield_fields(
                cls,
                globalns=globalns,
                localns=localns,
                warn_name_errors=warn_name_errors,
            )
        )
    else:
        fields_use = fields
    fields_to_values = str_mapping_to_field_mapping(
        cls,
        mapping,
        fields=fields_use,
        globalns=globalns,
        localns=localns,
        warn_name_errors=warn_name_errors,
        head=head,
        case_sensitive=case_sensitive,
        allow_extra=allow_extra,
    )
    field_names_to_values = {f.name: v for f, v in fields_to_values.items()}
    default = {
        f.name
        for f in fields_use
        if (not isinstance(f.default, Sentinel))
        or (not isinstance(f.default_factory, Sentinel))
    }
    have = set(field_names_to_values) | default
    missing = {f.name for f in fields_use} - have
    if len(missing) >= 1:
        raise MappingToDataclassError(cls=cls, fields=missing)
    return cls(**field_names_to_values)


@dataclass(kw_only=True, slots=True)
class MappingToDataclassError(Exception, Generic[TDataclass]):
    cls: type[TDataclass]
    fields: AbstractSet[str]

    @override
    def __str__(self) -> str:
        desc = ", ".join(map(repr, sorted(self.fields)))
        return f"Unable to construct {get_class_name(self.cls)!r}; missing values for {desc}"


##


def one_field(
    cls: type[Dataclass],
    key: str,
    /,
    *,
    fields: Iterable[_YieldFieldsClass[Any]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    head: bool = False,
    case_sensitive: bool = False,
) -> _YieldFieldsClass[Any]:
    """Get the unique field a key matches to."""
    if fields is None:
        fields_use = list(
            yield_fields(
                cls,
                globalns=globalns,
                localns=localns,
                warn_name_errors=warn_name_errors,
            )
        )
    else:
        fields_use = fields
    mapping = {f.name: f for f in fields_use}
    try:
        name = one_str(mapping, key, head=head, case_sensitive=case_sensitive)
    except OneStrEmptyError:
        raise OneFieldEmptyError(
            cls=cls, key=key, head=head, case_sensitive=case_sensitive
        ) from None
    except OneStrNonUniqueError as error:
        raise OneFieldNonUniqueError(
            cls=cls,
            key=key,
            head=head,
            case_sensitive=case_sensitive,
            first=error.first,
            second=error.second,
        ) from None
    return mapping[name]


@dataclass(kw_only=True, slots=True)
class OneFieldError(Exception, Generic[TDataclass]):
    cls: type[TDataclass]
    key: str
    head: bool = False
    case_sensitive: bool = False


@dataclass(kw_only=True, slots=True)
class OneFieldEmptyError(OneFieldError[TDataclass]):
    @override
    def __str__(self) -> str:
        head = f"Dataclass {get_class_name(self.cls)!r} does not contain"
        match self.head, self.case_sensitive:
            case False, True:
                tail = f"a field {self.key!r}"
            case False, False:
                tail = f"a field {self.key!r} (modulo case)"
            case True, True:
                tail = f"any field starting with {self.key!r}"
            case True, False:
                tail = f"any field starting with {self.key!r} (modulo case)"
            case _ as never:
                assert_never(never)
        return f"{head} {tail}"


@dataclass(kw_only=True, slots=True)
class OneFieldNonUniqueError(OneFieldError[TDataclass]):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        head = f"Dataclass {get_class_name(self.cls)!r} must contain"
        match self.head, self.case_sensitive:
            case False, True:
                raise ImpossibleCaseError(  # pragma: no cover
                    case=[f"{self.head=}", f"{self.case_sensitive=}"]
                )
            case False, False:
                mid = f"field {self.key!r} exactly once (modulo case)"
            case True, True:
                mid = f"exactly one field starting with {self.key!r}"
            case True, False:
                mid = f"exactly one field starting with {self.key!r} (modulo case)"
            case _ as never:
                assert_never(never)
        return f"{head} {mid}; got {self.first!r}, {self.second!r} and perhaps more"


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


def serialize_dataclass(
    obj: Dataclass,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    extra_equal: Mapping[type[_U], Callable[[_U, _U], bool]] | None = None,
    defaults: bool = False,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    extra_serializers: SerializeObjectExtra | None = None,
) -> str:
    """Serialize a Dataclass."""
    mapping: StrStrMapping = {}
    fields = list(
        yield_fields(
            obj, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
        )
    )
    for fld in fields:
        if fld.keep(
            include=include,
            exclude=exclude,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
            extra=extra_equal,
            defaults=defaults,
        ):
            mapping[fld.name] = serialize_object(
                fld.value,
                list_separator=list_separator,
                pair_separator=pair_separator,
                extra=extra_serializers,
            )
    return serialize_object(
        mapping,
        list_separator=list_separator,
        pair_separator=pair_separator,
        extra=extra_serializers,
    )


def parse_dataclass(
    text_or_mapping: str | StrStrMapping,
    cls: type[TDataclass],
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    head: bool = False,
    case_sensitive: bool = False,
    allow_extra_keys: bool = False,
    extra_parsers: ParseObjectExtra | None = None,
) -> TDataclass:
    """Construct a dataclass from a string or a mapping or strings."""
    match text_or_mapping:
        case str() as text:
            keys_to_serializes = _parse_dataclass_split_key_value_pairs(
                text,
                cls,
                list_separator=list_separator,
                pair_separator=pair_separator,
                brackets=brackets,
            )
        case Mapping() as keys_to_serializes:
            ...
        case _ as never:
            assert_never(never)
    fields = list(
        yield_fields(
            cls, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
        )
    )
    fields_to_serializes = str_mapping_to_field_mapping(
        cls,
        keys_to_serializes,
        fields=fields,
        globalns=globalns,
        localns=localns,
        warn_name_errors=warn_name_errors,
        head=head,
        case_sensitive=case_sensitive,
        allow_extra=allow_extra_keys,
    )
    field_names_to_values = {
        f.name: _parse_dataclass_parse_text(
            f,
            t,
            cls,
            list_separator=list_separator,
            pair_separator=pair_separator,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra_parsers,
        )
        for f, t in fields_to_serializes.items()
    }
    return mapping_to_dataclass(
        cls,
        field_names_to_values,
        fields=fields,
        globalns=globalns,
        localns=localns,
        warn_name_errors=warn_name_errors,
        head=head,
        case_sensitive=case_sensitive,
        allow_extra=allow_extra_keys,
    )


def _parse_dataclass_split_key_value_pairs(
    text: str,
    cls: type[TDataclass],
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
) -> StrStrMapping:
    with suppress(ExtractGroupError):
        text = extract_group(r"^\{?(.*?)\}?$", text)
    try:
        return split_key_value_pairs(
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            brackets=brackets,
            mapping=True,
        )
    except _SplitKeyValuePairsSplitError as error:
        raise _ParseDataClassSplitKeyValuePairsSplitError(
            text=error.inner, cls=cls
        ) from None
    except _SplitKeyValuePairsDuplicateKeysError as error:
        raise _ParseDataClassSplitKeyValuePairsDuplicateKeysError(
            text=error.text, cls=cls, counts=error.counts
        ) from None


def _parse_dataclass_parse_text(
    field: _YieldFieldsClass[Any],
    text: str,
    cls: type[Dataclass],
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseObjectExtra | None = None,
) -> Any:
    try:
        return parse_object(
            field.type_,
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
    except ParseObjectError:
        raise _ParseDataClassParseValueError(cls=cls, field=field, text=text) from None


@dataclass(kw_only=True, slots=True)
class ParseDataClassError(Exception, Generic[TDataclass]):
    text: str
    cls: type[TDataclass]


@dataclass(kw_only=True, slots=True)
class _ParseDataClassSplitKeyValuePairsSplitError(ParseDataClassError):
    @override
    def __str__(self) -> str:
        return f"Unable to construct {get_class_name(self.cls)!r}; failed to split key-value pair {self.text!r}"


@dataclass(kw_only=True, slots=True)
class _ParseDataClassSplitKeyValuePairsDuplicateKeysError(ParseDataClassError):
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Unable to construct {get_class_name(self.cls)!r} since there are duplicate keys; got {self.counts!r}"


@dataclass(kw_only=True, slots=True)
class _ParseDataClassParseValueError(ParseDataClassError[TDataclass]):
    field: _YieldFieldsClass[Any]

    @override
    def __str__(self) -> str:
        return f"Unable to construct {get_class_name(self.cls)!r}; unable to parse field {self.field.name!r} of type {self.field.type_!r}; got {self.text!r}"


##


def str_mapping_to_field_mapping(
    cls: type[TDataclass],
    mapping: Mapping[str, _T],
    /,
    *,
    fields: Iterable[_YieldFieldsClass[Any]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    head: bool = False,
    case_sensitive: bool = False,
    allow_extra: bool = False,
) -> Mapping[_YieldFieldsClass[Any], _T]:
    """Convert a string-mapping into a field-mapping."""
    keys_to_fields: Mapping[str, _YieldFieldsClass[Any]] = {}
    for key in mapping:
        try:
            keys_to_fields[key] = one_field(
                cls,
                key,
                fields=fields,
                globalns=globalns,
                localns=localns,
                warn_name_errors=warn_name_errors,
                head=head,
                case_sensitive=case_sensitive,
            )
        except OneFieldEmptyError:
            if not allow_extra:
                raise StrMappingToFieldMappingError(
                    cls=cls, key=key, head=head, case_sensitive=case_sensitive
                ) from None
    return {field: mapping[key] for key, field in keys_to_fields.items()}


@dataclass(kw_only=True, slots=True)
class StrMappingToFieldMappingError(Exception):
    cls: type[Dataclass]
    key: str
    head: bool = False
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        head = f"Dataclass {get_class_name(self.cls)!r} does not contain"
        match self.head, self.case_sensitive:
            case False, True:
                tail = f"a field {self.key!r}"
            case False, False:
                tail = f"a field {self.key!r} (modulo case)"
            case True, True:
                tail = f"any field starting with {self.key!r}"
            case True, False:
                tail = f"any field starting with {self.key!r} (modulo case)"
            case _ as never:
                assert_never(never)
        return f"{head} {tail}"


##


@overload
def yield_fields(
    obj: Dataclass,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
) -> Iterator[_YieldFieldsInstance[Any]]: ...
@overload
def yield_fields(
    obj: type[Dataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
) -> Iterator[_YieldFieldsClass[Any]]: ...
def yield_fields(
    obj: Dataclass | type[Dataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
) -> Iterator[_YieldFieldsInstance[Any]] | Iterator[_YieldFieldsClass[Any]]:
    """Yield the fields of a dataclass."""
    if is_dataclass_instance(obj):
        for field in yield_fields(
            type(obj),
            globalns=globalns,
            localns=localns,
            warn_name_errors=warn_name_errors,
        ):
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
        hints = get_type_hints(
            obj, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
        )
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
    "OneFieldEmptyError",
    "OneFieldError",
    "OneFieldNonUniqueError",
    "ParseDataClassError",
    "StrMappingToFieldMappingError",
    "YieldFieldsError",
    "dataclass_repr",
    "dataclass_to_dict",
    "mapping_to_dataclass",
    "one_field",
    "parse_dataclass",
    "replace_non_sentinel",
    "str_mapping_to_field_mapping",
    "yield_fields",
]
