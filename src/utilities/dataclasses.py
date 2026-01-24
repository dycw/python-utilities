from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from dataclasses import MISSING, dataclass, field, fields
from typing import TYPE_CHECKING, Any, assert_never, overload, override

from utilities.constants import (
    ABS_TOL,
    BRACKETS,
    LIST_SEPARATOR,
    PAIR_SEPARATOR,
    REL_TOL,
    Sentinel,
    sentinel,
)
from utilities.core import (
    ExtractGroupError,
    OneStrEmptyError,
    OneStrNonUniqueError,
    extract_group,
    get_class_name,
    is_sentinel,
    one_str,
)
from utilities.errors import ImpossibleCaseError
from utilities.iterables import cmp_nullable
from utilities.operator import is_equal
from utilities.parse import (
    _ParseObjectExtraNonUniqueError,
    _ParseObjectParseError,
    parse_object,
    serialize_object,
)
from utilities.text import (
    _SplitKeyValuePairsDuplicateKeysError,
    _SplitKeyValuePairsSplitError,
    split_key_value_pairs,
)
from utilities.types import MaybeType, SupportsLT
from utilities.typing import get_type_hints, is_dataclass_class, is_dataclass_instance

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from collections.abc import Set as AbstractSet

    from utilities.types import (
        Dataclass,
        ParseObjectExtra,
        SerializeObjectExtra,
        StrMapping,
        StrStrMapping,
    )


def dataclass_to_dict[T](
    obj: Dataclass,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    rel_tol: float = REL_TOL,
    abs_tol: float = ABS_TOL,
    extra: Mapping[type[T], Callable[[T, T], bool]] | None = None,
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


def is_nullable_lt[T: SupportsLT](x: T | None, y: T | None, /) -> bool | None:
    """Compare two nullable fields."""
    match cmp_nullable(x, y):
        case 1:
            return False
        case -1:
            return True
        case 0:
            return None
        case never:
            assert_never(never)


##


def mapping_to_dataclass[T: Dataclass](
    cls: type[T],
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
) -> T:
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
    try:
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
    except _StrMappingToFieldMappingEmptyError as error:
        raise _MappingToDataClassEmptyError(
            cls=cls, key=error.key, head=head, case_sensitive=case_sensitive
        ) from None
    except _StrMappingToFieldMappingNonUniqueError as error:
        raise _MappingToDataClassNonUniqueError(
            cls=cls,
            key=error.key,
            head=head,
            case_sensitive=case_sensitive,
            first=error.first,
            second=error.second,
        ) from None
    field_names_to_values = {f.name: v for f, v in fields_to_values.items()}
    default = {
        f.name
        for f in fields_use
        if (not is_sentinel(f.default)) or (not is_sentinel(f.default_factory))
    }
    have = set(field_names_to_values) | default
    missing = {f.name for f in fields_use} - have
    if len(missing) >= 1:
        raise _MappingToDataClassMissingValuesError(cls=cls, fields=missing)
    return cls(**field_names_to_values)


@dataclass(kw_only=True, slots=True)
class MappingToDataclassError[T: Dataclass](Exception):
    cls: type[T]


@dataclass(kw_only=True, slots=True)
class _MappingToDataClassEmptyError(MappingToDataclassError):
    key: str
    head: bool = False
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        return _empty_error_str(
            self.cls, self.key, head=self.head, case_sensitive=self.case_sensitive
        )


@dataclass(kw_only=True, slots=True)
class _MappingToDataClassNonUniqueError(MappingToDataclassError):
    key: str
    head: bool = False
    case_sensitive: bool = False
    first: str
    second: str

    @override
    def __str__(self) -> str:
        return _non_unique_error_str(
            self.cls,
            self.key,
            self.first,
            self.second,
            head=self.head,
            case_sensitive=self.case_sensitive,
        )


@dataclass(kw_only=True, slots=True)
class _MappingToDataClassMissingValuesError(MappingToDataclassError):
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
        raise _OneFieldEmptyError(
            cls=cls, key=key, head=head, case_sensitive=case_sensitive
        ) from None
    except OneStrNonUniqueError as error:
        raise _OneFieldNonUniqueError(
            cls=cls,
            key=key,
            head=head,
            case_sensitive=case_sensitive,
            first=error.first,
            second=error.second,
        ) from None
    return mapping[name]


@dataclass(kw_only=True, slots=True)
class OneFieldError[T: Dataclass](Exception):
    cls: type[T]
    key: str
    head: bool = False
    case_sensitive: bool = False


@dataclass(kw_only=True, slots=True)
class _OneFieldEmptyError(OneFieldError):
    @override
    def __str__(self) -> str:
        return _empty_error_str(
            self.cls, self.key, head=self.head, case_sensitive=self.case_sensitive
        )


@dataclass(kw_only=True, slots=True)
class _OneFieldNonUniqueError(OneFieldError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        return _non_unique_error_str(
            self.cls,
            self.key,
            self.first,
            self.second,
            head=self.head,
            case_sensitive=self.case_sensitive,
        )


##


def serialize_dataclass[T](
    obj: Dataclass,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    rel_tol: float = REL_TOL,
    abs_tol: float = ABS_TOL,
    extra_equal: Mapping[type[T], Callable[[T, T], bool]] | None = None,
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


def parse_dataclass[T: Dataclass](
    text_or_mapping: str | StrStrMapping,
    cls: type[T],
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
) -> T:
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
        case never:
            assert_never(never)
    fields = list(
        yield_fields(
            cls, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
        )
    )
    try:
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
    except _StrMappingToFieldMappingEmptyError as error:
        raise _ParseDataClassStrMappingToFieldMappingEmptyError(
            cls=cls, key=error.key, head=head, case_sensitive=case_sensitive
        ) from None
    except _StrMappingToFieldMappingNonUniqueError as error:
        raise _ParseDataClassStrMappingToFieldMappingNonUniqueError(
            cls=cls,
            key=error.key,
            head=head,
            case_sensitive=case_sensitive,
            first=error.first,
            second=error.second,
        ) from None
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
    try:
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
    except _MappingToDataClassMissingValuesError as error:
        raise _ParseDataClassMissingValuesError(cls=cls, fields=error.fields) from None


def _parse_dataclass_split_key_value_pairs[T: Dataclass](
    text: str,
    cls: type[T],
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
            cls=cls, counts=error.counts
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
    except _ParseObjectParseError:
        raise _ParseDataClassTextParseError(cls=cls, field=field, text=text) from None
    except _ParseObjectExtraNonUniqueError as error:
        raise _ParseDataClassTextExtraNonUniqueError(
            cls=cls, field=field, first=error.first, second=error.second
        ) from None


@dataclass(kw_only=True, slots=True)
class ParseDataClassError[T: Dataclass](Exception):
    cls: type[T]


@dataclass(kw_only=True, slots=True)
class _ParseDataClassSplitKeyValuePairsSplitError(ParseDataClassError):
    text: str

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
class _ParseDataClassTextParseError(ParseDataClassError):
    field: _YieldFieldsClass[Any]
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to construct {get_class_name(self.cls)!r} since the field {self.field.name!r} of type {self.field.type_!r} could not be parsed; got {self.text!r}"


@dataclass(kw_only=True, slots=True)
class _ParseDataClassTextExtraNonUniqueError(ParseDataClassError):
    field: _YieldFieldsClass[Any]
    first: type[Any]
    second: type[Any]

    @override
    def __str__(self) -> str:
        return f"Unable to construct {get_class_name(self.cls)!r} since the field {self.field.name!r} of type {self.field.type_!r} must contain exactly one parent class in `extra`; got {self.first!r}, {self.second!r} and perhaps more"


@dataclass(kw_only=True, slots=True)
class _ParseDataClassStrMappingToFieldMappingEmptyError(ParseDataClassError):
    key: str
    head: bool = False
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        head = f"Unable to construct {get_class_name(self.cls)!r} since it does not contain"
        tail = _empty_error_str_core(
            self.key, head=self.head, case_sensitive=self.case_sensitive
        )
        return f"{head} {tail}"


@dataclass(kw_only=True, slots=True)
class _ParseDataClassStrMappingToFieldMappingNonUniqueError(ParseDataClassError):
    key: str
    head: bool = False
    case_sensitive: bool = False
    first: str
    second: str

    @override
    def __str__(self) -> str:
        head = f"Unable to construct {get_class_name(self.cls)!r} since it must contain"
        tail = _non_unique_error_str_core(
            self.key,
            self.first,
            self.second,
            head=self.head,
            case_sensitive=self.case_sensitive,
        )
        return f"{head} {tail}"


@dataclass(kw_only=True, slots=True)
class _ParseDataClassMissingValuesError(ParseDataClassError):
    fields: AbstractSet[str]

    @override
    def __str__(self) -> str:
        desc = ", ".join(map(repr, sorted(self.fields)))
        return f"Unable to construct {get_class_name(self.cls)!r}; missing values for {desc}"


##


def str_mapping_to_field_mapping[T: Dataclass, U](
    cls: type[T],
    mapping: Mapping[str, U],
    /,
    *,
    fields: Iterable[_YieldFieldsClass[Any]] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    head: bool = False,
    case_sensitive: bool = False,
    allow_extra: bool = False,
) -> Mapping[_YieldFieldsClass[Any], U]:
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
        except _OneFieldEmptyError:
            if not allow_extra:
                raise _StrMappingToFieldMappingEmptyError(
                    cls=cls, key=key, head=head, case_sensitive=case_sensitive
                ) from None
        except _OneFieldNonUniqueError as error:
            raise _StrMappingToFieldMappingNonUniqueError(
                cls=cls,
                key=key,
                head=head,
                case_sensitive=case_sensitive,
                first=error.first,
                second=error.second,
            ) from None
    return {field: mapping[key] for key, field in keys_to_fields.items()}


@dataclass(kw_only=True, slots=True)
class StrMappingToFieldMappingError[T: Dataclass](Exception):
    cls: type[T]
    key: str
    head: bool = False
    case_sensitive: bool = False


@dataclass(kw_only=True, slots=True)
class _StrMappingToFieldMappingEmptyError(StrMappingToFieldMappingError):
    @override
    def __str__(self) -> str:
        return _empty_error_str(
            self.cls, self.key, head=self.head, case_sensitive=self.case_sensitive
        )


@dataclass(kw_only=True, slots=True)
class _StrMappingToFieldMappingNonUniqueError(StrMappingToFieldMappingError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        return _non_unique_error_str(
            self.cls,
            self.key,
            self.first,
            self.second,
            head=self.head,
            case_sensitive=self.case_sensitive,
        )


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
    obj: MaybeType[Dataclass],
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
class _YieldFieldsInstance[T]:
    name: str
    value: T = field(hash=False)
    type_: Any = field(hash=False)
    default: T | Sentinel = field(default=sentinel, hash=False)
    default_factory: Callable[[], T] | Sentinel = field(default=sentinel, hash=False)
    repr: bool = True
    hash_: bool | None = None
    init: bool = True
    compare: bool = True
    metadata: StrMapping = field(default_factory=dict, hash=False)
    kw_only: bool | Sentinel = sentinel

    def equals_default[U](
        self,
        *,
        rel_tol: float = REL_TOL,
        abs_tol: float = ABS_TOL,
        extra: Mapping[type[U], Callable[[U, U], bool]] | None = None,
    ) -> bool:
        """Check if the field value equals its default."""
        if is_sentinel(self.default) and is_sentinel(self.default_factory):
            return False
        if (not is_sentinel(self.default)) and is_sentinel(self.default_factory):
            expected = self.default
        elif is_sentinel(self.default) and (not is_sentinel(self.default_factory)):
            expected = self.default_factory()
        else:  # pragma: no cover
            raise ImpossibleCaseError(
                case=[f"{self.default=}", f"{self.default_factory=}"]
            )
        return is_equal(
            self.value, expected, rel_tol=rel_tol, abs_tol=abs_tol, extra=extra
        )

    def keep[U](
        self,
        *,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] | None = None,
        rel_tol: float = REL_TOL,
        abs_tol: float = ABS_TOL,
        extra: Mapping[type[U], Callable[[U, U], bool]] | None = None,
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
class _YieldFieldsClass[T]:
    name: str
    type_: Any = field(hash=False)
    default: T | Sentinel = field(default=sentinel, hash=False)
    default_factory: Callable[[], T] | Sentinel = field(default=sentinel, hash=False)
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


def _empty_error_str(
    cls: type[Dataclass],
    key: str,
    /,
    *,
    head: bool = False,
    case_sensitive: bool = False,
) -> str:
    head_msg = f"Dataclass {get_class_name(cls)!r} does not contain"
    tail_msg = _empty_error_str_core(key, head=head, case_sensitive=case_sensitive)
    return f"{head_msg} {tail_msg}"


def _empty_error_str_core(
    key: str, /, *, head: bool = False, case_sensitive: bool = False
) -> str:
    match head, case_sensitive:
        case False, True:
            return f"a field {key!r}"
        case False, False:
            return f"a field {key!r} (modulo case)"
        case True, True:
            return f"any field starting with {key!r}"
        case True, False:
            return f"any field starting with {key!r} (modulo case)"
        case never:
            assert_never(never)


def _non_unique_error_str(
    cls: type[Dataclass],
    key: str,
    first: str,
    second: str,
    /,
    *,
    head: bool = False,
    case_sensitive: bool = False,
) -> str:
    head_msg = f"Dataclass {get_class_name(cls)!r} must contain"
    tail_msg = _non_unique_error_str_core(
        key, first, second, head=head, case_sensitive=case_sensitive
    )
    return f"{head_msg} {tail_msg}"


def _non_unique_error_str_core(
    key: str,
    first: str,
    second: str,
    /,
    *,
    head: bool = False,
    case_sensitive: bool = False,
) -> str:
    match head, case_sensitive:
        case False, True:
            raise ImpossibleCaseError(  # pragma: no cover
                case=[f"{head=}", f"{case_sensitive=}"]
            )
        case False, False:
            head_msg = f"field {key!r} exactly once (modulo case)"
        case True, True:
            head_msg = f"exactly one field starting with {key!r}"
        case True, False:
            head_msg = f"exactly one field starting with {key!r} (modulo case)"
        case never:
            assert_never(never)
    return f"{head_msg}; got {first!r}, {second!r} and perhaps more"


__all__ = [
    "MappingToDataclassError",
    "OneFieldError",
    "ParseDataClassError",
    "StrMappingToFieldMappingError",
    "YieldFieldsError",
    "dataclass_to_dict",
    "is_nullable_lt",
    "mapping_to_dataclass",
    "one_field",
    "parse_dataclass",
    "str_mapping_to_field_mapping",
    "yield_fields",
]
