from __future__ import annotations

import datetime as dt
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from enum import Enum, unique
from functools import cached_property, partial
from itertools import chain
from logging import Formatter, LogRecord
from math import isinf, isnan
from pathlib import Path
from re import Pattern, search
from typing import TYPE_CHECKING, Any, Literal, Self, assert_never, overload, override
from uuid import UUID

from orjson import (
    OPT_PASSTHROUGH_DATACLASS,
    OPT_PASSTHROUGH_DATETIME,
    OPT_SORT_KEYS,
    dumps,
    loads,
)

from utilities.concurrent import concurrent_map
from utilities.dataclasses import dataclass_to_dict
from utilities.functions import ensure_class, is_string_mapping
from utilities.iterables import (
    OneEmptyError,
    always_iterable,
    merge_sets,
    one,
    one_unique,
)
from utilities.logging import get_logging_level_number
from utilities.math import MAX_INT64, MIN_INT64
from utilities.types import (
    Dataclass,
    DateOrDateTime,
    LogLevel,
    MaybeIterable,
    PathLike,
    StrMapping,
)
from utilities.tzlocal import get_local_time_zone
from utilities.uuid import UUID_PATTERN
from utilities.version import Version, parse_version
from utilities.whenever import (
    parse_date,
    parse_plain_datetime,
    parse_time,
    parse_timedelta,
    parse_zoned_datetime,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)
from utilities.zoneinfo import ensure_time_zone

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet
    from logging import _FormatStyle

    from utilities.types import Parallelism


# serialize


@unique
class _Prefixes(Enum):
    dataclass = "dc"
    date = "d"
    datetime = "dt"
    enum = "e"
    exception_class = "exc"
    exception_instance = "exi"
    float_ = "fl"
    frozenset_ = "fr"
    list_ = "l"
    nan = "nan"
    none = "none"
    path = "p"
    pos_inf = "pos_inf"
    neg_inf = "neg_inf"
    set_ = "s"
    timedelta = "td"
    time = "tm"
    tuple_ = "tu"
    unserializable = "un"
    uuid = "uu"
    version = "v"


type _DataclassHook = Callable[[type[Dataclass], StrMapping], StrMapping]
type _ErrorMode = Literal["raise", "drop", "str"]


@dataclass(kw_only=True, slots=True)
class Unserializable:
    """An unserialiable object."""

    qualname: str
    repr: str
    str: str


def serialize(
    obj: Any,
    /,
    *,
    before: Callable[[Any], Any] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    dataclass_hook: _DataclassHook | None = None,
    dataclass_defaults: bool = False,
) -> bytes:
    """Serialize an object."""
    obj_use = _pre_process(
        obj,
        before=before,
        globalns=globalns,
        localns=localns,
        warn_name_errors=warn_name_errors,
        dataclass_hook=dataclass_hook,
        dataclass_defaults=dataclass_defaults,
    )
    return dumps(
        obj_use,
        option=OPT_PASSTHROUGH_DATACLASS | OPT_PASSTHROUGH_DATETIME | OPT_SORT_KEYS,
    )


def _pre_process(
    obj: Any,
    /,
    *,
    before: Callable[[Any], Any] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    dataclass_hook: _DataclassHook | None = None,
    dataclass_defaults: bool = False,
    error: _ErrorMode = "raise",
) -> Any:
    if before is not None:
        obj = before(obj)
    pre = partial(
        _pre_process,
        before=before,
        globalns=globalns,
        localns=localns,
        warn_name_errors=warn_name_errors,
        dataclass_hook=dataclass_hook,
        dataclass_defaults=dataclass_defaults,
        error=error,
    )
    match obj:
        # singletons
        case None:
            return f"[{_Prefixes.none.value}]"
        case dt.datetime() as datetime:
            return f"[{_Prefixes.datetime.value}]{serialize_datetime(datetime)}"
        case dt.date() as date:  # after datetime
            return f"[{_Prefixes.date.value}]{serialize_date(date)}"
        case dt.time() as time:
            return f"[{_Prefixes.time.value}]{serialize_time(time)}"
        case dt.timedelta() as timedelta:
            return f"[{_Prefixes.timedelta.value}]{serialize_timedelta(timedelta)}"
        case Exception() as error_:
            return {
                f"[{_Prefixes.exception_instance.value}|{type(error_).__qualname__}]": pre(
                    error_.args
                )
            }
        case float() as float_:
            if isinf(float_) or isnan(float_):
                return f"[{_Prefixes.float_.value}]{float_}"
            return float_
        case int() as int_:
            if MIN_INT64 <= int_ <= MAX_INT64:
                return int_
            raise _SerializeIntegerError(obj=int_)
        case UUID() as uuid:
            return f"[{_Prefixes.uuid.value}]{uuid}"
        case Path() as path:
            return f"[{_Prefixes.path.value}]{path!s}"
        case str() as str_:
            return str_
        case type() as error_cls if issubclass(error_cls, Exception):
            return f"[{_Prefixes.exception_class.value}|{error_cls.__qualname__}]"
        case Version() as version:
            return f"[{_Prefixes.version.value}]{version!s}"
        # contains
        case Dataclass() as dataclass:
            asdict = dataclass_to_dict(
                dataclass,
                globalns=globalns,
                localns=localns,
                warn_name_errors=warn_name_errors,
                final=partial(_dataclass_final, hook=dataclass_hook),
                defaults=dataclass_defaults,
            )
            return pre(asdict)
        case Enum() as enum:
            return {
                f"[{_Prefixes.enum.value}|{type(enum).__qualname__}]": pre(enum.value)
            }
        case frozenset() as frozenset_:
            return _pre_process_container(
                frozenset_,
                frozenset,
                _Prefixes.frozenset_,
                before=before,
                globalns=globalns,
                localns=localns,
                warn_name_errors=warn_name_errors,
                dataclass_hook=dataclass_hook,
            )
        case list() as list_:
            return _pre_process_container(
                list_,
                list,
                _Prefixes.list_,
                before=before,
                globalns=globalns,
                localns=localns,
                warn_name_errors=warn_name_errors,
                dataclass_hook=dataclass_hook,
            )
        case Mapping() as mapping:
            return {k: pre(v) for k, v in mapping.items()}
        case set() as set_:
            return _pre_process_container(
                set_,
                set,
                _Prefixes.set_,
                before=before,
                globalns=globalns,
                localns=localns,
                warn_name_errors=warn_name_errors,
                dataclass_hook=dataclass_hook,
            )
        case tuple() as tuple_:
            return _pre_process_container(
                tuple_,
                tuple,
                _Prefixes.tuple_,
                before=before,
                globalns=globalns,
                localns=localns,
                warn_name_errors=warn_name_errors,
                dataclass_hook=dataclass_hook,
            )
        # other
        case _:
            unserializable = Unserializable(
                qualname=type(obj).__qualname__, repr=repr(obj), str=str(obj)
            )
            return pre(unserializable)


def _pre_process_container(
    obj: Any,
    cls: type[frozenset | list | set | tuple],
    prefix: _Prefixes,
    /,
    *,
    before: Callable[[Any], Any] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    dataclass_hook: _DataclassHook | None = None,
    dataclass_include_defaults: bool = False,
) -> Any:
    values = [
        _pre_process(
            o,
            before=before,
            globalns=globalns,
            localns=localns,
            warn_name_errors=warn_name_errors,
            dataclass_hook=dataclass_hook,
            dataclass_defaults=dataclass_include_defaults,
        )
        for o in obj
    ]
    if issubclass(cls, list) and issubclass(list, type(obj)):
        return values
    if issubclass(cls, type(obj)):
        key = f"[{prefix.value}]"
    else:
        key = f"[{prefix.value}|{type(obj).__qualname__}]"
    return {key: values}


def _dataclass_final(
    cls: type[Dataclass], mapping: StrMapping, /, *, hook: _DataclassHook | None = None
) -> StrMapping:
    if hook is not None:
        mapping = hook(cls, mapping)
    return {f"[{_Prefixes.dataclass.value}|{cls.__qualname__}]": mapping}


@dataclass(kw_only=True, slots=True)
class SerializeError(Exception):
    obj: Any


@dataclass(kw_only=True, slots=True)
class _SerializeIntegerError(SerializeError):
    @override
    def __str__(self) -> str:
        return f"Integer {self.obj} is out of range"


# deserialize


def deserialize(
    data: bytes,
    /,
    *,
    dataclass_hook: _DataclassHook | None = None,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    """Deserialize an object."""
    return _object_hook(
        loads(data),
        data=data,
        dataclass_hook=dataclass_hook,
        objects=objects,
        redirects=redirects,
    )


_NONE_PATTERN = re.compile(r"^\[" + _Prefixes.none.value + r"\]$")
_LOCAL_DATETIME_PATTERN = re.compile(
    r"^\["
    + _Prefixes.datetime.value
    + r"\](\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)$"
)
_UUID_PATTERN = re.compile(r"^\[" + _Prefixes.uuid.value + r"\](" + UUID_PATTERN + ")$")
_ZONED_DATETIME_PATTERN = re.compile(
    r"^\["
    + _Prefixes.datetime.value
    + r"\](\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?[\+\-]\d{2}:\d{2}(?::\d{2})?\[(?!(?:dt\.)).+?\])$"
)


def _make_unit_pattern(prefix: _Prefixes, /) -> Pattern[str]:
    return re.compile(r"^\[" + prefix.value + r"\](.+)$")


(
    _DATE_PATTERN,
    _FLOAT_PATTERN,
    _PATH_PATTERN,
    _TIME_PATTERN,
    _TIMEDELTA_PATTERN,
    _VERSION_PATTERN,
) = map(
    _make_unit_pattern,
    [
        _Prefixes.date,
        _Prefixes.float_,
        _Prefixes.path,
        _Prefixes.time,
        _Prefixes.timedelta,
        _Prefixes.version,
    ],
)


def _make_container_pattern(prefix: _Prefixes, /) -> Pattern[str]:
    return re.compile(r"^\[" + prefix.value + r"(?:\|(.+))?\]$")


(
    _DATACLASS_PATTERN,
    _ENUM_PATTERN,
    _EXCEPTION_CLASS_PATTERN,
    _EXCEPTION_INSTANCE_PATTERN,
    _FROZENSET_PATTERN,
    _LIST_PATTERN,
    _SET_PATTERN,
    _TUPLE_PATTERN,
) = map(
    _make_container_pattern,
    [
        _Prefixes.dataclass,
        _Prefixes.enum,
        _Prefixes.exception_class,
        _Prefixes.exception_instance,
        _Prefixes.frozenset_,
        _Prefixes.list_,
        _Prefixes.set_,
        _Prefixes.tuple_,
    ],
)


def _object_hook(
    obj: bool | float | str | list[Any] | StrMapping | Dataclass | None,  # noqa: FBT001
    /,
    *,
    data: bytes,
    dataclass_hook: _DataclassHook | None = None,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    match obj:
        case bool() | int() | float() | Dataclass() | None:
            return obj
        case str() as text:
            if match := _NONE_PATTERN.search(text):
                return None
            if match := _DATE_PATTERN.search(text):
                return parse_date(match.group(1))
            if match := _FLOAT_PATTERN.search(text):
                return float(match.group(1))
            if match := _LOCAL_DATETIME_PATTERN.search(text):
                return parse_plain_datetime(match.group(1))
            if match := _PATH_PATTERN.search(text):
                return Path(match.group(1))
            if match := _TIME_PATTERN.search(text):
                return parse_time(match.group(1))
            if match := _TIMEDELTA_PATTERN.search(text):
                return parse_timedelta(match.group(1))
            if match := _UUID_PATTERN.search(text):
                return UUID(match.group(1))
            if match := _VERSION_PATTERN.search(text):
                return parse_version(match.group(1))
            if match := _ZONED_DATETIME_PATTERN.search(text):
                return parse_zoned_datetime(match.group(1))
            if (
                exc_class := _object_hook_exception_class(
                    text, data=data, objects=objects, redirects=redirects
                )
            ) is not None:
                return exc_class
            return text
        case list() as list_:
            return [
                _object_hook(
                    i,
                    data=data,
                    dataclass_hook=dataclass_hook,
                    objects=objects,
                    redirects=redirects,
                )
                for i in list_
            ]
        case Mapping() as mapping:
            if len(mapping) == 1:
                key, value = one(mapping.items())
                for cls, pattern in [
                    (frozenset, _FROZENSET_PATTERN),
                    (list, _LIST_PATTERN),
                    (set, _SET_PATTERN),
                    (tuple, _TUPLE_PATTERN),
                ]:
                    if (
                        container := _object_hook_container(
                            key,
                            value,
                            cls,
                            pattern,
                            data=data,
                            dataclass_hook=dataclass_hook,
                            objects=objects,
                            redirects=redirects,
                        )
                    ) is not None:
                        return container
                if (
                    is_string_mapping(value)
                    and (
                        dataclass := _object_hook_dataclass(
                            key,
                            value,
                            data=data,
                            hook=dataclass_hook,
                            objects=objects,
                            redirects=redirects,
                        )
                    )
                    is not None
                ):
                    return dataclass
                if (
                    enum := _object_hook_enum(
                        key, value, data=data, objects=objects, redirects=redirects
                    )
                ) is not None:
                    return enum
                if (
                    is_string_mapping(value)
                    and (
                        exc_instance := _object_hook_exception_instance(
                            key, value, data=data, objects=objects, redirects=redirects
                        )
                    )
                    is not None
                ):
                    return exc_instance
            return {
                k: _object_hook(
                    v,
                    data=data,
                    dataclass_hook=dataclass_hook,
                    objects=objects,
                    redirects=redirects,
                )
                for k, v in mapping.items()
            }
        case _ as never:
            assert_never(never)


def _object_hook_container(
    key: str,
    value: Any,
    cls: type[Any],
    pattern: Pattern[str],
    /,
    *,
    data: bytes,
    dataclass_hook: _DataclassHook | None = None,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    if not (match := pattern.search(key)):
        return None
    if (qualname := match.group(1)) is None:
        cls_use = cls
    else:
        cls_use = _object_hook_get_object(
            qualname, data=data, objects=objects, redirects=redirects
        )
    return cls_use(
        _object_hook(
            v,
            data=data,
            dataclass_hook=dataclass_hook,
            objects=objects,
            redirects=redirects,
        )
        for v in value
    )


def _object_hook_dataclass(
    key: str,
    value: StrMapping,
    /,
    *,
    data: bytes,
    hook: _DataclassHook | None = None,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    if not (match := _DATACLASS_PATTERN.search(key)):
        return None
    cls = _object_hook_get_object(
        match.group(1), data=data, objects=objects, redirects=redirects
    )
    if hook is not None:
        value = hook(cls, value)
    items = {
        k: _object_hook(
            v, data=data, dataclass_hook=hook, objects=objects, redirects=redirects
        )
        for k, v in value.items()
    }
    return cls(**items)


def _object_hook_enum(
    key: str,
    value: Any,
    /,
    *,
    data: bytes,
    dataclass_hook: _DataclassHook | None = None,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    if not (match := _ENUM_PATTERN.search(key)):
        return None
    cls: type[Enum] = _object_hook_get_object(
        match.group(1), data=data, objects=objects, redirects=redirects
    )
    value_use = _object_hook(
        value,
        data=data,
        dataclass_hook=dataclass_hook,
        objects=objects,
        redirects=redirects,
    )
    return one(i for i in cls if i.value == value_use)


def _object_hook_exception_class(
    qualname: str,
    /,
    *,
    data: bytes = b"",
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> type[Exception] | None:
    if not (match := _EXCEPTION_CLASS_PATTERN.search(qualname)):
        return None
    return _object_hook_get_object(
        match.group(1), data=data, objects=objects, redirects=redirects
    )


def _object_hook_exception_instance(
    key: str,
    value: StrMapping,
    /,
    *,
    data: bytes = b"",
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Exception | None:
    if not (match := _EXCEPTION_INSTANCE_PATTERN.search(key)):
        return None
    cls = _object_hook_get_object(
        match.group(1), data=data, objects=objects, redirects=redirects
    )
    items = _object_hook(value, data=data, objects=objects, redirects=redirects)
    return cls(*items)


def _object_hook_get_object(
    qualname: str,
    /,
    *,
    data: bytes = b"",
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> type[Any]:
    if qualname == Unserializable.__qualname__:
        return Unserializable
    if (objects is None) and (redirects is None):
        raise _DeserializeNoObjectsError(data=data, qualname=qualname)
    if objects is not None:
        with suppress(OneEmptyError):
            return one(o for o in objects if o.__qualname__ == qualname)
    if redirects:
        with suppress(KeyError):
            return redirects[qualname]
    raise _DeserializeObjectNotFoundError(data=data, qualname=qualname)


@dataclass(kw_only=True, slots=True)
class DeserializeError(Exception):
    data: bytes
    qualname: str


@dataclass(kw_only=True, slots=True)
class _DeserializeNoObjectsError(DeserializeError):
    @override
    def __str__(self) -> str:
        return f"Objects required to deserialize {self.qualname!r} from {self.data!r}"


@dataclass(kw_only=True, slots=True)
class _DeserializeObjectNotFoundError(DeserializeError):
    @override
    def __str__(self) -> str:
        return (
            f"Unable to find object to deserialize {self.qualname!r} from {self.data!r}"
        )


# logging


_LOG_RECORD_DEFAULT_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "getMessage",
    "levelname",
    "levelno",
    "lineno",
    "message",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "taskName",
    "thread",
    "threadName",
}


class OrjsonFormatter(Formatter):
    """Formatter for JSON logs."""

    @override
    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: _FormatStyle = "%",
        validate: bool = True,
        /,
        *,
        defaults: StrMapping | None = None,
        before: Callable[[Any], Any] | None = None,
        globalns: StrMapping | None = None,
        localns: StrMapping | None = None,
        warn_name_errors: bool = False,
        dataclass_hook: _DataclassHook | None = None,
        dataclass_defaults: bool = False,
    ) -> None:
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        self._before = before
        self._globalns = globalns
        self._localns = localns
        self._warn_name_errors = warn_name_errors
        self._dataclass_hook = dataclass_hook
        self._dataclass_defaults = dataclass_defaults

    @override
    def format(self, record: LogRecord) -> str:
        from utilities.tzlocal import get_local_time_zone

        extra = {
            k: v
            for k, v in record.__dict__.items()
            if (k not in _LOG_RECORD_DEFAULT_ATTRS) and (not k.startswith("_"))
        }
        log_record = OrjsonLogRecord(
            name=record.name,
            level=record.levelno,
            path_name=Path(record.pathname),
            line_num=record.lineno,
            message=record.getMessage(),
            datetime=dt.datetime.fromtimestamp(
                record.created, tz=get_local_time_zone()
            ),
            func_name=record.funcName,
            extra=extra if len(extra) >= 1 else None,
        )
        return serialize(
            log_record,
            before=self._before,
            globalns=self._globalns,
            localns=self._localns,
            warn_name_errors=self._warn_name_errors,
            dataclass_hook=self._dataclass_hook,
            dataclass_defaults=self._dataclass_defaults,
        ).decode()


def get_log_records(
    path: PathLike,
    /,
    *,
    parallelism: Parallelism = "processes",
    dataclass_hook: _DataclassHook | None = None,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> GetLogRecordsOutput:
    """Get the log records under a directory."""
    path = Path(path)
    files = [p for p in path.iterdir() if p.is_file()]
    func = partial(
        _get_log_records_one,
        dataclass_hook=dataclass_hook,
        objects=objects,
        redirects=redirects,
    )
    try:
        from utilities.pqdm import pqdm_map
    except ModuleNotFoundError:  # pragma: no cover
        outputs = concurrent_map(func, files, parallelism=parallelism)
    else:
        outputs = pqdm_map(func, files, parallelism=parallelism)
    records = sorted(
        chain.from_iterable(o.records for o in outputs), key=lambda r: r.datetime
    )
    for i, record in enumerate(records):
        record.index = i
    return GetLogRecordsOutput(
        path=path,
        files=files,
        num_files=len(outputs),
        num_files_ok=sum(o.file_ok for o in outputs),
        num_files_error=sum(not o.file_ok for o in outputs),
        num_lines=sum(o.num_lines for o in outputs),
        num_lines_ok=sum(o.num_lines_ok for o in outputs),
        num_lines_blank=sum(o.num_lines_blank for o in outputs),
        num_lines_error=sum(o.num_lines_error for o in outputs),
        records=records,
        missing=merge_sets(*(o.missing for o in outputs)),
        other_errors=list(chain.from_iterable(o.other_errors for o in outputs)),
    )


@dataclass(kw_only=True)
class GetLogRecordsOutput:
    """A collection of outputs."""

    path: Path
    files: list[Path] = field(default_factory=list)
    num_files: int = 0
    num_files_ok: int = 0
    num_files_error: int = 0
    num_lines: int = 0
    num_lines_ok: int = 0
    num_lines_blank: int = 0
    num_lines_error: int = 0
    records: list[IndexedOrjsonLogRecord] = field(default_factory=list, repr=False)
    missing: AbstractSet[str] = field(default_factory=set)
    other_errors: list[Exception] = field(default_factory=list)

    @overload
    def __getitem__(self, item: int, /) -> OrjsonLogRecord: ...
    @overload
    def __getitem__(self, item: slice, /) -> Sequence[OrjsonLogRecord]: ...
    def __getitem__(
        self, item: int | slice, /
    ) -> OrjsonLogRecord | Sequence[OrjsonLogRecord]:
        return self.records[item]

    def __len__(self) -> int:
        return len(self.records)

    @cached_property
    def dataframe(self) -> Any:
        from polars import DataFrame, Object, String, UInt64

        from utilities.polars import zoned_datetime

        records = [
            replace(
                r,
                path_name=str(r.path_name),
                log_file=None if r.log_file is None else str(r.log_file),
            )
            for r in self.records
        ]
        if len(records) >= 1:
            time_zone = one_unique(ensure_time_zone(r.datetime) for r in records)
        else:
            time_zone = get_local_time_zone()
        return DataFrame(
            data=[dataclass_to_dict(r, recursive=False) for r in records],
            schema={
                "index": UInt64,
                "name": String,
                "message": String,
                "level": UInt64,
                "path_name": String,
                "line_num": UInt64,
                "datetime": zoned_datetime(time_zone=time_zone),
                "func_name": String,
                "stack_info": String,
                "extra": Object,
                "log_file": String,
                "log_file_line_num": UInt64,
            },
        )

    def filter(
        self,
        *,
        index: int | None = None,
        min_index: int | None = None,
        max_index: int | None = None,
        name: str | None = None,
        message: str | None = None,
        level: LogLevel | None = None,
        min_level: LogLevel | None = None,
        max_level: LogLevel | None = None,
        date_or_datetime: DateOrDateTime | None = None,
        min_date_or_datetime: DateOrDateTime | None = None,
        max_date_or_datetime: DateOrDateTime | None = None,
        func_name: bool | str | None = None,
        extra: bool | MaybeIterable[str] | None = None,
        log_file: bool | PathLike | None = None,
        log_file_line_num: bool | int | None = None,
        min_log_file_line_num: int | None = None,
        max_log_file_line_num: int | None = None,
    ) -> Self:
        records = self.records
        if index is not None:
            records = [r for r in records if r.index == index]
        if min_index is not None:
            records = [r for r in records if r.index >= min_index]
        if max_index is not None:
            records = [r for r in records if r.index <= max_index]
        if name is not None:
            records = [r for r in records if search(name, r.name)]
        if message is not None:
            records = [r for r in records if search(message, r.message)]
        if level is not None:
            records = [r for r in records if r.level == get_logging_level_number(level)]
        if min_level is not None:
            records = [
                r for r in records if r.level >= get_logging_level_number(min_level)
            ]
        if max_level is not None:
            records = [
                r for r in records if r.level <= get_logging_level_number(max_level)
            ]
        if level is not None:
            records = [r for r in records if r.level == get_logging_level_number(level)]
        if min_level is not None:
            records = [
                r for r in records if r.level >= get_logging_level_number(min_level)
            ]
        if max_level is not None:
            records = [
                r for r in records if r.level <= get_logging_level_number(max_level)
            ]
        if date_or_datetime is not None:
            match date_or_datetime:
                case dt.datetime() as datetime:
                    records = [r for r in records if r.datetime == datetime]
                case dt.date() as date:
                    records = [r for r in records if r.date == date]
                case _ as never:
                    assert_never(never)
        if min_date_or_datetime is not None:
            match min_date_or_datetime:
                case dt.datetime() as min_datetime:
                    records = [r for r in records if r.datetime >= min_datetime]
                case dt.date() as min_date:
                    records = [r for r in records if r.date >= min_date]
                case _ as never:
                    assert_never(never)
        if max_date_or_datetime is not None:
            match max_date_or_datetime:
                case dt.datetime() as max_datetime:
                    records = [r for r in records if r.datetime <= max_datetime]
                case dt.date() as max_date:
                    records = [r for r in records if r.date <= max_date]
                case _ as never:
                    assert_never(never)
        if func_name is not None:
            match func_name:
                case bool() as has_func_name:
                    records = [
                        r for r in records if (r.func_name is not None) is has_func_name
                    ]
                case str():
                    records = [
                        r
                        for r in records
                        if (r.func_name is not None) and search(func_name, r.func_name)
                    ]
                case _ as never:
                    assert_never(never)
        if extra is not None:
            match extra:
                case bool() as has_extra:
                    records = [r for r in records if (r.extra is not None) is has_extra]
                case str() | Iterable() as keys:
                    records = [
                        r
                        for r in records
                        if (r.extra is not None)
                        and set(r.extra).issuperset(always_iterable(keys))
                    ]
                case _ as never:
                    assert_never(never)
        if log_file is not None:
            match log_file:
                case bool() as has_log_file:
                    records = [
                        r for r in records if (r.log_file is not None) is has_log_file
                    ]
                case Path() | str():
                    records = [
                        r
                        for r in records
                        if (r.log_file is not None)
                        and search(str(log_file), str(r.log_file))
                    ]
                case _ as never:
                    assert_never(never)
        if log_file_line_num is not None:
            match log_file_line_num:
                case bool() as has_log_file_line_num:
                    records = [
                        r
                        for r in records
                        if (r.log_file_line_num is not None) is has_log_file_line_num
                    ]
                case int():
                    records = [
                        r for r in records if r.log_file_line_num == log_file_line_num
                    ]
                case _ as never:
                    assert_never(never)
        if min_log_file_line_num is not None:
            records = [
                r
                for r in records
                if (r.log_file_line_num is not None)
                and (r.log_file_line_num >= min_log_file_line_num)
            ]
        if max_log_file_line_num is not None:
            records = [
                r
                for r in records
                if (r.log_file_line_num is not None)
                and (r.log_file_line_num >= max_log_file_line_num)
            ]
        return replace(self, records=records)

    @property
    def frac_files_ok(self) -> float:
        return self.num_files_ok / self.num_files

    @property
    def frac_files_error(self) -> float:
        return self.num_files_error / self.num_files

    @property
    def frac_lines_ok(self) -> float:
        return self.num_lines_ok / self.num_lines

    @property
    def frac_lines_blank(self) -> float:
        return self.num_lines_blank / self.num_lines

    @property
    def frac_lines_error(self) -> float:
        return self.num_lines_error / self.num_lines


@dataclass(order=True, kw_only=True)
class OrjsonLogRecord:
    """The log record as a dataclass."""

    name: str
    message: str
    level: int
    path_name: Path
    line_num: int
    datetime: dt.datetime
    func_name: str | None = None
    stack_info: str | None = None
    extra: StrMapping | None = None
    log_file: Path | None = None
    log_file_line_num: int | None = None

    @cached_property
    def date(self) -> dt.date:
        return self.datetime.date()


@dataclass(order=True, kw_only=True)
class IndexedOrjsonLogRecord(OrjsonLogRecord):
    """An indexed log record."""

    index: int


def _get_log_records_one(
    path: Path,
    /,
    *,
    dataclass_hook: _DataclassHook | None = None,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> _GetLogRecordsOneOutput:
    path = Path(path)
    try:
        with path.open() as fh:
            lines = fh.readlines()
    except UnicodeDecodeError as error:  # skipif-ci-and-windows
        return _GetLogRecordsOneOutput(path=path, file_ok=False, other_errors=[error])
    num_lines_blank, num_lines_error = 0, 0
    missing: set[str] = set()
    records: list[IndexedOrjsonLogRecord] = []
    errors: list[Exception] = []
    objects_use = {OrjsonLogRecord} | (set() if objects is None else objects)
    for i, line in enumerate(lines, start=1):
        if line.strip("\n") == "":
            num_lines_blank += 1
        else:
            try:
                result = deserialize(
                    line.encode(),
                    dataclass_hook=dataclass_hook,
                    objects=objects_use,
                    redirects=redirects,
                )
                record = ensure_class(result, OrjsonLogRecord)
            except (
                _DeserializeNoObjectsError,
                _DeserializeObjectNotFoundError,
            ) as error:
                num_lines_error += 1
                missing.add(error.qualname)
            except Exception as error:  # noqa: BLE001
                num_lines_error += 1
                errors.append(error)
            else:
                record.log_file = path
                record.log_file_line_num = i
                indexed = IndexedOrjsonLogRecord(
                    index=len(records),
                    name=record.name,
                    message=record.message,
                    level=record.level,
                    path_name=record.path_name,
                    line_num=record.line_num,
                    datetime=record.datetime,
                    func_name=record.func_name,
                    stack_info=record.stack_info,
                    extra=record.extra,
                    log_file=record.log_file,
                    log_file_line_num=record.log_file_line_num,
                )
                records.append(indexed)
    return _GetLogRecordsOneOutput(
        path=path,
        file_ok=True,
        num_lines=len(lines),
        num_lines_ok=len(records),
        num_lines_blank=num_lines_blank,
        num_lines_error=num_lines_error,
        records=sorted(records, key=lambda r: r.datetime),
        missing=missing,
        other_errors=errors,
    )


@dataclass(kw_only=True, slots=True)
class _GetLogRecordsOneOutput:
    path: Path
    file_ok: bool = False
    num_lines: int = 0
    num_lines_ok: int = 0
    num_lines_blank: int = 0
    num_lines_error: int = 0
    records: list[IndexedOrjsonLogRecord] = field(default_factory=list, repr=False)
    missing: set[str] = field(default_factory=set)
    other_errors: list[Exception] = field(default_factory=list, repr=False)


__all__ = [
    "DeserializeError",
    "GetLogRecordsOutput",
    "OrjsonFormatter",
    "OrjsonLogRecord",
    "SerializeError",
    "deserialize",
    "get_log_records",
    "serialize",
]
