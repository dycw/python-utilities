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
from zoneinfo import ZoneInfo

from orjson import (
    OPT_PASSTHROUGH_DATACLASS,
    OPT_PASSTHROUGH_DATETIME,
    OPT_SORT_KEYS,
    dumps,
    loads,
)
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    MonthDay,
    PlainDateTime,
    Time,
    TimeDelta,
    YearMonth,
    ZonedDateTime,
)

from utilities.concurrent import concurrent_map
from utilities.dataclasses import dataclass_to_dict
from utilities.functions import ensure_class, is_string_mapping
from utilities.gzip import read_binary
from utilities.iterables import (
    OneEmptyError,
    always_iterable,
    merge_sets,
    one,
    one_unique,
)
from utilities.json import write_formatted_json
from utilities.logging import get_logging_level_number
from utilities.math import MAX_INT64, MIN_INT64
from utilities.types import Dataclass, LogLevel, MaybeIterable, PathLike, StrMapping
from utilities.tzlocal import LOCAL_TIME_ZONE
from utilities.version import Version, parse_version
from utilities.whenever import from_timestamp

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet
    from logging import _FormatStyle

    from utilities.types import Parallelism


# serialize


@unique
class _Prefixes(Enum):
    dataclass = "dc"
    date = "d"
    date_delta = "dd"
    date_time_delta = "D"
    enum = "e"
    exception_class = "Ex"
    exception_instance = "ex"
    float_ = "fl"
    frozenset_ = "fr"
    list_ = "l"
    month_day = "md"
    none = "none"
    path = "p"
    plain_date_time = "pd"
    py_date = "!d"
    py_plain_date_time = "!pd"
    py_time = "!ti"
    py_zoned_date_time = "!zd"
    set_ = "s"
    time = "ti"
    time_delta = "td"
    tuple_ = "tu"
    unserializable = "un"
    uuid = "uu"
    version = "v"
    year_month = "ym"
    zoned_date_time = "zd"


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
        case Date() as date:
            return f"[{_Prefixes.date.value}]{date}"
        case DateDelta() as date:
            return f"[{_Prefixes.date_delta.value}]{date}"
        case DateTimeDelta() as date:
            return f"[{_Prefixes.date_time_delta.value}]{date}"
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
        case MonthDay() as month_day:
            return f"[{_Prefixes.month_day.value}]{month_day!s}"
        case Path() as path:
            return f"[{_Prefixes.path.value}]{path!s}"
        case PlainDateTime() as datetime:
            return f"[{_Prefixes.plain_date_time.value}]{datetime}"
        case str() as str_:
            return str_
        case Time() as time:
            return f"[{_Prefixes.time.value}]{time}"
        case TimeDelta() as time_delta:
            return f"[{_Prefixes.time_delta.value}]{time_delta}"
        case type() as error_cls if issubclass(error_cls, Exception):
            return f"[{_Prefixes.exception_class.value}|{error_cls.__qualname__}]"
        case UUID() as uuid:
            return f"[{_Prefixes.uuid.value}]{uuid}"
        case Version() as version:
            return f"[{_Prefixes.version.value}]{version}"
        case YearMonth() as year_month:
            return f"[{_Prefixes.year_month.value}]{year_month}"
        case ZonedDateTime() as datetime:
            return f"[{_Prefixes.zoned_date_time.value}]{datetime}"
        case dt.datetime() as py_datetime:
            match py_datetime.tzinfo:
                case None:
                    datetime = PlainDateTime.from_py_datetime(py_datetime)
                    return f"[{_Prefixes.py_plain_date_time.value}]{datetime}"
                case ZoneInfo():
                    datetime = ZonedDateTime.from_py_datetime(py_datetime)
                    return f"[{_Prefixes.py_zoned_date_time.value}]{datetime}"
                case _:  # pragma: no cover
                    raise NotImplementedError
        case dt.date() as py_date:
            date = Date.from_py_date(py_date)
            return f"[{_Prefixes.py_date.value}]{date}"
        case dt.time() as py_time:
            time = Time.from_py_time(py_time)
            return f"[{_Prefixes.py_time.value}]{time}"
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
    return None


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


(
    _DATE_PATTERN,
    _DATE_DELTA_PATTERN,
    _DATE_TIME_DELTA_PATTERN,
    _FLOAT_PATTERN,
    _MONTH_DAY_PATTERN,
    _NONE_PATTERN,
    _PATH_PATTERN,
    _PLAIN_DATE_TIME_PATTERN,
    _PY_DATE_PATTERN,
    _PY_PLAIN_DATE_TIME_PATTERN,
    _PY_TIME_PATTERN,
    _PY_ZONED_DATE_TIME_PATTERN,
    _TIME_PATTERN,
    _TIME_DELTA_PATTERN,
    _UUID_PATTERN,
    _VERSION_PATTERN,
    _YEAR_MONTH_PATTERN,
    _ZONED_DATE_TIME_PATTERN,
) = [
    re.compile(r"^\[" + p.value + r"\](" + ".*" + ")$")
    for p in [
        _Prefixes.date,
        _Prefixes.date_delta,
        _Prefixes.date_time_delta,
        _Prefixes.float_,
        _Prefixes.month_day,
        _Prefixes.none,
        _Prefixes.path,
        _Prefixes.plain_date_time,
        _Prefixes.py_date,
        _Prefixes.py_plain_date_time,
        _Prefixes.py_time,
        _Prefixes.py_zoned_date_time,
        _Prefixes.time,
        _Prefixes.time_delta,
        _Prefixes.uuid,
        _Prefixes.version,
        _Prefixes.year_month,
        _Prefixes.zoned_date_time,
    ]
]


(
    _DATACLASS_PATTERN,
    _ENUM_PATTERN,
    _EXCEPTION_CLASS_PATTERN,
    _EXCEPTION_INSTANCE_PATTERN,
    _FROZENSET_PATTERN,
    _LIST_PATTERN,
    _SET_PATTERN,
    _TUPLE_PATTERN,
) = [
    re.compile(r"^\[" + p.value + r"(?:\|(.+))?\]$")
    for p in [
        _Prefixes.dataclass,
        _Prefixes.enum,
        _Prefixes.exception_class,
        _Prefixes.exception_instance,
        _Prefixes.frozenset_,
        _Prefixes.list_,
        _Prefixes.set_,
        _Prefixes.tuple_,
    ]
]


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
                return Date.parse_common_iso(match.group(1))
            if match := _DATE_DELTA_PATTERN.search(text):
                return DateDelta.parse_common_iso(match.group(1))
            if match := _DATE_TIME_DELTA_PATTERN.search(text):
                return DateTimeDelta.parse_common_iso(match.group(1))
            if match := _FLOAT_PATTERN.search(text):
                return float(match.group(1))
            if match := _MONTH_DAY_PATTERN.search(text):
                return MonthDay.parse_common_iso(match.group(1))
            if match := _PATH_PATTERN.search(text):
                return Path(match.group(1))
            if match := _PLAIN_DATE_TIME_PATTERN.search(text):
                return PlainDateTime.parse_common_iso(match.group(1))
            if match := _PY_DATE_PATTERN.search(text):
                return Date.parse_common_iso(match.group(1)).py_date()
            if match := _PY_PLAIN_DATE_TIME_PATTERN.search(text):
                return PlainDateTime.parse_common_iso(match.group(1)).py_datetime()
            if match := _PY_TIME_PATTERN.search(text):
                return Time.parse_common_iso(match.group(1)).py_time()
            if match := _PY_ZONED_DATE_TIME_PATTERN.search(text):
                return ZonedDateTime.parse_common_iso(match.group(1)).py_datetime()
            if match := _TIME_PATTERN.search(text):
                return Time.parse_common_iso(match.group(1))
            if match := _TIME_DELTA_PATTERN.search(text):
                return TimeDelta.parse_common_iso(match.group(1))
            if match := _UUID_PATTERN.search(text):
                return UUID(match.group(1))
            if match := _VERSION_PATTERN.search(text):
                return parse_version(match.group(1))
            if match := _YEAR_MONTH_PATTERN.search(text):
                return YearMonth.parse_common_iso(match.group(1))
            if match := _ZONED_DATE_TIME_PATTERN.search(text):
                return ZonedDateTime.parse_common_iso(match.group(1))
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
            datetime=from_timestamp(record.created, time_zone=LOCAL_TIME_ZONE),
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
        from polars import DataFrame, Datetime, Object, String, UInt64

        records = [
            replace(
                r,
                path_name=str(r.path_name),
                log_file=None if r.log_file is None else str(r.log_file),
            )
            for r in self.records
        ]
        if len(records) >= 1:
            time_zone = one_unique(ZoneInfo(r.datetime.tz) for r in records)
        else:
            time_zone = LOCAL_TIME_ZONE
        return DataFrame(
            data=[
                dataclass_to_dict(
                    replace(r, datetime=r.datetime.py_datetime()), recursive=False
                )
                for r in records
            ],
            schema={
                "index": UInt64,
                "name": String,
                "message": String,
                "level": UInt64,
                "path_name": String,
                "line_num": UInt64,
                "datetime": Datetime(time_zone=time_zone),
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
        date: Date | None = None,
        min_date: Date | None = None,
        max_date: Date | None = None,
        datetime: ZonedDateTime | None = None,
        min_datetime: ZonedDateTime | None = None,
        max_datetime: ZonedDateTime | None = None,
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
        if date is not None:
            records = [r for r in records if r.date == date]
        if min_date is not None:
            records = [r for r in records if r.date >= min_date]
        if max_date is not None:
            records = [r for r in records if r.date <= max_date]
        if datetime is not None:
            records = [r for r in records if r.datetime == datetime]
        if min_datetime is not None:
            records = [r for r in records if r.datetime >= min_datetime]
        if max_datetime is not None:
            records = [r for r in records if r.datetime <= max_datetime]
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
                and (r.log_file_line_num <= max_log_file_line_num)
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
    datetime: ZonedDateTime
    func_name: str | None = None
    stack_info: str | None = None
    extra: StrMapping | None = None
    log_file: Path | None = None
    log_file_line_num: int | None = None

    @cached_property
    def date(self) -> Date:
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
        lines = path.read_text().splitlines()
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


# read/write


def read_object(
    path: PathLike,
    /,
    *,
    decompress: bool = False,
    dataclass_hook: _DataclassHook | None = None,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    """Read an object from disk."""
    data = read_binary(path, decompress=decompress)
    return deserialize(
        data, dataclass_hook=dataclass_hook, objects=objects, redirects=redirects
    )


def write_object(
    obj: Any,
    path: PathLike,
    /,
    *,
    before: Callable[[Any], Any] | None = None,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    dataclass_hook: _DataclassHook | None = None,
    dataclass_defaults: bool = False,
    compress: bool = False,
    overwrite: bool = False,
) -> None:
    """Write an object to disk."""
    data = serialize(
        obj,
        before=before,
        globalns=globalns,
        localns=localns,
        warn_name_errors=warn_name_errors,
        dataclass_hook=dataclass_hook,
        dataclass_defaults=dataclass_defaults,
    )
    write_formatted_json(data, path, compress=compress, overwrite=overwrite)


__all__ = [
    "DeserializeError",
    "GetLogRecordsOutput",
    "OrjsonFormatter",
    "OrjsonLogRecord",
    "SerializeError",
    "deserialize",
    "get_log_records",
    "read_object",
    "serialize",
    "write_object",
]
