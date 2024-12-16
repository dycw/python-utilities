from __future__ import annotations

import datetime as dt
import re
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum, unique
from functools import partial, reduce
from itertools import chain
from logging import Formatter, LogRecord
from math import isinf, isnan
from operator import or_
from pathlib import Path
from re import Pattern
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, assert_never
from uuid import UUID

from orjson import (
    OPT_PASSTHROUGH_DATACLASS,
    OPT_PASSTHROUGH_DATETIME,
    OPT_SORT_KEYS,
    dumps,
    loads,
)
from typing_extensions import override

from utilities.concurrent import concurrent_map
from utilities.dataclasses import asdict_without_defaults
from utilities.iterables import OneEmptyError, one
from utilities.math import MAX_INT64, MIN_INT64
from utilities.types import Dataclass, PathLike, StrMapping, ensure_class
from utilities.uuid import UUID_PATTERN
from utilities.whenever import (
    parse_date,
    parse_local_datetime,
    parse_time,
    parse_timedelta,
    parse_zoned_datetime,
    serialize_date,
    serialize_local_datetime,
    serialize_time,
    serialize_timedelta,
    serialize_zoned_datetime,
)

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet
    from logging import _FormatStyle

    from utilities.concurrent import Parallelism


# serialize


@unique
class _Prefixes(Enum):
    dataclass = "dc"
    date = "d"
    datetime = "dt"
    enum = "e"
    float_ = "fl"
    frozenset_ = "fr"
    list_ = "l"
    nan = "nan"
    path = "p"
    pos_inf = "pos_inf"
    neg_inf = "neg_inf"
    set_ = "s"
    timedelta = "td"
    time = "tm"
    tuple_ = "tu"
    unserializable = "un"
    uuid = "uu"


_DataclassFinalHook: TypeAlias = Callable[[type[Dataclass], StrMapping], StrMapping]
_ErrorMode: TypeAlias = Literal["raise", "drop", "str"]


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
    dataclass_final_hook: _DataclassFinalHook | None = None,
) -> bytes:
    """Serialize an object."""
    obj_use = _pre_process(
        obj, before=before, dataclass_final_hook=dataclass_final_hook
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
    dataclass_final_hook: _DataclassFinalHook | None = None,
    error: _ErrorMode = "raise",
) -> Any:
    if before is not None:
        obj = before(obj)
    pre = partial(
        _pre_process,
        before=before,
        dataclass_final_hook=dataclass_final_hook,
        error=error,
    )
    match obj:
        # singletons
        case dt.datetime():
            if obj.tzinfo is None:
                ser = serialize_local_datetime(obj)
            elif obj.tzinfo is dt.UTC:
                ser = serialize_zoned_datetime(obj).replace("UTC", "dt.UTC")
            else:
                ser = serialize_zoned_datetime(obj)
            return f"[{_Prefixes.datetime.value}]{ser}"
        case dt.date():  # after datetime
            ser = serialize_date(obj)
            return f"[{_Prefixes.date.value}]{ser}"
        case dt.time():
            ser = serialize_time(obj)
            return f"[{_Prefixes.time.value}]{ser}"
        case dt.timedelta():
            ser = serialize_timedelta(obj)
            return f"[{_Prefixes.timedelta.value}]{ser}"
        case float():
            if isinf(obj) or isnan(obj):
                return f"[{_Prefixes.float_.value}]{obj}"
            return obj
        case int():
            if MIN_INT64 <= obj <= MAX_INT64:
                return obj
            raise _SerializeIntegerError(obj=obj)
        case UUID():
            return f"[{_Prefixes.uuid.value}]{obj}"
        case Path():
            ser = str(obj)
            return f"[{_Prefixes.path.value}]{ser}"
        case str():
            return obj
        # contains
        case Dataclass():
            obj_as_dict = asdict_without_defaults(
                obj, final=partial(_dataclass_final, hook=dataclass_final_hook)
            )
            return pre(obj_as_dict)
        case dict():
            return {k: pre(v) for k, v in obj.items()}
        case Enum():
            return {
                f"[{_Prefixes.enum.value}|{type(obj).__qualname__}]": pre(obj.value)
            }
        case frozenset():
            return _pre_process_container(
                obj,
                frozenset,
                _Prefixes.frozenset_,
                before=before,
                dataclass_final_hook=dataclass_final_hook,
            )
        case list():
            return _pre_process_container(
                obj,
                list,
                _Prefixes.list_,
                before=before,
                dataclass_final_hook=dataclass_final_hook,
            )
        case set():
            return _pre_process_container(
                obj,
                set,
                _Prefixes.set_,
                before=before,
                dataclass_final_hook=dataclass_final_hook,
            )
        case tuple():
            return _pre_process_container(
                obj,
                tuple,
                _Prefixes.tuple_,
                before=before,
                dataclass_final_hook=dataclass_final_hook,
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
    dataclass_final_hook: _DataclassFinalHook | None = None,
) -> Any:
    values = [
        _pre_process(o, before=before, dataclass_final_hook=dataclass_final_hook)
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
    cls: type[Dataclass],
    mapping: StrMapping,
    /,
    *,
    hook: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
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
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    """Deserialize an object."""
    return _object_hook(loads(data), data=data, objects=objects, redirects=redirects)


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
_ZONED_DATETIME_ALTERNATIVE_PATTERN = re.compile(
    r"^\["
    + _Prefixes.datetime.value
    + r"\](\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\+00:00\[dt\.UTC\])$"
)


def _make_unit_pattern(prefix: _Prefixes, /) -> Pattern[str]:
    return re.compile(r"^\[" + prefix.value + r"\](.+)$")


_DATE_PATTERN, _FLOAT_PATTERN, _PATH_PATTERN, _TIME_PATTERN, _TIMEDELTA_PATTERN = map(
    _make_unit_pattern,
    [
        _Prefixes.date,
        _Prefixes.float_,
        _Prefixes.path,
        _Prefixes.time,
        _Prefixes.timedelta,
    ],
)


def _make_container_pattern(prefix: _Prefixes, /) -> Pattern[str]:
    return re.compile(r"^\[" + prefix.value + r"(?:\|(.+))?\]$")


(
    _DATACLASS_PATTERN,
    _ENUM_PATTERN,
    _FROZENSET_PATTERN,
    _LIST_PATTERN,
    _SET_PATTERN,
    _TUPLE_PATTERN,
) = map(
    _make_container_pattern,
    [
        _Prefixes.dataclass,
        _Prefixes.enum,
        _Prefixes.frozenset_,
        _Prefixes.list_,
        _Prefixes.set_,
        _Prefixes.tuple_,
    ],
)


def _object_hook(
    obj: bool | float | str | dict[str, Any] | list[Any] | Dataclass | None,  # noqa: FBT001
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    match obj:
        case bool() | int() | float() | Dataclass() | None:
            return obj
        case str():
            if match := _DATE_PATTERN.search(obj):
                return parse_date(match.group(1))
            if match := _FLOAT_PATTERN.search(obj):
                return float(match.group(1))
            if match := _LOCAL_DATETIME_PATTERN.search(obj):
                return parse_local_datetime(match.group(1))
            if match := _PATH_PATTERN.search(obj):
                return Path(match.group(1))
            if match := _TIME_PATTERN.search(obj):
                return parse_time(match.group(1))
            if match := _TIMEDELTA_PATTERN.search(obj):
                return parse_timedelta(match.group(1))
            if match := _UUID_PATTERN.search(obj):
                return UUID(match.group(1))
            if match := _ZONED_DATETIME_PATTERN.search(obj):
                return parse_zoned_datetime(match.group(1))
            if match := _ZONED_DATETIME_ALTERNATIVE_PATTERN.search(obj):
                return parse_zoned_datetime(
                    match.group(1).replace("dt.UTC", "UTC")
                ).replace(tzinfo=dt.UTC)
            return obj
        case list():
            return [
                _object_hook(o, data=data, objects=objects, redirects=redirects)
                for o in obj
            ]
        case dict():
            if len(obj) == 1:
                key, value = one(obj.items())
                for cls, pattern in [
                    (frozenset, _FROZENSET_PATTERN),
                    (list, _LIST_PATTERN),
                    (set, _SET_PATTERN),
                    (tuple, _TUPLE_PATTERN),
                ]:
                    result = _object_hook_container(
                        key,
                        value,
                        cls,
                        pattern,
                        data=data,
                        objects=objects,
                        redirects=redirects,
                    )
                    if result is not None:
                        return result
                result = _object_hook_dataclass(
                    key, value, data=data, objects=objects, redirects=redirects
                )
                if result is not None:
                    return result
                result = _object_hook_enum(
                    key, value, data=data, objects=objects, redirects=redirects
                )
                if result is not None:
                    return result
                return {
                    k: _object_hook(v, data=data, objects=objects, redirects=redirects)
                    for k, v in obj.items()
                }
            return {
                k: _object_hook(v, data=data, objects=objects, redirects=redirects)
                for k, v in obj.items()
            }
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


def _object_hook_container(
    key: str,
    value: Any,
    cls: type[Any],
    pattern: Pattern[str],
    /,
    *,
    data: bytes,
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
        _object_hook(v, data=data, objects=objects, redirects=redirects) for v in value
    )


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


def _object_hook_dataclass(
    key: str,
    value: Any,
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    if not (match := _DATACLASS_PATTERN.search(key)):
        return None
    cls = _object_hook_get_object(
        match.group(1), data=data, objects=objects, redirects=redirects
    )
    items = {
        k: _object_hook(v, data=data, objects=objects, redirects=redirects)
        for k, v in value.items()
    }
    return cls(**items)


def _object_hook_enum(
    key: str,
    value: Any,
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> Any:
    if not (match := _ENUM_PATTERN.search(key)):
        return None
    cls: type[Enum] = _object_hook_get_object(
        match.group(1), data=data, objects=objects, redirects=redirects
    )
    value_use = _object_hook(value, data=data, objects=objects, redirects=redirects)
    return one(i for i in cls if i.value == value_use)


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
        dataclass_final_hook: _DataclassFinalHook | None = None,
    ) -> None:
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        self._before = before
        self._dataclass_final_hook = dataclass_final_hook

    @override
    def format(self, record: LogRecord) -> str:
        from tzlocal import get_localzone

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
            datetime=dt.datetime.fromtimestamp(record.created, tz=get_localzone()),
            func_name=record.funcName,
            extra=extra if len(extra) >= 1 else None,
        )
        return serialize(
            log_record,
            before=self._before,
            dataclass_final_hook=self._dataclass_final_hook,
        ).decode()


@dataclass(kw_only=True, slots=True)
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


@dataclass(kw_only=True, slots=True)
class GetLogRecordsOutput:
    """A collection of outputs."""

    path: Path
    files: list[Path] = field(default_factory=list)
    num_lines: int = 0
    num_records: int = 0
    num_errors: int = 0
    records: list[OrjsonLogRecord] = field(default_factory=list, repr=False)
    missing: set[str] = field(default_factory=set)
    other_errors: list[Exception] = field(default_factory=list)

    @property
    def frac_success(self) -> float:
        return self.num_records / self.num_lines

    @property
    def frac_error(self) -> float:
        return self.num_errors / self.num_lines

    @property
    def num_files(self) -> int:
        return len(self.files)


def get_log_records(
    path: PathLike,
    /,
    *,
    parallelism: Parallelism = "processes",
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> GetLogRecordsOutput:
    """Get the log records under a directory."""
    path = Path(path)
    files = list(path.iterdir())
    func = partial(_get_log_records_one, objects=objects, redirects=redirects)
    try:
        from utilities.pqdm import pqdm_map
    except ModuleNotFoundError:  # pragma: no cover
        outputs = concurrent_map(func, files, parallelism=parallelism)
    else:
        outputs = pqdm_map(func, files, parallelism=parallelism)
    return GetLogRecordsOutput(
        path=path,
        files=files,
        records=sorted(
            chain.from_iterable(o.records for o in outputs), key=lambda r: r.datetime
        ),
        num_lines=sum(o.num_lines for o in outputs),
        num_records=sum(o.num_records for o in outputs),
        num_errors=sum(o.num_errors for o in outputs),
        missing=set(reduce(or_, (o.missing for o in outputs))),
        other_errors=list(chain.from_iterable(o.other_errors for o in outputs)),
    )


@dataclass(kw_only=True, slots=True)
class _GetLogRecordsOneOutput:
    path: Path
    num_lines: int = 0
    num_records: int = 0
    num_errors: int = 0
    records: list[OrjsonLogRecord] = field(default_factory=list, repr=False)
    missing: set[str] = field(default_factory=set)
    other_errors: list[Exception] = field(default_factory=list, repr=False)


def _get_log_records_one(
    path: Path,
    /,
    *,
    objects: AbstractSet[type[Any]] | None = None,
    redirects: Mapping[str, type[Any]] | None = None,
) -> _GetLogRecordsOneOutput:
    path = Path(path)
    with path.open() as fh:
        lines = fh.readlines()
    num_errors = 0
    missing: set[str] = set()
    records: list[OrjsonLogRecord] = []
    errors: list[Exception] = []
    objects_use = {OrjsonLogRecord} | (set() if objects is None else objects)
    for i, line in enumerate(lines, start=1):
        try:
            result = deserialize(
                line.encode(), objects=objects_use, redirects=redirects
            )
            record = ensure_class(result, OrjsonLogRecord)
        except (_DeserializeNoObjectsError, _DeserializeObjectNotFoundError) as error:
            num_errors += 1
            missing.add(error.qualname)
        except Exception as error:  # noqa: BLE001
            num_errors += 1
            errors.append(error)
        else:
            record.log_file = path
            record.log_file_line_num = i
            records.append(record)
    return _GetLogRecordsOneOutput(
        path=path,
        records=sorted(records, key=lambda r: r.datetime),
        num_lines=len(lines),
        num_records=len(records),
        num_errors=num_errors,
        missing=missing,
        other_errors=errors,
    )


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
