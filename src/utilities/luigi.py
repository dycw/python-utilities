from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from contextlib import suppress
from enum import Enum
from pathlib import Path
from re import search
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast, overload

import luigi
from luigi import Parameter, PathParameter, Target, Task, TaskParameter
from luigi import build as _build
from luigi.task import flatten
from sqlalchemy.exc import DatabaseError
from typing_extensions import override

from utilities.datetime import (
    EPOCH_UTC,
    ensure_date,
    ensure_datetime,
    ensure_time,
    get_now,
    parse_date,
    parse_datetime,
    parse_time,
    round_to_next_weekday,
    round_to_prev_weekday,
    serialize_date,
    serialize_datetime,
    serialize_time,
)
from utilities.enum import ensure_enum, parse_enum
from utilities.iterables import one
from utilities.sentinel import sentinel
from utilities.text import ensure_str, join_strs, split_str
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from luigi.interface import LuigiRunResult
    from semver import Version
    from sqlalchemy import Engine, Select

    from utilities.logging import LogLevel
    from utilities.types import IterableStrs, PathLike

# parameters


class DateParameter(luigi.DateParameter):
    """A parameter which takes the value of a `dt.date`."""

    @override
    def normalize(self, value: dt.date | str) -> dt.date:
        return ensure_date(value)

    @override
    def parse(self, s: str) -> dt.date:
        return parse_date(s)

    @override
    def serialize(self, dt: dt.date) -> str:
        return serialize_date(dt)


class DateHourParameter(luigi.DateHourParameter):
    """A parameter which takes the value of an hourly `dt.datetime`."""

    def __init__(self, interval: int = 1, **kwargs: Any) -> None:
        super().__init__(interval, EPOCH_UTC, **kwargs)

    @override
    def normalize(self, dt: dt.datetime | str) -> dt.datetime:
        return ensure_datetime(dt)

    @override
    def parse(self, s: str) -> dt.datetime:
        return parse_datetime(s)

    @override
    def serialize(self, dt: dt.datetime) -> str:
        return serialize_datetime(dt)


class DateMinuteParameter(luigi.DateMinuteParameter):
    """A parameter which takes the value of a minutely `dt.datetime`."""

    def __init__(self, interval: int = 1, **kwargs: Any) -> None:
        super().__init__(interval=interval, start=EPOCH_UTC, **kwargs)

    @override
    def normalize(self, dt: dt.datetime | str) -> dt.datetime:
        return ensure_datetime(dt)

    @override
    def parse(self, s: str) -> dt.datetime:
        return parse_datetime(s)

    @override
    def serialize(self, dt: dt.datetime) -> str:
        return serialize_datetime(dt)


class DateSecondParameter(luigi.DateSecondParameter):
    """A parameter which takes the value of a secondly `dt.datetime`."""

    def __init__(self, interval: int = 1, **kwargs: Any) -> None:
        super().__init__(interval, EPOCH_UTC, **kwargs)

    @override
    def normalize(self, dt: dt.datetime | str) -> dt.datetime:
        return ensure_datetime(dt)

    @override
    def parse(self, s: str) -> dt.datetime:
        return parse_datetime(s)

    @override
    def serialize(self, dt: dt.datetime) -> str:
        return serialize_datetime(dt)


_E = TypeVar("_E", bound=Enum)


class EnumParameter(Parameter, Generic[_E]):
    """A parameter which takes the value of an Enum."""

    def __init__(
        self, enum: type[_E], /, *args: Any, case_sensitive: bool = True, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._enum = enum
        self._case_sensitive = case_sensitive

    @override
    def normalize(self, x: _E | str) -> _E:
        return ensure_enum(self._enum, x, case_sensitive=self._case_sensitive)

    @override
    def parse(self, x: str) -> _E:
        return parse_enum(self._enum, x, case_sensitive=self._case_sensitive)

    @override
    def serialize(self, x: _E) -> str:
        return x.name


class FrozenSetIntsParameter(Parameter):
    """A parameter which takes the value of a frozen set of integers."""

    def __init__(
        self, *, separator: str = ",", empty: str = str(sentinel), **kwargs: Any
    ) -> None:
        self._separator = separator
        self._empty = empty
        super().__init__(**kwargs)

    @override
    def normalize(self, x: Iterable[int]) -> frozenset[int]:
        return frozenset(x)

    @override
    def parse(self, x: str) -> frozenset[int]:
        return frozenset(
            map(int, split_str(x, separator=self._separator, empty=self._empty))
        )

    @override
    def serialize(self, x: frozenset[int]) -> str:
        return join_strs(
            sorted(map(str, x)), separator=self._separator, empty=self._empty
        )


class FrozenSetStrsParameter(Parameter):
    """A parameter which takes the value of a frozen set of strings."""

    def __init__(
        self, *, separator: str = ",", empty: str = "{N/A}", **kwargs: Any
    ) -> None:
        self._separator = separator
        self._empty = empty
        super().__init__(**kwargs)

    @override
    def normalize(self, x: IterableStrs) -> frozenset[str]:
        return frozenset(x)

    @override
    def parse(self, x: str) -> frozenset[str]:
        return frozenset(split_str(x, separator=self._separator, empty=self._empty))

    @override
    def serialize(self, x: frozenset[str]) -> str:
        return join_strs(sorted(x), separator=self._separator, empty=self._empty)


class TableParameter(Parameter):
    """Parameter taking the value of a SQLAlchemy table."""

    @override
    def normalize(self, x: Any) -> Any:
        """Normalize a `Table` or model argument."""
        return x

    @override
    def serialize(self, x: Any) -> str:
        """Serialize a `Table` or model argument."""
        from utilities.sqlalchemy import get_table_name

        return get_table_name(x)


class TimeParameter(Parameter):
    """A parameter which takes the value of a `dt.time`."""

    @override
    def normalize(self, x: dt.time | str) -> dt.time:
        return ensure_time(x)

    @override
    def parse(self, x: str) -> dt.time:
        return parse_time(x)

    @override
    def serialize(self, x: dt.time) -> str:
        return serialize_time(x)


class VersionParameter(Parameter):
    """Parameter taking the value of a `Version`."""

    @override
    def normalize(self, x: Version | str) -> Version:
        """Normalize a `Version` argument."""
        from utilities.semver import ensure_version

        return ensure_version(x)

    @override
    def parse(self, x: str) -> Version:
        """Parse a `Version` argument."""
        from semver import Version

        return Version.parse(x)

    @override
    def serialize(self, x: Version) -> str:
        """Serialize a `Version` argument."""
        return str(x)


class WeekdayParameter(Parameter):
    """A parameter which takes the valeu of the previous/next weekday."""

    def __init__(
        self, *args: Any, rounding: Literal["prev", "next"] = "prev", **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        if rounding == "prev":
            self._rounder = round_to_prev_weekday
        else:
            self._rounder = round_to_next_weekday

    @override
    def normalize(self, x: dt.date | str) -> dt.date:
        with suppress(AttributeError, ModuleNotFoundError):
            from utilities.pandas import timestamp_to_date

            x = timestamp_to_date(x)
        return self._rounder(ensure_date(x))

    @override
    def parse(self, x: str) -> dt.date:
        return parse_date(x)

    @override
    def serialize(self, x: dt.date) -> str:
        return serialize_date(x)


# targets


class PathTarget(Target):
    """A local target whose `path` attribute is a Pathlib instance."""

    def __init__(self, path: PathLike, /) -> None:
        super().__init__()
        self.path = Path(path)

    @override
    def exists(self) -> bool:  # type: ignore[]
        """Check if the target exists."""
        return self.path.exists()


class DatabaseTarget(Target):
    """A target point to a set of rows in a database."""

    def __init__(self, sel: Select[Any], engine: Engine, /) -> None:
        super().__init__()
        self._sel = sel.limit(1)
        self._engine = engine

    def exists(self) -> bool:  # type: ignore[]
        from utilities.sqlalchemy import get_table_does_not_exist_message

        engine = self._engine
        match = get_table_does_not_exist_message(engine)
        try:
            with engine.begin() as conn:
                res = conn.execute(self._sel).one_or_none()
        except DatabaseError as error:
            if search(match, ensure_str(one(error.args))):
                return False
            raise  # pragma: no cover
        return res is not None


class EngineParameter(Parameter):
    """Parameter taking the value of a SQLAlchemy engine."""

    @override
    def normalize(self, x: Engine) -> Engine:
        """Normalize an `Engine` argument."""
        return x

    @override
    def parse(self, x: str) -> Engine:
        """Parse an `Engine` argument."""
        from sqlalchemy import create_engine

        return create_engine(x)

    @override
    def serialize(self, x: Engine) -> str:
        """Serialize an `Engine` argument."""
        return x.url.render_as_string()


# tasks


class ExternalTask(ABC, luigi.ExternalTask):
    """An external task with `exists()` defined here."""

    @abstractmethod
    def exists(self) -> bool:
        """Predicate on which the external task is deemed to exist."""
        msg = f"{self=}"  # pragma: no cover
        raise NotImplementedError(msg)  # pragma: no cover

    @override
    def output(self) -> _ExternalTaskDummyTarget:  # type: ignore[]
        return _ExternalTaskDummyTarget(self)


class _ExternalTaskDummyTarget(Target):
    """Dummy target for `ExternalTask`."""

    def __init__(self, task: ExternalTask, /) -> None:
        super().__init__()
        self._task = task

    @override
    def exists(self) -> bool:  # type: ignore[]
        return self._task.exists()


_Task = TypeVar("_Task", bound=Task)


class AwaitTask(ExternalTask, Generic[_Task]):
    """Await the completion of another task."""

    task = cast(_Task, TaskParameter())

    @override
    def exists(self) -> bool:
        return self.task.complete()


class AwaitTime(ExternalTask):
    """Await a specific moment of time."""

    datetime = cast(dt.datetime, DateSecondParameter())

    @override
    def exists(self) -> bool:
        return get_now(time_zone=UTC) >= self.datetime


class ExternalFile(ExternalTask):
    """Await an external file on the local disk."""

    path = cast(Path, PathParameter())

    @override
    def exists(self) -> bool:
        return self.path.exists()


# functions


@overload
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: Literal[False] = False,
    local_scheduler: bool = False,
    log_level: LogLevel | None = None,
    workers: int | None = None,
) -> bool: ...
@overload
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: Literal[True],
    local_scheduler: bool = False,
    log_level: LogLevel | None = None,
    workers: int | None = None,
) -> LuigiRunResult: ...
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: bool = False,
    local_scheduler: bool = False,
    log_level: LogLevel | None = None,
    workers: int | None = None,
) -> bool | LuigiRunResult:
    """Build a set of tasks."""
    return _build(
        task,
        detailed_summary=detailed_summary,
        local_scheduler=local_scheduler,
        **({} if log_level is None else {"log_level": log_level}),
        **({} if workers is None else {"workers": workers}),
    )


_Task = TypeVar("_Task", bound=Task)


@overload
def clone(
    task: Task, cls: type[_Task], /, *, await_: Literal[True], **kwargs: Any
) -> AwaitTask[_Task]: ...
@overload
def clone(
    task: Task, cls: type[_Task], /, *, await_: bool = False, **kwargs: Any
) -> _Task: ...
def clone(
    task: Task, cls: type[_Task], /, *, await_: bool = False, **kwargs: Any
) -> _Task | AwaitTask[_Task]:
    """Clone a task."""
    cloned = cast(_Task, task.clone(cls, **kwargs))
    return AwaitTask(cloned) if await_ else cloned


def yield_dependencies(task: Task, /, *, recursive: bool = False) -> Iterator[Task]:
    """Yield the upstream dependencies of a task."""
    for t in cast(Iterable[Task], flatten(task.requires())):
        yield t
        if recursive:
            yield from yield_dependencies(t, recursive=recursive)


__all__ = [
    "AwaitTask",
    "AwaitTime",
    "DatabaseTarget",
    "DateHourParameter",
    "DateMinuteParameter",
    "DateParameter",
    "DateSecondParameter",
    "EngineParameter",
    "EnumParameter",
    "ExternalFile",
    "ExternalTask",
    "FrozenSetIntsParameter",
    "FrozenSetStrsParameter",
    "PathTarget",
    "TableParameter",
    "TimeParameter",
    "VersionParameter",
    "WeekdayParameter",
    "build",
    "clone",
    "yield_dependencies",
]
