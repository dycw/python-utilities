from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from contextlib import suppress
from enum import Enum
from pathlib import Path
from typing import Any, Generic, Literal, TypeVar, cast, overload

import luigi
from luigi import Parameter, PathParameter, Target, Task, TaskParameter
from luigi import build as _build
from luigi.interface import LuigiRunResult
from luigi.notifications import smtp
from luigi.parameter import MissingParameterException
from luigi.task import Register, flatten
from typing_extensions import override

from utilities.datetime import (
    EPOCH_UTC,
    UTC,
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
from utilities.json import deserialize, serialize
from utilities.logging import LogLevel
from utilities.pathlib import PathLike
from utilities.typing import IterableStrs

_E = TypeVar("_E", bound=Enum)


# paramaters


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


class FrozenSetStrsParameter(Parameter):
    """A parameter which takes the value of a frozen set of strings."""

    @override
    def normalize(self, x: IterableStrs) -> frozenset[str]:
        return frozenset(x)

    @override
    def parse(self, x: str) -> frozenset[str]:
        return deserialize(x)

    @override
    def serialize(self, x: frozenset[str]) -> str:
        return serialize(x)


class TimeParameter(Parameter, Generic[_E]):
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
    def exists(self) -> bool:  # type: ignore
        """Check if the target exists."""
        return self.path.exists()


# tasks


class ExternalTask(ABC, luigi.ExternalTask):
    """An external task with `exists()` defined here."""

    @abstractmethod
    def exists(self) -> bool:
        """Predicate on which the external task is deemed to exist."""
        msg = f"{self=}"  # pragma: no cover
        raise NotImplementedError(msg)  # pragma: no cover

    @override
    def output(self) -> _ExternalTaskDummyTarget:  # type: ignore
        return _ExternalTaskDummyTarget(self)


class _ExternalTaskDummyTarget(Target):
    """Dummy target for `ExternalTask`."""

    def __init__(self, task: ExternalTask, /) -> None:
        super().__init__()
        self._task = task

    @override
    def exists(self) -> bool:  # type: ignore
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
        return get_now(tz=UTC) >= self.datetime


class ExternalFile(ExternalTask):
    """Await an external file on the local disk."""

    path = cast(Path, PathParameter())

    @override
    def exists(self) -> bool:
        return self.path.exists()


# fucntions


@overload
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: Literal[False] = False,
    local_scheduler: bool = False,
    log_level: LogLevel | None = None,
    workers: int | None = None,
) -> bool:
    ...


@overload
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: Literal[True],
    local_scheduler: bool = False,
    log_level: LogLevel | None = None,
    workers: int | None = None,
) -> LuigiRunResult:
    ...


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
) -> AwaitTask[_Task]:
    ...


@overload
def clone(
    task: Task, cls: type[_Task], /, *, await_: bool = False, **kwargs: Any
) -> _Task:
    ...


def clone(
    task: Task, cls: type[_Task], /, *, await_: bool = False, **kwargs: Any
) -> _Task | AwaitTask[_Task]:
    """Clone a task."""
    cloned = cast(_Task, task.clone(cls, **kwargs))
    return AwaitTask(cloned) if await_ else cloned


@overload
def get_dependencies_downstream(
    task: Task, /, *, cls: type[_Task], recursive: bool = False
) -> frozenset[_Task]:
    ...


@overload
def get_dependencies_downstream(
    task: Task, /, *, cls: None = None, recursive: bool = False
) -> frozenset[Task]:
    ...


def get_dependencies_downstream(
    task: Task, /, *, cls: type[Task] | None = None, recursive: bool = False
) -> frozenset[Task]:
    """Get the downstream dependencies of a task."""
    return frozenset(_yield_dependencies_downstream(task, cls=cls, recursive=recursive))


def _yield_dependencies_downstream(
    task: Task, /, *, cls: type[Task] | None = None, recursive: bool = False
) -> Iterator[Task]:
    for task_cls in cast(Iterable[type[Task]], get_task_classes(cls=cls)):
        yield from _yield_dependencies_downstream_1(task, task_cls, recursive=recursive)


def _yield_dependencies_downstream_1(
    task: Task, task_cls: type[Task], /, *, recursive: bool = False
) -> Iterator[Task]:
    try:
        cloned = clone(task, task_cls)
    except (MissingParameterException, TypeError):
        pass
    else:
        if task in get_dependencies_upstream(cloned, recursive=recursive):
            yield cloned
            if recursive:
                yield from get_dependencies_downstream(cloned, recursive=recursive)


def get_dependencies_upstream(
    task: Task, /, *, recursive: bool = False
) -> frozenset[Task]:
    """Get the upstream dependencies of a task."""
    return frozenset(_yield_dependencies_upstream(task, recursive=recursive))


def _yield_dependencies_upstream(
    task: Task, /, *, recursive: bool = False
) -> Iterator[Task]:
    for t in cast(Iterable[Task], flatten(task.requires())):
        yield t
        if recursive:
            yield from get_dependencies_upstream(t, recursive=recursive)


@overload
def get_task_classes(*, cls: type[_Task]) -> frozenset[type[_Task]]:
    ...


@overload
def get_task_classes(*, cls: None = None) -> frozenset[type[Task]]:
    ...


def get_task_classes(*, cls: type[_Task] | None = None) -> frozenset[type[_Task]]:
    """Yield the task classes. Optionally filter down."""
    return frozenset(_yield_task_classes(cls=cls))


def _yield_task_classes(*, cls: type[_Task] | None = None) -> Iterator[type[_Task]]:
    """Yield the task classes. Optionally filter down."""
    for name in cast(Any, Register).task_names():
        task_cls = cast(Any, Register).get_task_cls(name)
        if (
            (cls is None) or ((cls is not task_cls) and issubclass(task_cls, cls))
        ) and (task_cls is not smtp):
            yield cast(type[_Task], task_cls)


__all__ = [
    "AwaitTask",
    "AwaitTime",
    "build",
    "clone",
    "DateHourParameter",
    "DateMinuteParameter",
    "DateParameter",
    "DateSecondParameter",
    "EnumParameter",
    "ExternalFile",
    "ExternalTask",
    "FrozenSetStrsParameter",
    "get_dependencies_downstream",
    "get_dependencies_upstream",
    "get_task_classes",
    "PathTarget",
    "TimeParameter",
    "WeekdayParameter",
]
