import datetime as dt
from abc import ABC
from abc import abstractmethod
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import suppress
from enum import Enum
from pathlib import Path
from typing import Any
from typing import Generic
from typing import Literal
from typing import Optional
from typing import TypeVar
from typing import Union
from typing import cast
from typing import overload

import luigi
from beartype import beartype
from luigi import DateSecondParameter
from luigi import Parameter
from luigi import PathParameter
from luigi import Target
from luigi import Task
from luigi import TaskParameter
from luigi import build as _build
from luigi.interface import LuigiRunResult
from luigi.notifications import smtp
from luigi.parameter import MissingParameterException
from luigi.task import Register
from luigi.task import flatten

from utilities.datetime import EPOCH_UTC
from utilities.datetime import UTC
from utilities.datetime import ensure_date
from utilities.datetime import ensure_time
from utilities.datetime import parse_date
from utilities.datetime import parse_time
from utilities.datetime import round_to_next_weekday
from utilities.datetime import round_to_prev_weekday
from utilities.datetime import serialize_date
from utilities.datetime import serialize_time
from utilities.enum import ensure_enum
from utilities.enum import parse_enum
from utilities.logging import LogLevel
from utilities.pathlib import PathLike

_E = TypeVar("_E", bound=Enum)


# paramaters


class EnumParameter(Parameter, Generic[_E]):
    """A parameter which takes the value of an Enum."""

    @beartype
    def __init__(
        self,
        enum: type[_E],
        /,
        *args: Any,
        case_sensitive: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._enum = enum
        self._case_sensitive = case_sensitive

    @beartype
    def normalize(self, member: Union[_E, str], /) -> _E:  # noqa: D102
        return ensure_enum(
            self._enum,
            member,
            case_sensitive=self._case_sensitive,
        )

    @beartype
    def parse(self, member: str, /) -> _E:  # noqa: D102
        return parse_enum(
            self._enum,
            member,
            case_sensitive=self._case_sensitive,
        )

    @beartype
    def serialize(self, member: _E, /) -> str:  # noqa: D102
        return member.name


class DateParameter(luigi.DateParameter):
    """A parameter which takes the value of a `dt.date`."""

    @beartype
    def normalize(self, date: Union[dt.date, str], /) -> dt.date:  # noqa: D102
        return ensure_date(date)

    @beartype
    def parse(self, date: str, /) -> dt.date:  # noqa: D102
        return parse_date(date)

    @beartype
    def serialize(self, date: dt.date, /) -> str:  # noqa: D102
        return serialize_date(date)


class TimeParameter(Parameter, Generic[_E]):
    """A parameter which takes the value of a `dt.time`."""

    @beartype
    def normalize(self, time: Union[dt.time, str], /) -> dt.time:  # noqa: D102
        return ensure_time(time)

    @beartype
    def parse(self, time: str, /) -> dt.time:  # noqa: D102
        return parse_time(time)

    @beartype
    def serialize(self, time: dt.time, /) -> str:  # noqa: D102
        return serialize_time(time)


class WeekdayParameter(Parameter):
    """A parameter which takes the valeu of the previous/next weekday."""

    @beartype
    def __init__(
        self,
        *args: Any,
        rounding: Literal["prev", "next"] = "prev",
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        if rounding == "prev":
            self._rounder = round_to_prev_weekday
        else:
            self._rounder = round_to_next_weekday

    @beartype
    def normalize(self, date: Union[dt.date, str], /) -> dt.date:  # noqa: D102
        with suppress(AttributeError, ModuleNotFoundError):
            from utilities.pandas import timestamp_to_date

            date = timestamp_to_date(date)
        return self._rounder(ensure_date(date))

    @beartype
    def parse(self, date: str, /) -> dt.date:  # noqa: D102
        return parse_date(date)

    @beartype
    def serialize(self, date: dt.date, /) -> str:  # noqa: D102
        return serialize_date(date)


# targets


class PathTarget(Target):
    """A local target whose `path` attribute is a Pathlib instance."""

    @beartype
    def __init__(self, path: PathLike, /) -> None:
        super().__init__()
        self.path = Path(path)

    @beartype
    def exists(self) -> bool:
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

    @beartype
    def output(self) -> "_ExternalTaskDummyTarget":  # noqa: D102
        return _ExternalTaskDummyTarget(self)


class _ExternalTaskDummyTarget(Target):
    """Dummy target for `ExternalTask`."""

    @beartype
    def __init__(self, task: ExternalTask, /) -> None:
        super().__init__()
        self._task = task

    @beartype
    def exists(self) -> bool:
        return self._task.exists()


_Task = TypeVar("_Task", bound=Task)


class AwaitTask(ExternalTask, Generic[_Task]):
    """Await the completion of another task."""

    task = cast(_Task, TaskParameter())

    @beartype
    def exists(self) -> bool:  # noqa: D102
        return self.task.complete()


class AwaitTime(ExternalTask):
    """Await a specific moment of time."""

    datetime = cast(dt.datetime, DateSecondParameter(start=EPOCH_UTC))

    @beartype
    def exists(self) -> bool:  # noqa: D102
        return dt.datetime.now(tz=UTC) >= self.datetime


class ExternalFile(ExternalTask):
    """Await an external file on the local disk."""

    path = cast(Path, PathParameter())

    @beartype
    def exists(self) -> bool:  # noqa: D102
        return self.path.exists()


# fucntions


@overload
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: Literal[False] = False,
    local_scheduler: bool = False,
    log_level: Optional[LogLevel] = None,
    workers: Optional[int] = None,
) -> bool:
    ...


@overload
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: Literal[True],
    local_scheduler: bool = False,
    log_level: Optional[LogLevel] = None,
    workers: Optional[int] = None,
) -> LuigiRunResult:
    ...


@beartype
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: bool = False,
    local_scheduler: bool = False,
    log_level: Optional[LogLevel] = None,
    workers: Optional[int] = None,
) -> Union[bool, LuigiRunResult]:
    """Build a set of tasks."""
    return _build(
        task,
        detailed_summary=detailed_summary,
        local_scheduler=local_scheduler,
        **({} if log_level is None else {"log_level": log_level}),
        **({} if workers is None else {"workers": workers}),
    )


_Task = TypeVar("_Task", bound=Task)


@beartype
def clone(task: Task, cls: type[_Task], /, **kwargs: Any) -> _Task:
    """Clone a task."""
    return cast(_Task, task.clone(cls, **kwargs))


@overload
def get_dependencies_downstream(
    task: Task,
    /,
    *,
    cls: type[_Task],
    recursive: bool = False,
) -> frozenset[_Task]:
    ...


@overload
def get_dependencies_downstream(
    task: Task,
    /,
    *,
    cls: None = None,
    recursive: bool = False,
) -> frozenset[Task]:
    ...


@beartype
def get_dependencies_downstream(
    task: Task,
    /,
    *,
    cls: Optional[type[Task]] = None,
    recursive: bool = False,
) -> frozenset[Task]:
    """Get the downstream dependencies of a task."""
    return frozenset(
        _yield_dependencies_downstream(task, cls=cls, recursive=recursive),
    )


@beartype
def _yield_dependencies_downstream(
    task: Task,
    /,
    *,
    cls: Optional[type[Task]] = None,
    recursive: bool = False,
) -> Iterator[Task]:
    for task_cls in cast(Iterable[type[Task]], get_task_classes(cls=cls)):
        try:
            cloned = clone(task, task_cls)
        except (MissingParameterException, TypeError):
            pass
        else:
            if task in get_dependencies_upstream(cloned, recursive=recursive):
                yield cloned
                if recursive:
                    yield from get_dependencies_downstream(
                        cloned,
                        recursive=recursive,
                    )


@beartype
def get_dependencies_upstream(
    task: Task,
    /,
    *,
    recursive: bool = False,
) -> frozenset[Task]:
    """Get the upstream dependencies of a task."""
    return frozenset(_yield_dependencies_upstream(task, recursive=recursive))


@beartype
def _yield_dependencies_upstream(
    task: Task,
    /,
    *,
    recursive: bool = False,
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


@beartype
def get_task_classes(
    *,
    cls: Optional[type[_Task]] = None,
) -> frozenset[type[_Task]]:
    """Yield the task classes. Optionally filter down."""
    return frozenset(_yield_task_classes(cls=cls))


@beartype
def _yield_task_classes(
    *,
    cls: Optional[type[_Task]] = None,
) -> Iterator[type[_Task]]:
    """Yield the task classes. Optionally filter down."""
    for name in cast(Any, Register).task_names():
        task_cls = cast(Any, Register).get_task_cls(name)
        if (
            (cls is None) or ((cls is not task_cls) and issubclass(task_cls, cls))
        ) and (task_cls is not smtp):
            yield cast(type[_Task], task_cls)
