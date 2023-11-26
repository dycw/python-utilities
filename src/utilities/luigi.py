from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, Generic, Literal, TypeVar, cast, overload

import luigi
from luigi import Parameter, PathParameter, Target, Task, TaskParameter
from luigi import build as _build
from luigi.interface import LuigiRunResult
from luigi.notifications import smtp
from luigi.parameter import MissingParameterException
from luigi.task import Register, flatten
from semver import Version
from typing_extensions import override

from utilities._luigi.common import (
    DateHourParameter,
    DateMinuteParameter,
    DateParameter,
    DateSecondParameter,
    EnumParameter,
    TimeParameter,
    WeekdayParameter,
)
from utilities.datetime import UTC, get_now
from utilities.json import deserialize, serialize
from utilities.logging import LogLevel
from utilities.pathlib import PathLike
from utilities.semver import ensure_version
from utilities.types import IterableStrs

# parameters


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


class VersionParameter(Parameter):
    """Parameter taking the value of a `Version`."""

    @override
    def normalize(self, x: Version | str) -> Version:
        """Normalize a `Version` argument."""
        return ensure_version(x)

    @override
    def parse(self, x: str) -> Version:
        """Parse a `Version` argument."""
        return Version.parse(x)

    @override
    def serialize(self, x: Version) -> str:
        """Serialize a `Version` argument."""
        return str(x)


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
    "DateHourParameter",
    "DateMinuteParameter",
    "DateParameter",
    "DateSecondParameter",
    "EnumParameter",
    "ExternalFile",
    "ExternalTask",
    "FrozenSetStrsParameter",
    "PathTarget",
    "TimeParameter",
    "VersionParameter",
    "WeekdayParameter",
    "build",
    "clone",
    "get_dependencies_downstream",
    "get_dependencies_upstream",
    "get_task_classes",
]


try:
    from utilities._luigi.sqlalchemy import (
        DatabaseTarget,
        EngineParameter,
        TableParameter,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["DatabaseTarget", "EngineParameter", "TableParameter"]


try:
    from utilities._luigi.typed_settings import (
        AnnotationAndKeywordsToDictError,
        AnnotationIterableToClassError,
        AnnotationToClassError,
        annotation_and_keywords_to_dict,
        annotation_date_to_class,
        annotation_datetime_to_class,
        annotation_iterable_to_class,
        annotation_to_class,
        annotation_union_to_class,
        build_params_mixin,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "AnnotationAndKeywordsToDictError",
        "AnnotationIterableToClassError",
        "AnnotationToClassError",
        "annotation_and_keywords_to_dict",
        "annotation_date_to_class",
        "annotation_datetime_to_class",
        "annotation_iterable_to_class",
        "annotation_to_class",
        "annotation_union_to_class",
        "build_params_mixin",
    ]
