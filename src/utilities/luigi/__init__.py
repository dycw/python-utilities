import datetime as dt
from collections.abc import Iterable, Iterator
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Generic,
    Literal,
    Optional,
    TypeVar,
    Union,
    cast,
    overload,
)

from beartype import beartype
from luigi import Parameter, Target, Task
from luigi import build as _build
from luigi.interface import LuigiRunResult
from luigi.notifications import smtp
from luigi.parameter import MissingParameterException
from luigi.task import Register, flatten

from utilities.datetime import parse_time, serialize_time
from utilities.enum import parse_enum
from utilities.logging import LogLevel
from utilities.pathlib import PathLike

_E = TypeVar("_E", bound=Enum)


class EnumParameter(Parameter, Generic[_E]):
    """A parameter which takes the value of an Enum."""

    @beartype
    def __init__(self, enum: type[_E], /, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._enum = enum

    @beartype
    def normalize(self, member: Union[_E, str], /) -> _E:  # noqa: D102
        return parse_enum(self._enum, member)

    @beartype
    def parse(self, member: str, /) -> _E:  # noqa: D102
        return parse_enum(self._enum, member)

    @beartype
    def serialize(self, member: _E, /) -> str:  # noqa: D102
        return member.name


class TimeParameter(Parameter, Generic[_E]):
    """A parameter which takes the value of a `dt.time`."""

    @beartype
    def normalize(self, time: dt.time, /) -> dt.time:  # noqa: D102
        return time

    @beartype
    def parse(self, time: str, /) -> dt.time:  # noqa: D102
        return parse_time(time)

    @beartype
    def serialize(self, time: dt.time, /) -> str:  # noqa: D102
        return serialize_time(time)


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
    for name in Register.task_names():
        task_cls = Register.get_task_cls(name)
        if (
            (cls is None)
            or ((cls is not task_cls) and issubclass(task_cls, cls))
        ) and (task_cls is not smtp):
            yield cast(type[_Task], task_cls)
