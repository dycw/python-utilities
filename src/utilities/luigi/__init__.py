from collections.abc import Iterable
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from typing import Literal
from typing import TypeVar
from typing import cast
from typing import overload

from beartype import beartype
from luigi import Target
from luigi import Task
from luigi import build as _build
from luigi.interface import LuigiRunResult
from luigi.notifications import smtp
from luigi.parameter import MissingParameterException
from luigi.task import Register
from luigi.task import flatten

from utilities.logging import LogLevel
from utilities.pathlib import PathLike


class PathTarget(Target):
    """A local target whose `path` attribute is a Pathlib instance."""

    @beartype
    def __init__(self, path: PathLike, /) -> None:
        super().__init__()
        self.path = Path(path)

    @beartype
    def exists(self) -> bool:
        return self.path.exists()


@overload
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: Literal[False] = False,  # noqa: U100
    local_scheduler: bool = False,  # noqa: U100
    log_level: LogLevel | None = None,  # noqa: U100
    workers: int | None = None,  # noqa: U100
) -> bool:
    ...


@overload
def build(
    task: Iterable[Task],
    /,
    *,
    detailed_summary: Literal[True],  # noqa: U100
    local_scheduler: bool = False,  # noqa: U100
    log_level: LogLevel | None = None,  # noqa: U100
    workers: int | None = None,  # noqa: U100
) -> LuigiRunResult:
    ...


@beartype
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


@beartype
def clone(task: Task, cls: type[_Task], /, **kwargs: Any) -> _Task:
    """Clone a task."""

    return cast(_Task, task.clone(cls, **kwargs))


@overload
def yield_dependencies_downstream(
    task: Task, /, *, cls: type[_Task], recursive: bool = False  # noqa: U100
) -> Iterator[_Task]:
    ...


@overload
def yield_dependencies_downstream(
    task: Task, /, *, cls: None = None, recursive: bool = False  # noqa: U100
) -> Iterator[Task]:
    ...


@beartype
def yield_dependencies_downstream(
    task: Task, /, *, cls: type[Task] | None = None, recursive: bool = False
) -> Iterator[Task]:
    """Yield the downlaodstream dependencies of a task."""

    for task_cls in cast(Iterable[type[Task]], yield_task_classes(cls=cls)):
        try:
            cloned = clone(task, task_cls)
        except (MissingParameterException, TypeError):
            pass
        else:
            if task in yield_dependencies_upstream(cloned, recursive=recursive):
                yield cloned
                if recursive:
                    yield from yield_dependencies_downstream(
                        cloned, recursive=recursive
                    )


@beartype
def yield_dependencies_upstream(
    task: Task, /, *, recursive: bool = False
) -> Iterator[Task]:
    """Yield the upstream dependencies of a task."""

    for t in cast(Iterable[Task], flatten(task.requires())):
        yield t
        if recursive:
            yield from yield_dependencies_upstream(t, recursive=recursive)


@overload
def yield_task_classes(
    *, cls: type[_Task]  # noqa: U100
) -> Iterator[type[_Task]]:
    ...


@overload
def yield_task_classes(
    *, cls: None = None  # noqa: U100
) -> Iterator[type[Task]]:
    ...


@beartype
def yield_task_classes(
    *, cls: type[_Task] | None = None
) -> Iterator[type[_Task]]:
    """Yield the task classes. Optionally filter down."""

    for name in Register.task_names():
        task_cls = Register.get_task_cls(name)
        if (
            (cls is None)
            or ((cls is not task_cls) and issubclass(task_cls, cls))
        ) and (task_cls is not smtp):
            yield cast(type[_Task], task_cls)
