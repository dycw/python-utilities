from collections.abc import Iterable
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from typing import Literal
from typing import Optional
from typing import TypeVar
from typing import Union
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
    task: Task, /, *, cls: type[_Task], recursive: bool = False
) -> frozenset[_Task]:
    ...


@overload
def get_dependencies_downstream(
    task: Task, /, *, cls: None = None, recursive: bool = False
) -> frozenset[Task]:
    ...


@beartype
def get_dependencies_downstream(
    task: Task, /, *, cls: Optional[type[Task]] = None, recursive: bool = False
) -> frozenset[Task]:
    """Get the downstream dependencies of a task."""
    return frozenset(
        _yield_dependencies_downstream(task, cls=cls, recursive=recursive)
    )


@beartype
def _yield_dependencies_downstream(
    task: Task, /, *, cls: Optional[type[Task]] = None, recursive: bool = False
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
                        cloned, recursive=recursive
                    )


@beartype
def get_dependencies_upstream(
    task: Task, /, *, recursive: bool = False
) -> frozenset[Task]:
    """Get the upstream dependencies of a task."""
    return frozenset(_yield_dependencies_upstream(task, recursive=recursive))


@beartype
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


@beartype
def get_task_classes(
    *, cls: Optional[type[_Task]] = None
) -> frozenset[type[_Task]]:
    """Yield the task classes. Optionally filter down."""
    return frozenset(_yield_task_classes(cls=cls))


@beartype
def _yield_task_classes(
    *, cls: Optional[type[_Task]] = None
) -> Iterator[type[_Task]]:
    """Yield the task classes. Optionally filter down."""
    for name in Register.task_names():
        task_cls = Register.get_task_cls(name)
        if (
            (cls is None)
            or ((cls is not task_cls) and issubclass(task_cls, cls))
        ) and (task_cls is not smtp):
            yield cast(type[_Task], task_cls)
