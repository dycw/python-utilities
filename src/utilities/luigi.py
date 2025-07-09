from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, assert_never, cast, overload, override

import luigi
from luigi import Parameter, PathParameter, Target, Task
from luigi import build as _build
from luigi.parameter import ParameterVisibility, _no_value
from whenever import ZonedDateTime

from utilities.whenever import SECOND, round_date_or_date_time

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from luigi.execution_summary import LuigiRunResult

    from utilities.types import Delta, LogLevel, PathLike, ZonedDateTimeLike


# parameters


class ZonedDateTimeParameter(Parameter):
    """A parameter which takes the value of a zoned datetime."""

    _delta: Delta

    @override
    def __init__[T](
        self,
        default: Any = _no_value,
        is_global: bool = False,
        significant: bool = True,
        description: str | None = None,
        config_path: None = None,
        positional: bool = True,
        always_in_help: bool = False,
        batch_method: Callable[[Iterable[T]], T] | None = None,
        visibility: ParameterVisibility = ParameterVisibility.PUBLIC,
        *,
        delta: Delta = SECOND,
    ) -> None:
        super().__init__(
            default,
            is_global,
            significant,
            description,
            config_path,
            positional,
            always_in_help,
            batch_method,
            visibility,
        )
        self._delta = delta

    @override
    def normalize(self, x: ZonedDateTimeLike) -> ZonedDateTime:
        match x:
            case ZonedDateTime() as date_time:
                ...
            case str() as text:
                date_time = ZonedDateTime.parse_common_iso(text)
            case _ as never:
                assert_never(never)
        return round_date_or_date_time(date_time, self._delta, mode="floor")

    @override
    def parse(self, x: str) -> ZonedDateTime:
        return ZonedDateTime.parse_common_iso(x)

    @override
    def serialize(self, x: ZonedDateTime) -> str:
        return x.format_common_iso()


# targets


class PathTarget(Target):
    """A local target whose `path` attribute is a Pathlib instance."""

    def __init__(self, path: PathLike, /) -> None:
        super().__init__()
        self.path = Path(path)

    @override
    def exists(self) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
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
    def output(self) -> _ExternalTaskDummyTarget:  # pyright: ignore[reportIncompatibleMethodOverride]
        return _ExternalTaskDummyTarget(self)


class _ExternalTaskDummyTarget(Target):
    """Dummy target for `ExternalTask`."""

    def __init__(self, task: ExternalTask, /) -> None:
        super().__init__()
        self._task = task

    @override
    def exists(self) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
        return self._task.exists()


class ExternalFile(ExternalTask):
    """Await an external file on the local disk."""

    path: Path = cast("Any", PathParameter())

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


__all__ = [
    "ExternalFile",
    "ExternalTask",
    "PathTarget",
    "ZonedDateTimeParameter",
    "build",
]
