from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast, overload, override

import luigi
from luigi import Parameter, PathParameter, Target, Task
from luigi import build as _build

from utilities.datetime import EPOCH_UTC

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Iterable

    from luigi.execution_summary import LuigiRunResult

    from utilities.types import LogLevel, MaybeStr, PathLike


# parameters


class DateHourParameter(luigi.DateHourParameter):
    """A parameter which takes the value of an hourly `dt.datetime`."""

    def __init__(self, interval: int = 1, **kwargs: Any) -> None:
        super().__init__(interval, EPOCH_UTC, **kwargs)

    @override
    def normalize(self, dt: MaybeStr[dt.datetime]) -> dt.datetime:
        from utilities.whenever import ensure_zoned_datetime

        return ensure_zoned_datetime(dt)

    @override
    def parse(self, s: str) -> dt.datetime:
        from utilities.whenever import parse_zoned_datetime

        return parse_zoned_datetime(s)

    @override
    def serialize(self, dt: dt.datetime) -> str:
        from utilities.whenever import serialize_zoned_datetime

        return serialize_zoned_datetime(dt)


class DateMinuteParameter(luigi.DateMinuteParameter):
    """A parameter which takes the value of a minutely `dt.datetime`."""

    def __init__(self, interval: int = 1, **kwargs: Any) -> None:
        super().__init__(interval=interval, start=EPOCH_UTC, **kwargs)

    @override
    def normalize(self, dt: MaybeStr[dt.datetime]) -> dt.datetime:
        from utilities.whenever import ensure_zoned_datetime

        return ensure_zoned_datetime(dt)

    @override
    def parse(self, s: str) -> dt.datetime:
        from utilities.whenever import parse_zoned_datetime

        return parse_zoned_datetime(s)

    @override
    def serialize(self, dt: dt.datetime) -> str:
        from utilities.whenever import serialize_zoned_datetime

        return serialize_zoned_datetime(dt)


class DateSecondParameter(luigi.DateSecondParameter):
    """A parameter which takes the value of a secondly `dt.datetime`."""

    def __init__(self, interval: int = 1, **kwargs: Any) -> None:
        super().__init__(interval, EPOCH_UTC, **kwargs)

    @override
    def normalize(self, dt: MaybeStr[dt.datetime]) -> dt.datetime:
        from utilities.whenever import ensure_zoned_datetime

        return ensure_zoned_datetime(dt)

    @override
    def parse(self, s: str) -> dt.datetime:
        from utilities.whenever import parse_zoned_datetime

        return parse_zoned_datetime(s)

    @override
    def serialize(self, dt: dt.datetime) -> str:
        from utilities.whenever import serialize_zoned_datetime

        return serialize_zoned_datetime(dt)


class TimeParameter(Parameter):
    """A parameter which takes the value of a `dt.time`."""

    @override
    def normalize(self, x: MaybeStr[dt.time]) -> dt.time:
        from utilities.whenever import ensure_time

        return ensure_time(x)

    @override
    def parse(self, x: str) -> dt.time:
        from utilities.whenever import parse_time

        return parse_time(x)

    @override
    def serialize(self, x: dt.time) -> str:
        from utilities.whenever import serialize_time

        return serialize_time(x)


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
    "DateHourParameter",
    "DateMinuteParameter",
    "DateSecondParameter",
    "ExternalFile",
    "ExternalTask",
    "PathTarget",
    "TimeParameter",
    "build",
]
