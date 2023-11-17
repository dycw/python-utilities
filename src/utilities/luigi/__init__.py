from __future__ import annotations

from utilities.luigi.luigi import (
    AwaitTask,
    AwaitTime,
    DateHourParameter,
    DateMinuteParameter,
    DateParameter,
    DateSecondParameter,
    EnumParameter,
    ExternalFile,
    ExternalTask,
    FrozenSetStrsParameter,
    PathTarget,
    TimeParameter,
    WeekdayParameter,
    build,
    clone,
    get_dependencies_downstream,
    get_dependencies_upstream,
    get_task_classes,
)

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


try:
    from utilities.luigi.semver import VersionParameter
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["VersionParameter"]


try:
    from utilities.luigi.sqlalchemy import (
        DatabaseTarget,
        EngineParameter,
        TableParameter,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["DatabaseTarget", "EngineParameter", "TableParameter"]


try:
    from utilities.luigi.typed_settings import (
        AmbiguousDateError,
        AmbiguousDatetimeError,
        InvalidAnnotationAndKeywordsError,
        InvalidAnnotationError,
        build_params_mixin,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "AmbiguousDateError",
        "AmbiguousDatetimeError",
        "build_params_mixin",
        "InvalidAnnotationAndKeywordsError",
        "InvalidAnnotationError",
    ]
