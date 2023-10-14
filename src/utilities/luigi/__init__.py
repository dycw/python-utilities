from __future__ import annotations

from .luigi import AwaitTask
from .luigi import AwaitTime
from .luigi import DateHourParameter
from .luigi import DateMinuteParameter
from .luigi import DateParameter
from .luigi import DateSecondParameter
from .luigi import EnumParameter
from .luigi import ExternalFile
from .luigi import ExternalTask
from .luigi import PathTarget
from .luigi import TimeParameter
from .luigi import WeekdayParameter
from .luigi import build
from .luigi import clone
from .luigi import get_dependencies_downstream
from .luigi import get_dependencies_upstream
from .luigi import get_task_classes

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
    "get_dependencies_downstream",
    "get_dependencies_upstream",
    "get_task_classes",
    "PathTarget",
    "TimeParameter",
    "WeekdayParameter",
]


try:
    from .attrs import AmbiguousDateError
    from .attrs import AmbiguousDatetimeError
    from .attrs import InvalidAnnotationAndKeywordsError
    from .attrs import InvalidAnnotationError
    from .attrs import build_params_mixin
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


try:
    from .semver import VersionParameter
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["VersionParameter"]


try:
    from .sqlalchemy import DatabaseTarget
    from .sqlalchemy import EngineParameter
    from .sqlalchemy import TableParameter
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["DatabaseTarget", "EngineParameter", "TableParameter"]
