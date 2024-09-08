from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

import tenacity
from tenacity import RetryCallState
from tenacity import wait_exponential_jitter as _wait_exponential_jitter
from tenacity._utils import MAX_WAIT
from typing_extensions import override

from utilities.datetime import duration_to_float

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.loguru import LogLevel
    from utilities.types import Duration

_INFO: LogLevel = cast(Any, "INFO")


def before_sleep_log(
    *, level: LogLevel = _INFO, exc_info: bool = False
) -> Callable[[RetryCallState], None]:
    """Use `loguru` in around `before_sleep_log`."""
    from utilities.loguru import get_logging_level_number

    level_num = get_logging_level_number(level)
    return tenacity.before_sleep_log(
        cast(Any, _LoguruAdapter()), level_num, exc_info=exc_info
    )


class wait_exponential_jitter(_wait_exponential_jitter):  # noqa: N801
    """Subclass of `wait_exponential_jitter` accepting durations."""

    @override
    def __init__(
        self,
        initial: Duration = 1,
        max: Duration = MAX_WAIT,
        exp_base: float = 2,
        jitter: Duration = 1,
    ) -> None:
        super().__init__(
            initial=duration_to_float(initial),
            max=duration_to_float(max),
            exp_base=exp_base,
            jitter=duration_to_float(jitter),
        )


class _LoguruAdapter:
    """Proxy for `loguru`, for use in `tenacity`."""

    def log(
        self,
        level: int,
        msg: Any,
        /,
        *,
        exc_info: BaseException | Literal[False] | None = None,
    ) -> None:
        from loguru import logger

        from utilities.loguru import get_logging_level_name

        level_name = get_logging_level_name(level)
        logger.opt(exception=exc_info).log(level_name, msg)


__all__ = ["before_sleep_log", "wait_exponential_jitter"]
