from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

import tenacity
from tenacity import RetryCallState

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.loguru import LogLevel

_INFO: LogLevel = cast(Any, "INFO")


def before_sleep_log(
    *, level: LogLevel = _INFO, exc_info: bool = False
) -> Callable[[RetryCallState], None]:
    """Use `loguru` in around `before_sleep_log`."""
    from utilities.loguru import get_logging_level

    return tenacity.before_sleep_log(
        cast(Any, _LoguruAdapter()), get_logging_level(level), exc_info=exc_info
    )


class _LoguruAdapter:
    """Proxy for `loguru`, for use in `tenacity`."""

    def log(
        self,
        msg: Any,
        level: int,
        /,
        *,
        exc_info: BaseException | Literal[False] | None = None,
    ) -> None:
        from loguru import logger

        logger.opt(exception=exc_info).log(msg, level)


__all__ = ["before_sleep_log"]
