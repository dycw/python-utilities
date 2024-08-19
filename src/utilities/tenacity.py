from __future__ import annotations

import logging
from typing import Any, Literal

from typing_extensions import override


class LoguruAdapter(logging.Logger):
    """Proxy for `loguru`, for use in `tenacity`."""

    @override
    def __init__(self) -> None: ...  # pyright: ignore[reportMissingSuperCall]

    @override
    def log(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        msg: Any,
        level: int,
        /,
        *,
        exc_info: BaseException | Literal[False] | None,
    ) -> None:
        from loguru import logger

        logger.opt(exception=exc_info).log(msg, level)


__all__ = ["LoguruAdapter"]
