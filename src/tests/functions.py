from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING

from loguru import logger
from tenacity import retry, wait_fixed

from utilities.functions import is_not_none
from utilities.loguru import LogLevel, log
from utilities.tenacity import before_sleep_log

if TYPE_CHECKING:
    from asyncio import sleep
    from collections.abc import Callable


# test tenacity
