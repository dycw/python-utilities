from __future__ import annotations

import sys
from asyncio import TaskGroup, run
from io import StringIO
from logging import StreamHandler, getLogger
from sys import stdout

from tests.test_traceback_funcs.async_ import func_async
from utilities.sys import log_traceback_excepthook
from utilities.traceback import trace

buffer = StringIO()
LOGGER = getLogger("utilities.sys")
LOGGER.setLevel("INFO")
handler = StreamHandler(stdout)
handler.setLevel("INFO")
LOGGER.addHandler(handler)


sys.excepthook = log_traceback_excepthook


@trace
async def calls_async(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    async with TaskGroup() as tg:
        _ = tg.create_task(func_async(1, 2, 3, 4, c=5, d=6, e=7))


_ = run(calls_async(1, 2, 3, 4, c=5, d=6, e=7))
