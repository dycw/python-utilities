from __future__ import annotations

import sys
from io import StringIO
from logging import StreamHandler, getLogger
from sys import stdout

from utilities.foo_funcs import func_decorated_first
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
def calls_func_first(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    try:
        return func_decorated_first(a, b, *args, c, **kwargs)
    except AssertionError as error:
        raise ValueError(*error.args)


_ = calls_func_first(1, 2, 3, 4, c=5, d=6, e=7)
