from __future__ import annotations

import sys
from functools import partial
from logging import getLogger

from utilities.foo_funcs import func_decorated_first
from utilities.sys import log_exception_paths
from utilities.traceback import trace

_LOGGER = getLogger(__name__)
sys.excepthook = partial(log_exception_paths, logger=_LOGGER)


@trace
def calls_func_first(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_decorated_first(a, b, *args, c, **kwargs)


_ = calls_func_first(1, 2, 3, 4, c=5, d=6, e=7)
