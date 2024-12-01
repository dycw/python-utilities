from __future__ import annotations

import sys
from functools import partial
from itertools import chain
from logging import getLogger

from utilities.sys import log_exception_paths
from utilities.traceback import trace

_LOGGER = getLogger(__name__)
sys.excepthook = partial(log_exception_paths, logger=_LOGGER)


@trace
def first(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return second(a, b, *args, c, **kwargs)


@trace
def second(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
    return result


_ = first(1, 2, 3, 4, c=5, d=6, e=7)
