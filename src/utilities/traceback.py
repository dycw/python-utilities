from __future__ import annotations

import inspect
import sys
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType


def f(a, b):
    c = a + b
    d = a - b
    return g(c, d)


def g(c, d):
    return c / d


try:
    f(2, 2)
except Exception:
    rich.print(log_exception())
