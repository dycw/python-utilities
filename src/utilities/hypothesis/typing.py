from __future__ import annotations

from typing import TypeVar

from hypothesis.strategies import SearchStrategy

_T = TypeVar("_T")
MaybeSearchStrategy = _T | SearchStrategy[_T]


Shape = int | tuple[int, ...]
