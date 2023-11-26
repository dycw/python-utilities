from __future__ import annotations

from typing import Any, TypeVar, cast

_T = TypeVar("_T")


def apply_opts(plot: _T, /, **opts: Any) -> _T:
    """Apply a set of options to a plot."""
    return cast(Any, plot).opts(**opts)


__all__ = ["apply_opts"]
