from __future__ import annotations

import shelve
from contextlib import contextmanager
from pathlib import Path
from shelve import Shelf
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


@contextmanager
def yield_shelf(path: PathLike, /) -> Iterator[Shelf[Any]]:
    """Yield a shelf."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with shelve.open(path) as shelf:  # noqa: S301
        yield shelf


__all__ = ["yield_shelf"]
