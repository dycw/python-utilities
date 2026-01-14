from __future__ import annotations

import shelve
from pathlib import Path
from shelve import Shelf
from typing import TYPE_CHECKING, Any, Literal

from utilities.contextlib import enhanced_context_manager

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


type _Flag = Literal["r", "w", "c", "n"]


@enhanced_context_manager
def yield_shelf(
    path: PathLike,
    /,
    *,
    flag: _Flag = "c",
    protocol: int | None = None,
    writeback: bool = False,
) -> Iterator[Shelf[Any]]:
    """Yield a shelf."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with shelve.open(path, flag=flag, protocol=protocol, writeback=writeback) as shelf:  # noqa: S301
        yield shelf


__all__ = ["yield_shelf"]
