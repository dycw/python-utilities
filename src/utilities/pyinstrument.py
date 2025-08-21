from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from pyinstrument.profiler import Profiler

from utilities.atomicwrites import writer
from utilities.pathlib import to_path
from utilities.whenever import format_compact, get_now_local

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import MaybeCallablePathLike


@contextmanager
def profile(path: MaybeCallablePathLike = Path.cwd, /) -> Iterator[None]:
    """Profile the contents of a block."""
    with Profiler() as profiler:
        yield
    filename = to_path(path).joinpath(
        f"profile__{format_compact(get_now_local(), path=True)}.html"
    )
    with writer(filename) as temp:
        _ = temp.write_text(profiler.output_html())


__all__ = ["profile"]
