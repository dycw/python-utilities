from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from pyinstrument.profiler import Profiler

from utilities.core import get_now_local, write_text
from utilities.pathlib import to_path
from utilities.whenever import format_compact

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
    text = profiler.output_html()
    write_text(filename, text, overwrite=True)


__all__ = ["profile"]
