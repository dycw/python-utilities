from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from pyinstrument.profiler import Profiler

from utilities.datetime import serialize_compact
from utilities.pathlib import get_path
from utilities.tzlocal import get_now_local

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import MaybeCallablePathLike


@contextmanager
def profile(*, path: MaybeCallablePathLike | None = Path.cwd) -> Iterator[None]:
    """Profile the contents of a block."""
    from utilities.atomicwrites import writer

    with Profiler() as profiler:
        yield
    filename = get_path(path=path).joinpath(
        f"profile__{serialize_compact(get_now_local())}.html"
    )
    with writer(filename) as temp, temp.open(mode="w") as fh:
        _ = fh.write(profiler.output_html())


__all__ = ["profile"]
