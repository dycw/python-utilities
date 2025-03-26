from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from pyinstrument.profiler import Profiler

from utilities.datetime import get_now_local, serialize_compact
from utilities.pathlib import PWD

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


@contextmanager
def profile(*, path: PathLike = PWD) -> Iterator[None]:
    """Profile the contents of a block."""
    from utilities.atomicwrites import writer

    with Profiler() as profiler:
        yield
    filename = Path(path, f"profile__{serialize_compact(get_now_local())}.html")
    with writer(filename) as temp, temp.open(mode="w") as fh:
        _ = fh.write(profiler.output_html())


__all__ = ["profile"]
