from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from pyinstrument.profiler import Profiler

from utilities.atomicwrites import writer
from utilities.datetime import UTC, get_now
from utilities.pathlib import PathLike


@contextmanager
def profile(*, path: PathLike = Path.cwd()) -> Iterator[None]:
    """Profile the contents of a block."""
    with Profiler() as profiler:
        yield
    now = get_now(tz=UTC)
    filename = Path(path, f"profile__{now:%Y%m%dT%H%M%S}.html")
    with writer(filename) as temp, temp.open(mode="w") as fh:
        _ = fh.write(profiler.output_html())


__all__ = ["profile"]
