from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from pyinstrument.profiler import Profiler

from utilities.atomicwrites import writer
from utilities.datetime import get_now, local_timezone
from utilities.pathvalidate import valid_path, valid_path_cwd
from utilities.types import PathLike


@contextmanager
def profile(*, path: PathLike = valid_path_cwd()) -> Iterator[None]:
    """Profile the contents of a block."""
    with Profiler() as profiler:
        yield
    now = get_now(tz=local_timezone())
    filename = valid_path(path, f"profile__{now:%Y%m%dT%H%M%S}.html")
    with writer(filename) as temp, temp.open(mode="w") as fh:
        _ = fh.write(profiler.output_html())


__all__ = ["profile"]
