import datetime as dt
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from pyinstrument.profiler import Profiler

from utilities.pathlib import PathLike
from utilities.typeguard import typeguard_ignore


@typeguard_ignore
@contextmanager
def profile(*, path: PathLike = Path.cwd()) -> Iterator[None]:
    """Profile the contents of a block."""

    with Profiler() as profiler:
        yield
    now = dt.datetime.now()
    filename = Path(path, f"profile__{now:%Y%m%dT%H%M%S}.html")
    with open(filename, mode="w") as fh:
        _ = fh.write(profiler.output_html())
    _ = fh  # this ensures that `fh` is considered returned; for coverage
