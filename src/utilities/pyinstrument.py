import datetime as dt
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from zoneinfo import ZoneInfo

from beartype import beartype
from pyinstrument.profiler import Profiler

from utilities.atomicwrites import writer
from utilities.pathlib import PathLike


@contextmanager
@beartype
def profile(*, path: PathLike = Path.cwd()) -> Iterator[None]:
    """Profile the contents of a block."""
    with Profiler() as profiler:
        yield
    now = dt.datetime.now(tz=ZoneInfo("UTC"))
    filename = Path(path, f"profile__{now:%Y%m%dT%H%M%S}.html")
    with writer(filename) as temp, temp.open(mode="w") as fh:
        _ = fh.write(profiler.output_html())
