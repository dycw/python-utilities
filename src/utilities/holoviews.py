from typing import Any

from beartype import beartype
from holoviews import save

from utilities.atomicwrites import writer
from utilities.pathlib import PathLike


@beartype
def save_plot(plot: Any, path: PathLike, /, *, overwrite: bool = False) -> None:
    """Atomically save a plot to save."""
    with writer(path, overwrite=overwrite) as temp:
        save(plot, temp, backend="bokeh")
