from typing import Any, TypeVar, cast

from beartype import beartype
from holoviews import save

from utilities.atomicwrites import writer
from utilities.pathlib import PathLike

_T = TypeVar("_T")


@beartype
def apply_opts(plot: _T, /, **opts: Any) -> _T:
    """Apply a set of options to a plot."""
    return cast(Any, plot).opts(**opts)


@beartype
def relabel_plot(plot: _T, label: str, /) -> _T:
    """Re-label a plot."""
    return cast(Any, plot).relabel(label)


@beartype
def save_plot(plot: Any, path: PathLike, /, *, overwrite: bool = False) -> None:
    """Atomically save a plot to disk."""
    with writer(path, overwrite=overwrite) as temp:
        save(plot, temp, backend="bokeh")
