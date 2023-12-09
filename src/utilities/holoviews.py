from __future__ import annotations

from typing import Any, TypeVar, cast

from holoviews import Layout, save

from utilities._holoviews.common import apply_opts
from utilities.atomicwrites import writer
from utilities.types import PathLike

_T = TypeVar("_T")


def apply_cols(layout: Layout, ncols: int, /) -> Layout:
    """Apply the `cols` argument to a layout."""
    return layout.cols(ncols)


def relabel_plot(plot: _T, label: str, /) -> _T:
    """Re-label a plot."""
    return cast(Any, plot).relabel(label)


def save_plot(plot: Any, path: PathLike, /, *, overwrite: bool = False) -> None:
    """Atomically save a plot to disk."""
    with writer(path, overwrite=overwrite) as temp:  # pragma: os-ne-linux
        save(plot, temp, backend="bokeh")


__all__ = ["apply_cols", "apply_opts", "relabel_plot", "save_plot"]


try:
    from utilities._holoviews.xarray import PlotCurveError, plot_curve
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["PlotCurveError", "plot_curve"]
