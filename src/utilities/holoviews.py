from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast

from holoviews import Curve, Layout, save
from holoviews.plotting import bokeh
from typing_extensions import override

from utilities.iterables import _CheckLengthMinError, check_length
from utilities.text import EnsureStrError, ensure_str
from utilities.types import PathLike, get_class_name

if TYPE_CHECKING:
    from utilities.math import IntPos
    from utilities.xarray import DataArrayB1, DataArrayF1, DataArrayI1


_ = bokeh
_T = TypeVar("_T")


def apply_cols(layout: Layout, ncols: int, /) -> Layout:
    """Apply the `cols` argument to a layout."""
    return layout.cols(ncols)


def apply_opts(plot: _T, /, **opts: Any) -> _T:
    """Apply a set of options to a plot."""
    return cast(Any, plot).opts(**opts)


def plot_curve(
    array: DataArrayB1 | DataArrayI1 | DataArrayF1,
    /,
    *,
    label: str | None = None,
    smooth: IntPos | None = None,
    aspect: float | None = None,
) -> Curve:
    """Plot a 1D array as a curve."""
    from utilities.numpy import has_dtype

    if has_dtype(array, bool):
        return plot_curve(array.astype(int), label=label, smooth=smooth, aspect=aspect)
    (kdim,) = array.dims
    try:
        vdim = ensure_str(array.name)
    except EnsureStrError:
        raise _PlotCurveArrayNameNotAStringError(name=array.name) from None
    try:
        _ = check_length(vdim, min=1)
    except _CheckLengthMinError:
        raise _PlotCurveArrayNameIsEmptyError(name=vdim) from None
    if label is None:
        label = vdim
    if smooth is not None:
        from utilities.xarray import ewma

        array = ewma(array, {kdim: smooth})
        label = f"{label} (MA{smooth})"
    curve = Curve(array, kdims=[kdim], vdims=[vdim], label=label)
    curve = apply_opts(curve, show_grid=True, tools=["hover"])
    if aspect is not None:
        return apply_opts(curve, aspect=aspect)
    return curve


class PlotCurveError(Exception): ...


@dataclass(kw_only=True)
class _PlotCurveArrayNameNotAStringError(PlotCurveError):
    name: Any

    @override
    def __str__(self) -> str:
        return f"Array name {self.name} must be a string; got {get_class_name(self.name)!r} instead"


@dataclass(kw_only=True)
class _PlotCurveArrayNameIsEmptyError(PlotCurveError):
    name: str

    @override
    def __str__(self) -> str:
        return f"Array name {self.name!r} must not be empty"


def relabel_plot(plot: _T, label: str, /) -> _T:
    """Re-label a plot."""
    return cast(Any, plot).relabel(label)


def save_plot(plot: Any, path: PathLike, /, *, overwrite: bool = False) -> None:
    """Atomically save a plot to disk."""
    from utilities.atomicwrites import writer  # pragma: os-ne-linux

    with writer(path, overwrite=overwrite) as temp:  # pragma: os-ne-linux
        save(plot, temp, backend="bokeh")


__all__ = [
    "PlotCurveError",
    "apply_cols",
    "apply_opts",
    "plot_curve",
    "relabel_plot",
    "save_plot",
]
