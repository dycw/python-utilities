from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from holoviews import Curve
from holoviews.plotting import bokeh
from typing_extensions import override

from utilities._holoviews.common import apply_opts
from utilities.iterables import _CheckLengthMinError, check_length
from utilities.numpy import has_dtype
from utilities.text import EnsureStrError, ensure_str
from utilities.types import get_class_name
from utilities.xarray import DataArrayB1, DataArrayF1, DataArrayI1

_ = bokeh


def plot_curve(
    array: DataArrayB1 | DataArrayI1 | DataArrayF1,
    /,
    *,
    label: str | None = None,
    smooth: int | None = None,
    aspect: float | None = None,
) -> Curve:
    """Plot a 1D array as a curve."""
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


class PlotCurveError(Exception):
    ...


@dataclass(frozen=True, kw_only=True, slots=True)
class _PlotCurveArrayNameNotAStringError(PlotCurveError):
    name: Any

    @override
    def __str__(self) -> str:
        return "Array name {} must be a string; got {!r} instead".format(
            self.name, get_class_name(self.name)
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class _PlotCurveArrayNameIsEmptyError(PlotCurveError):
    name: str

    @override
    def __str__(self) -> str:
        return "Array name {!r} must not be empty".format(self.name)


__all__ = ["PlotCurveError", "plot_curve"]
