from __future__ import annotations

from holoviews import Curve
from holoviews.plotting import bokeh

from utilities.numpy import has_dtype
from utilities.text import NotAStringError, ensure_str
from utilities.xarray import DataArrayB1, DataArrayF1, DataArrayI1

from .holoviews import apply_opts  # noqa: TID252

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
    except NotAStringError:
        msg = f"{array.name=}"
        raise ArrayNameNotAStringError(msg) from None
    if len(vdim) == 0:
        msg = f"{array.name=}"
        raise ArrayNameIsEmptyStringError(msg) from None
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


class ArrayNameNotAStringError(Exception):
    """Raised when the array name is not a string."""


class ArrayNameIsEmptyStringError(Exception):
    """Raised when the array name is the empty string."""


__all__ = ["ArrayNameIsEmptyStringError", "ArrayNameNotAStringError", "plot_curve"]
