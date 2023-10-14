from __future__ import annotations

from .holoviews import apply_opts
from .holoviews import relabel_plot
from .holoviews import save_plot

__all__ = ["apply_opts", "relabel_plot", "save_plot"]


try:
    from .xarray import ArrayNameIsEmptyStringError
    from .xarray import ArrayNameNotAStringError
    from .xarray import plot_curve
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "ArrayNameIsEmptyStringError",
        "ArrayNameNotAStringError",
        "plot_curve",
    ]
