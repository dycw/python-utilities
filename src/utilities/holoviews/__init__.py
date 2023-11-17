from __future__ import annotations

from utilities.holoviews.holoviews import apply_opts, relabel_plot, save_plot

__all__ = ["apply_opts", "relabel_plot", "save_plot"]


try:
    from utilities.holoviews.xarray import (
        ArrayNameIsEmptyStringError,
        ArrayNameNotAStringError,
        plot_curve,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["ArrayNameIsEmptyStringError", "ArrayNameNotAStringError", "plot_curve"]
