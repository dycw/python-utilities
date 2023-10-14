from __future__ import annotations

from .dataclasses import Dummy

__all__ = ["Dummy"]


try:
    from .xarray import rename_data_arrays
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["rename_data_arrays"]
