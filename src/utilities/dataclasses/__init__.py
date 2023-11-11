from __future__ import annotations

from utilities.dataclasses.dataclasses import Dummy

__all__ = [
    "Dummy",
]


try:
    from utilities.dataclasses.xarray import rename_data_arrays
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "rename_data_arrays",
    ]
