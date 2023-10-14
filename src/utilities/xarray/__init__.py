from __future__ import annotations

from .xarray import DataArray0
from .xarray import DataArray1
from .xarray import DataArray2
from .xarray import DataArray3
from .xarray import DataArrayB
from .xarray import DataArrayB0
from .xarray import DataArrayB1
from .xarray import DataArrayB2
from .xarray import DataArrayB3
from .xarray import DataArrayDns
from .xarray import DataArrayDns0
from .xarray import DataArrayDns1
from .xarray import DataArrayDns2
from .xarray import DataArrayDns3
from .xarray import DataArrayF
from .xarray import DataArrayF0
from .xarray import DataArrayF1
from .xarray import DataArrayF2
from .xarray import DataArrayF3
from .xarray import DataArrayI
from .xarray import DataArrayI0
from .xarray import DataArrayI1
from .xarray import DataArrayI2
from .xarray import DataArrayI3
from .xarray import DataArrayO
from .xarray import DataArrayO0
from .xarray import DataArrayO1
from .xarray import DataArrayO2
from .xarray import DataArrayO3

__all__ = [
    "DataArray0",
    "DataArray1",
    "DataArray2",
    "DataArray3",
    "DataArrayB",
    "DataArrayB0",
    "DataArrayB1",
    "DataArrayB2",
    "DataArrayB3",
    "DataArrayDns",
    "DataArrayDns0",
    "DataArrayDns1",
    "DataArrayDns2",
    "DataArrayDns3",
    "DataArrayF",
    "DataArrayF0",
    "DataArrayF1",
    "DataArrayF2",
    "DataArrayF3",
    "DataArrayI",
    "DataArrayI0",
    "DataArrayI1",
    "DataArrayI2",
    "DataArrayI3",
    "DataArrayO",
    "DataArrayO0",
    "DataArrayO1",
    "DataArrayO2",
    "DataArrayO3",
]


try:
    from .numbagg import ewma
    from .numbagg import exp_moving_sum
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["ewma", "exp_moving_sum"]
