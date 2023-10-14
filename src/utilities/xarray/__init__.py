from __future__ import annotations

from utilities.xarray.xarray import DataArray0
from utilities.xarray.xarray import DataArray1
from utilities.xarray.xarray import DataArray2
from utilities.xarray.xarray import DataArray3
from utilities.xarray.xarray import DataArrayB
from utilities.xarray.xarray import DataArrayB0
from utilities.xarray.xarray import DataArrayB1
from utilities.xarray.xarray import DataArrayB2
from utilities.xarray.xarray import DataArrayB3
from utilities.xarray.xarray import DataArrayDns
from utilities.xarray.xarray import DataArrayDns0
from utilities.xarray.xarray import DataArrayDns1
from utilities.xarray.xarray import DataArrayDns2
from utilities.xarray.xarray import DataArrayDns3
from utilities.xarray.xarray import DataArrayF
from utilities.xarray.xarray import DataArrayF0
from utilities.xarray.xarray import DataArrayF1
from utilities.xarray.xarray import DataArrayF2
from utilities.xarray.xarray import DataArrayF3
from utilities.xarray.xarray import DataArrayI
from utilities.xarray.xarray import DataArrayI0
from utilities.xarray.xarray import DataArrayI1
from utilities.xarray.xarray import DataArrayI2
from utilities.xarray.xarray import DataArrayI3
from utilities.xarray.xarray import DataArrayO
from utilities.xarray.xarray import DataArrayO0
from utilities.xarray.xarray import DataArrayO1
from utilities.xarray.xarray import DataArrayO2
from utilities.xarray.xarray import DataArrayO3

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
    from utilities.xarray.numbagg import ewma
    from utilities.xarray.numbagg import exp_moving_sum
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["ewma", "exp_moving_sum"]
