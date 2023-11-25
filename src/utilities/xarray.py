from __future__ import annotations

from utilities._xarray.common import (
    DataArray0,
    DataArray1,
    DataArray2,
    DataArray3,
    DataArrayB,
    DataArrayB0,
    DataArrayB1,
    DataArrayB2,
    DataArrayB3,
    DataArrayDns,
    DataArrayDns0,
    DataArrayDns1,
    DataArrayDns2,
    DataArrayDns3,
    DataArrayF,
    DataArrayF0,
    DataArrayF1,
    DataArrayF2,
    DataArrayF3,
    DataArrayI,
    DataArrayI0,
    DataArrayI1,
    DataArrayI2,
    DataArrayI3,
    DataArrayO,
    DataArrayO0,
    DataArrayO1,
    DataArrayO2,
    DataArrayO3,
)

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
    from utilities._xarray.numbagg import ewma, exp_moving_sum
except (ImportError, ModuleNotFoundError):  # pragma: no cover
    pass
else:
    __all__ += ["ewma", "exp_moving_sum"]
