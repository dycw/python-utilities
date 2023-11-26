from __future__ import annotations

from typing import Annotated, TypeAlias

from xarray import DataArray

from utilities.numpy import (
    DTypeB,
    DTypeDns,
    DTypeF,
    DTypeI,
    DTypeO,
    NDim0,
    NDim1,
    NDim2,
    NDim3,
)

# annotations - dtype
DataArrayB: TypeAlias = Annotated[DataArray, DTypeB]
DataArrayDns: TypeAlias = Annotated[DataArray, DTypeDns]
DataArrayF: TypeAlias = Annotated[DataArray, DTypeF]
DataArrayI: TypeAlias = Annotated[DataArray, DTypeI]
DataArrayO: TypeAlias = Annotated[DataArray, DTypeO]

# annotations - ndim
DataArray0: TypeAlias = Annotated[DataArray, NDim0]
DataArray1: TypeAlias = Annotated[DataArray, NDim1]
DataArray2: TypeAlias = Annotated[DataArray, NDim2]
DataArray3: TypeAlias = Annotated[DataArray, NDim3]

# annotated; dtype & ndim
DataArrayB0: TypeAlias = Annotated[DataArray, DTypeB, NDim0]
DataArrayDns0: TypeAlias = Annotated[DataArray, DTypeDns, NDim0]
DataArrayF0: TypeAlias = Annotated[DataArray, DTypeF, NDim0]
DataArrayI0: TypeAlias = Annotated[DataArray, DTypeI, NDim0]
DataArrayO0: TypeAlias = Annotated[DataArray, DTypeO, NDim0]

DataArrayB1: TypeAlias = Annotated[DataArray, DTypeB, NDim1]
DataArrayDns1: TypeAlias = Annotated[DataArray, DTypeDns, NDim1]
DataArrayF1: TypeAlias = Annotated[DataArray, DTypeF, NDim1]
DataArrayI1: TypeAlias = Annotated[DataArray, DTypeI, NDim1]
DataArrayO1: TypeAlias = Annotated[DataArray, DTypeO, NDim1]

DataArrayB2: TypeAlias = Annotated[DataArray, DTypeB, NDim2]
DataArrayDns2: TypeAlias = Annotated[DataArray, DTypeDns, NDim2]
DataArrayF2: TypeAlias = Annotated[DataArray, DTypeF, NDim2]
DataArrayI2: TypeAlias = Annotated[DataArray, DTypeI, NDim2]
DataArrayO2: TypeAlias = Annotated[DataArray, DTypeO, NDim2]

DataArrayB3: TypeAlias = Annotated[DataArray, DTypeB, NDim3]
DataArrayDns3: TypeAlias = Annotated[DataArray, DTypeDns, NDim3]
DataArrayF3: TypeAlias = Annotated[DataArray, DTypeF, NDim3]
DataArrayI3: TypeAlias = Annotated[DataArray, DTypeI, NDim3]
DataArrayO3: TypeAlias = Annotated[DataArray, DTypeO, NDim3]


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
