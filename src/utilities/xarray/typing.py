from typing import Annotated

from typing_extensions import TypeAlias
from xarray import DataArray

from utilities.beartype.numpy import (
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

# dtype
DataArrayB: TypeAlias = Annotated[DataArray, DTypeB]
DataArrayDns: TypeAlias = Annotated[DataArray, DTypeDns]
DataArrayF: TypeAlias = Annotated[DataArray, DTypeF]
DataArrayI: TypeAlias = Annotated[DataArray, DTypeI]
DataArrayO: TypeAlias = Annotated[DataArray, DTypeO]

# ndim
DataArray0: TypeAlias = Annotated[DataArray, NDim0]
DataArray1: TypeAlias = Annotated[DataArray, NDim1]
DataArray2: TypeAlias = Annotated[DataArray, NDim2]
DataArray3: TypeAlias = Annotated[DataArray, NDim3]

# compound
DataArrayB0: TypeAlias = Annotated[DataArray, DTypeB & NDim0]
DataArrayDns0: TypeAlias = Annotated[DataArray, DTypeDns & NDim0]
DataArrayF0: TypeAlias = Annotated[DataArray, DTypeF & NDim0]
DataArrayI0: TypeAlias = Annotated[DataArray, DTypeI & NDim0]
DataArrayO0: TypeAlias = Annotated[DataArray, DTypeO & NDim0]

DataArrayB1: TypeAlias = Annotated[DataArray, DTypeB & NDim1]
DataArrayDns1: TypeAlias = Annotated[DataArray, DTypeDns & NDim1]
DataArrayF1: TypeAlias = Annotated[DataArray, DTypeF & NDim1]
DataArrayI1: TypeAlias = Annotated[DataArray, DTypeI & NDim1]
DataArrayO1: TypeAlias = Annotated[DataArray, DTypeO & NDim1]

DataArrayB2: TypeAlias = Annotated[DataArray, DTypeB & NDim2]
DataArrayDns2: TypeAlias = Annotated[DataArray, DTypeDns & NDim2]
DataArrayF2: TypeAlias = Annotated[DataArray, DTypeF & NDim2]
DataArrayI2: TypeAlias = Annotated[DataArray, DTypeI & NDim2]
DataArrayO2: TypeAlias = Annotated[DataArray, DTypeO & NDim2]

DataArrayB3: TypeAlias = Annotated[DataArray, DTypeB & NDim3]
DataArrayDns3: TypeAlias = Annotated[DataArray, DTypeDns & NDim3]
DataArrayF3: TypeAlias = Annotated[DataArray, DTypeF & NDim3]
DataArrayI3: TypeAlias = Annotated[DataArray, DTypeI & NDim3]
DataArrayO3: TypeAlias = Annotated[DataArray, DTypeO & NDim3]
