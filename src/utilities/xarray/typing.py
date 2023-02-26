from typing import Annotated

from xarray import DataArray

from utilities.beartype.numpy import DTypeB
from utilities.beartype.numpy import DTypeDns
from utilities.beartype.numpy import DTypeF
from utilities.beartype.numpy import DTypeI
from utilities.beartype.numpy import DTypeO
from utilities.beartype.numpy import NDim0
from utilities.beartype.numpy import NDim1
from utilities.beartype.numpy import NDim2
from utilities.beartype.numpy import NDim3

# dtype
DataArrayB = Annotated[DataArray, DTypeB]
DataArrayDns = Annotated[DataArray, DTypeDns]
DataArrayF = Annotated[DataArray, DTypeF]
DataArrayI = Annotated[DataArray, DTypeI]
DataArrayO = Annotated[DataArray, DTypeO]

# ndim
DataArray0 = Annotated[DataArray, NDim0]
DataArray1 = Annotated[DataArray, NDim1]
DataArray2 = Annotated[DataArray, NDim2]
DataArray3 = Annotated[DataArray, NDim3]

# compound
DataArrayB0 = Annotated[DataArray, DTypeB & NDim0]
DataArrayDns0 = Annotated[DataArray, DTypeDns & NDim0]
DataArrayF0 = Annotated[DataArray, DTypeF & NDim0]
DataArrayI0 = Annotated[DataArray, DTypeI & NDim0]
DataArrayO0 = Annotated[DataArray, DTypeO & NDim0]

DataArrayB1 = Annotated[DataArray, DTypeB & NDim1]
DataArrayDns1 = Annotated[DataArray, DTypeDns & NDim1]
DataArrayF1 = Annotated[DataArray, DTypeF & NDim1]
DataArrayI1 = Annotated[DataArray, DTypeI & NDim1]
DataArrayO1 = Annotated[DataArray, DTypeO & NDim1]

DataArrayB2 = Annotated[DataArray, DTypeB & NDim2]
DataArrayDns2 = Annotated[DataArray, DTypeDns & NDim2]
DataArrayF2 = Annotated[DataArray, DTypeF & NDim2]
DataArrayI2 = Annotated[DataArray, DTypeI & NDim2]
DataArrayO2 = Annotated[DataArray, DTypeO & NDim2]

DataArrayB3 = Annotated[DataArray, DTypeB & NDim3]
DataArrayDns3 = Annotated[DataArray, DTypeDns & NDim3]
DataArrayF3 = Annotated[DataArray, DTypeF & NDim3]
DataArrayI3 = Annotated[DataArray, DTypeI & NDim3]
DataArrayO3 = Annotated[DataArray, DTypeO & NDim3]
