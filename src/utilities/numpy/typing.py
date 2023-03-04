from typing import Annotated, Any, cast

from beartype.vale import IsAttr, IsEqual
from numpy import bool_, dtype, float64, int64, object_
from numpy.typing import NDArray

from utilities.beartype import NDim0, NDim1, NDim2, NDim3

# dtypes
datetime64D = dtype("datetime64[D]")  # noqa: N816
datetime64Y = dtype("datetime64[Y]")  # noqa: N816
datetime64ns = dtype("datetime64[ns]")

# dtype checkers
DTypeB = IsAttr["dtype", IsEqual[bool]]
DTypeDD = IsAttr["dtype", IsEqual[datetime64D]]
DTypeDY = IsAttr["dtype", IsEqual[datetime64Y]]
DTypeDns = IsAttr["dtype", IsEqual[datetime64ns]]
DTypeF = IsAttr["dtype", IsEqual[float]]
DTypeI = IsAttr["dtype", IsEqual[int]]
DTypeO = IsAttr["dtype", IsEqual[object]]

# annotated; dtype
NDArrayB = NDArray[bool_]
NDArrayDD = NDArray[cast(Any, datetime64D)]
NDArrayDY = NDArray[cast(Any, datetime64Y)]
NDArrayDns = NDArray[cast(Any, datetime64ns)]
NDArrayF = NDArray[float64]
NDArrayI = NDArray[int64]
NDArrayO = NDArray[object_]

# annotated; ndim
NDArray0 = Annotated[NDArray[Any], NDim0]
NDArray1 = Annotated[NDArray[Any], NDim1]
NDArray2 = Annotated[NDArray[Any], NDim2]
NDArray3 = Annotated[NDArray[Any], NDim3]

# annotated; dtype & ndim
NDArrayB0 = Annotated[NDArrayB, NDim0]
NDArrayDD0 = Annotated[NDArrayDD, NDim0]
NDArrayDY0 = Annotated[NDArrayDY, NDim0]
NDArrayDns0 = Annotated[NDArrayDns, NDim0]
NDArrayF0 = Annotated[NDArrayF, NDim0]
NDArrayI0 = Annotated[NDArrayI, NDim0]
NDArrayO0 = Annotated[NDArrayO, NDim0]

NDArrayB1 = Annotated[NDArrayB, NDim1]
NDArrayDD1 = Annotated[NDArrayDD, NDim1]
NDArrayDY1 = Annotated[NDArrayDY, NDim1]
NDArrayDns1 = Annotated[NDArrayDns, NDim1]
NDArrayF1 = Annotated[NDArrayF, NDim1]
NDArrayI1 = Annotated[NDArrayI, NDim1]
NDArrayO1 = Annotated[NDArrayO, NDim1]

NDArrayB2 = Annotated[NDArrayB, NDim2]
NDArrayDD2 = Annotated[NDArrayDD, NDim2]
NDArrayDY2 = Annotated[NDArrayDY, NDim2]
NDArrayDns2 = Annotated[NDArrayDns, NDim2]
NDArrayF2 = Annotated[NDArrayF, NDim2]
NDArrayI2 = Annotated[NDArrayI, NDim2]
NDArrayO2 = Annotated[NDArrayO, NDim2]

NDArrayB3 = Annotated[NDArrayB, NDim3]
NDArrayDD3 = Annotated[NDArrayDD, NDim3]
NDArrayDY3 = Annotated[NDArrayDY, NDim3]
NDArrayDns3 = Annotated[NDArrayDns, NDim3]
NDArrayF3 = Annotated[NDArrayF, NDim3]
NDArrayI3 = Annotated[NDArrayI, NDim3]
NDArrayO3 = Annotated[NDArrayO, NDim3]
