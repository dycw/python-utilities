from typing import Annotated
from typing import Any
from typing import cast

from numpy import bool_
from numpy import float64
from numpy import int64
from numpy import object_
from numpy.typing import NDArray

from utilities.beartype.numpy import NDim0
from utilities.beartype.numpy import NDim1
from utilities.beartype.numpy import NDim2
from utilities.beartype.numpy import NDim3
from utilities.numpy import datetime64D
from utilities.numpy import datetime64ns
from utilities.numpy import datetime64Y

# dtype
NDArrayB = NDArray[bool_]
NDArrayDD = NDArray[cast(Any, datetime64D)]
NDArrayDY = NDArray[cast(Any, datetime64Y)]
NDArrayDns = NDArray[cast(Any, datetime64ns)]
NDArrayF = NDArray[float64]
NDArrayI = NDArray[int64]
NDArrayO = NDArray[object_]

# ndim
NDArray0 = Annotated[NDArray[Any], NDim0]
NDArray1 = Annotated[NDArray[Any], NDim1]
NDArray2 = Annotated[NDArray[Any], NDim2]
NDArray3 = Annotated[NDArray[Any], NDim3]

# compound
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
