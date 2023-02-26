from beartype.vale import IsAttr
from beartype.vale import IsEqual

from utilities.numpy import datetime64D
from utilities.numpy import datetime64ns
from utilities.numpy import datetime64Y

# dtype
DTypeB = IsAttr["dtype", IsEqual[bool]]
DTypeDD = IsAttr["dtype", IsEqual[datetime64D]]
DTypeDY = IsAttr["dtype", IsEqual[datetime64Y]]
DTypeDns = IsAttr["dtype", IsEqual[datetime64ns]]
DTypeF = IsAttr["dtype", IsEqual[float]]
DTypeI = IsAttr["dtype", IsEqual[int]]
DTypeO = IsAttr["dtype", IsEqual[object]]

# ndim
NDim0 = IsAttr["ndim", IsEqual[0]]
NDim1 = IsAttr["ndim", IsEqual[1]]
NDim2 = IsAttr["ndim", IsEqual[2]]
NDim3 = IsAttr["ndim", IsEqual[3]]
