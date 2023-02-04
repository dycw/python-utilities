from beartype.vale import IsAttr, IsEqual

from utilities.numpy import datetime64ns

DTypeBool = IsAttr["dtype", IsEqual[bool]]
DTypeDatetime64ns = IsAttr["dtype", IsEqual[datetime64ns]]
DTypeFloat = IsAttr["dtype", IsEqual[float]]
DTypeInt = IsAttr["dtype", IsEqual[int]]
DTypeObject = IsAttr["dtype", IsEqual[object]]
