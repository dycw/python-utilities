from beartype.vale import IsAttr, IsEqual

from utilities.pandas import Int64, boolean, string

DTypeBoolean = IsAttr["dtype", IsEqual[boolean]]
DTypeInt64 = IsAttr["dtype", IsEqual[Int64]]
DTypeString = IsAttr["dtype", IsEqual[string]]
