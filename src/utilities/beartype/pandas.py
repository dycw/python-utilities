from beartype.vale import IsAttr
from beartype.vale import IsEqual

from utilities.pandas import Int64
from utilities.pandas import boolean
from utilities.pandas import string

DTypeBoolean = IsAttr["dtype", IsEqual[boolean]]
DTypeInt64 = IsAttr["dtype", IsEqual[Int64]]
DTypeString = IsAttr["dtype", IsEqual[string]]
