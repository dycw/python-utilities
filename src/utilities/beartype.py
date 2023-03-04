from collections.abc import Iterable
from typing import Annotated

from beartype.vale import IsAttr, IsEqual, IsInstance

IterableStrs = Annotated[Iterable[str], ~IsInstance[str]]


# ndim checkers
NDim0 = IsAttr["ndim", IsEqual[0]]
NDim1 = IsAttr["ndim", IsEqual[1]]
NDim2 = IsAttr["ndim", IsEqual[2]]
NDim3 = IsAttr["ndim", IsEqual[3]]
