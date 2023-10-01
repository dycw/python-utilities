from __future__ import annotations

from beartype.vale import IsAttr
from beartype.vale import IsEqual

NDim0 = IsAttr["ndim", IsEqual[0]]
NDim1 = IsAttr["ndim", IsEqual[1]]
NDim2 = IsAttr["ndim", IsEqual[2]]
NDim3 = IsAttr["ndim", IsEqual[3]]
