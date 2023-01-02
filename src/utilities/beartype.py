from collections.abc import Iterable
from typing import Annotated

from beartype.vale import IsInstance

IterableStrs = Annotated[Iterable[str], ~IsInstance[str]]
