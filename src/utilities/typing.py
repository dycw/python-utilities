from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from typing import Any

SequenceStrs = list[str] | tuple[str, ...]
IterableStrs = SequenceStrs | AbstractSet[str] | Mapping[str, Any]
Number = float | int


__all__ = ["IterableStrs", "Number", "SequenceStrs"]
