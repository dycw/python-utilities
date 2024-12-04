from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# Truth = Literal["true", "false"]  # in 3.12, use type TruthLit = ...
type TruthLit = Literal["true", "false"]  # in 3.12, use type TruthLit = ...


@dataclass(kw_only=True, slots=True)
class Example:
    x
