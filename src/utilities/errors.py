from __future__ import annotations

from dataclasses import dataclass
from typing import override


@dataclass(kw_only=True, slots=True)
class ImpossibleCaseError(Exception):
    case: list[str]

    @override
    def __str__(self) -> str:
        desc = ", ".join(self.case)
        return f"Case must be possible: {desc}."


__all__ = ["ImpossibleCaseError"]
