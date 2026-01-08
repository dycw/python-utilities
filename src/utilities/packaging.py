from __future__ import annotations

from typing import override

from packaging.requirements import Requirement, _parse_requirement
from packaging.specifiers import Specifier, SpecifierSet


def format_requirement(requirement: str, /) -> str:
    return str(SortedRequirement(requirement))


class SortedRequirement(Requirement):
    @override
    def __init__(self, requirement_string: str) -> None:
        super().__init__(requirement_string)
        parsed = _parse_requirement(requirement_string)
        self.specifier = _CustomSpecifierSet(parsed.specifier)


class _CustomSpecifierSet(SpecifierSet):
    @override
    def __str__(self) -> str:
        specs = sorted(self._specs, key=self._key)
        return ", ".join(map(str, specs))

    def _key(self, spec: Specifier, /) -> int:
        return [">=", "<"].index(spec.operator)


__all__ = ["SortedRequirement", "format_requirement"]
