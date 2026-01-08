from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Self, override

import packaging._parser
from packaging.requirements import _parse_requirement
from packaging.specifiers import Specifier, SpecifierSet

if TYPE_CHECKING:
    from packaging._parser import MarkerList


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class ParsedRequirement:
    requirement: str
    _parsed: packaging._parser.ParsedRequirement

    @override
    def __str__(self) -> str:
        return str(SortedRequirement(self.requirement))

    @classmethod
    def new(cls, requirement: str, /) -> Self:
        return cls(requirement=requirement, _parsed=_parse_requirement(requirement))

    @property
    def extras(self) -> list[str]:
        return self._parsed.extras

    @property
    def marker(self) -> MarkerList | None:
        return self._parsed.marker

    @property
    def name(self) -> str:
        return self._parsed.name

    @property
    def specifier(self) -> str:
        return self._parsed.specifier

    @property
    def specifier_set(self) -> _CustomSpecifierSet:
        return _CustomSpecifierSet(_parse_requirement(self.requirement).specifier)

    @property
    def url(self) -> str:
        return self._parsed.url


class SortedRequirement(packaging.requirements.Requirement):
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


__all__ = ["ParsedRequirement"]
