from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self, override

import packaging._parser
from packaging.requirements import Requirement, _parse_requirement
from packaging.specifiers import Specifier, SpecifierSet
from pytest import Mark

if TYPE_CHECKING:
    import packaging.requirements
    from packaging._parser import MarkerList


def format_requirement(requirement: str, /) -> str:
    return str(SortedRequirement(requirement))


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class ParsedRequirement:
    requirement: str
    _parsed: packaging._parser.ParsedRequirement

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
