from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Self, overload, override

import packaging._parser
import packaging.requirements
from packaging.requirements import _parse_requirement
from packaging.specifiers import Specifier, SpecifierSet

from utilities.iterables import OneEmptyError, one

if TYPE_CHECKING:
    from packaging._parser import MarkerList


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class Requirement:
    requirement: str
    _parsed_req: packaging._parser.ParsedRequirement
    _custom_req: _CustomRequirement

    def __getitem__(self, operator: str, /) -> str:
        return self.specifier_set[operator]

    @override
    def __str__(self) -> str:
        return str(self._custom_req)

    @classmethod
    def new(cls, requirement: str, /) -> Self:
        return cls(
            requirement=requirement,
            _parsed_req=_parse_requirement(requirement),
            _custom_req=_CustomRequirement(requirement),
        )

    @property
    def extras(self) -> list[str]:
        return self._parsed_req.extras

    @overload
    def get(self, operator: str, default: str, /) -> str: ...
    @overload
    def get(self, operator: str, default: None = None, /) -> str | None: ...
    def get(self, operator: str, default: str | None = None, /) -> str | None:
        return self.specifier_set.get(operator, default)

    @property
    def marker(self) -> MarkerList | None:
        return self._parsed_req.marker

    @property
    def name(self) -> str:
        return self._parsed_req.name

    @property
    def specifier(self) -> str:
        return self._parsed_req.specifier

    @property
    def specifier_set(self) -> _CustomSpecifierSet:
        return _CustomSpecifierSet(_parse_requirement(self.requirement).specifier)

    @property
    def url(self) -> str:
        return self._parsed_req.url


class _CustomRequirement(packaging.requirements.Requirement):
    @override
    def __init__(self, requirement_string: str) -> None:
        super().__init__(requirement_string)
        parsed = _parse_requirement(requirement_string)
        self.specifier = _CustomSpecifierSet(parsed.specifier)


class _CustomSpecifierSet(SpecifierSet):
    def __getitem__(self, operator: str, /) -> str:
        try:
            return one(s.version for s in self if s.operator == operator)
        except OneEmptyError:
            raise KeyError(operator) from None

    @override
    def __str__(self) -> str:
        specs = sorted(self._specs, key=self._sort_key)
        return ", ".join(map(str, specs))

    @overload
    def get(self, operator: str, default: str, /) -> str: ...
    @overload
    def get(self, operator: str, default: None = None, /) -> str | None: ...
    def get(self, operator: str, default: str | None = None, /) -> str | None:
        try:
            return self[operator]
        except KeyError:
            return default

    def _sort_key(self, spec: Specifier, /) -> int:
        return [">=", "<"].index(spec.operator)


__all__ = ["Requirement"]
