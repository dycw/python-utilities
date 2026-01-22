from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self, overload, override

import packaging._parser
import packaging.requirements
from packaging.requirements import _parse_requirement
from packaging.specifiers import Specifier, SpecifierSet

from utilities.core import OneEmptyError, one

if TYPE_CHECKING:
    from packaging._parser import MarkerList


@dataclass(order=True, unsafe_hash=True, slots=True)
class Requirement:
    requirement: str
    _parsed_req: packaging._parser.ParsedRequirement = field(init=False, repr=False)
    _custom_req: _CustomRequirement = field(init=False, repr=False)

    def __getitem__(self, operator: str, /) -> str:
        return self.specifier_set[operator]

    def __post_init__(self) -> None:
        self._parsed_req = _parse_requirement(self.requirement)
        self._custom_req = _CustomRequirement(self.requirement)

    @override
    def __str__(self) -> str:
        return str(self._custom_req)

    def drop(self, operator: str, /) -> Self:
        return type(self)(str(self._custom_req.drop(operator)))

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

    def replace(self, operator: str, version: str | None, /) -> Self:
        return type(self)(str(self._custom_req.replace(operator, version)))

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
    specifier: _CustomSpecifierSet

    @override
    def __init__(self, requirement_string: str) -> None:
        super().__init__(requirement_string)
        parsed = _parse_requirement(requirement_string)
        self.specifier = _CustomSpecifierSet(parsed.specifier)  # pyright: ignore[reportIncompatibleVariableOverride]

    def drop(self, operator: str, /) -> Self:
        new = type(self)(super().__str__())
        new.specifier = self.specifier.drop(operator)
        return new

    def replace(self, operator: str, version: str | None, /) -> Self:
        new = type(self)(super().__str__())
        new.specifier = self.specifier.replace(operator, version)
        return new


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

    def drop(self, operator: str, /) -> Self:
        if any(s.operator == operator for s in self):
            return type(self)(s for s in self if s.operator != operator)
        raise KeyError(operator)

    @overload
    def get(self, operator: str, default: str, /) -> str: ...
    @overload
    def get(self, operator: str, default: None = None, /) -> str | None: ...
    def get(self, operator: str, default: str | None = None, /) -> str | None:
        try:
            return self[operator]
        except KeyError:
            return default

    def replace(self, operator: str, version: str | None, /) -> Self:
        specifiers = [s for s in self if s.operator != operator]
        if version is not None:
            specifiers.append(Specifier(spec=f"{operator}{version}"))
        return type(self)(specifiers)

    def _sort_key(self, spec: Specifier, /) -> int:
        return ["==", "!=", "~=", ">", ">=", "<", "<="].index(spec.operator)


__all__ = ["Requirement"]
