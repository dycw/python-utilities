from __future__ import annotations

import re
import reprlib
from dataclasses import dataclass, field
from inspect import signature
from itertools import islice
from reprlib import _possibly_sorted  # pyright: ignore[reportAttributeAccessIssue]
from typing import TYPE_CHECKING, Any

from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


@dataclass(repr=False)
class ReprLocals:
    """An object for `repr`ing local variables."""

    locals: Mapping[str, Any]
    func: Callable[..., Any]
    include_underscore: bool = field(default=False, kw_only=True)
    include_none: bool = field(default=False, kw_only=True)

    @override
    def __repr__(self) -> str:
        mapping = _filter_mapping(
            self.locals,
            func=self.func,
            include_underscore=self.include_underscore,
            include_none=self.include_none,
        )
        return _custom_mapping_repr(mapping)

    @override
    def __str__(self) -> str:
        return self.__repr__()


def _custom_mapping_repr(mapping: Mapping[str, Any], /) -> str:
    """Apply the custom representation to a mapping."""
    return ", ".join(f"{k}={_custom_repr(v)}" for k, v in mapping.items())


def _custom_repr(obj: Any, /) -> str:
    """Apply the custom representation."""
    return _CUSTOM_REPR.repr(obj)


class CustomRepr(reprlib.Repr):
    """Custom representation."""

    @override
    def repr_dict(self, x: Mapping[str, Any], level: int) -> str:
        n = len(x)
        if n == 0:
            return ""
        if level <= 0:
            return f"({self.fillvalue})"
        newlevel = level - 1
        repr1 = self.repr1
        pieces = []
        for key in islice(_possibly_sorted(x), self.maxdict):
            keyrepr = key if isinstance(key, str) else repr1(key, newlevel)
            valrepr = repr1(x[key], newlevel)
            pieces.append(f"{keyrepr}={valrepr}")
        if n > self.maxdict:
            pieces.append(self.fillvalue)
        return ", ".join(pieces)


_CUSTOM_REPR = CustomRepr()


_FILTER_MAPPING_REGEX = re.compile(r"^_")


def _filter_mapping(
    mapping: Mapping[str, Any],
    /,
    *,
    func: Callable[..., Any] | None = None,
    include_underscore: bool = False,
    include_none: bool = False,
) -> Mapping[str, Any]:
    """Filter a mapping."""
    if func is not None:
        params = set(signature(func).parameters)
        mapping = {k: v for k, v in mapping.items() if k in params}
    if not include_underscore:
        mapping = {
            k: v for k, v in mapping.items() if not _FILTER_MAPPING_REGEX.search(k)
        }
    if not include_none:
        mapping = {k: v for k, v in mapping.items() if v is not None}
    return mapping


__all__ = ["ReprLocals"]
