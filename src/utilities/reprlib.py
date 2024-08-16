from __future__ import annotations

import re
import reprlib
from dataclasses import dataclass, field
from inspect import signature
from itertools import islice
from reprlib import _possibly_sorted
from typing import TYPE_CHECKING, Any

from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

_DESCRIBE_MAPPING_REGEX = re.compile(r"^_")


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


def _custom_repr(obj: Any, /) -> str:
    """Apply the custom representation."""
    return _CUSTOM_REPR.repr(obj)


def _custom_mapping_repr(mapping: Mapping[str, Any], /) -> str:
    """Apply the custom representation to a mapping."""
    return ", ".join(f"{k}={_custom_repr(v)}" for k, v in mapping.items())


@dataclass(repr=False)
class ReprLocals:
    """An object for `repr`ing local variables."""

    locals: Callable[[], Mapping[str, Any]]
    func: Callable[..., Any] | None = field(default=None, kw_only=True)
    include_underscore: bool = field(default=False, kw_only=True)
    include_none: bool = field(default=False, kw_only=True)

    def __repr__(self) -> str:
        mapping = self.locals()
        mapping = {k: v for k, v in mapping.items() if v is not None}
        return reprlib.repr(mapping)


def _describe_mapping(
    mapping: Mapping[str, Any],
    /,
    *,
    func: Callable[..., Any] | None = None,
    include_underscore: bool = False,
    include_none: bool = False,
) -> str:
    """Describe a mapping."""
    if not include_underscore:
        mapping = {
            k: v for k, v in mapping.items() if not _DESCRIBE_MAPPING_REGEX.search(k)
        }
    if not include_none:
        mapping = {k: v for k, v in mapping.items() if v is not None}
    if func is not None:
        params = set(signature(func).parameters)
        mapping = {k: v for k, v in mapping.items() if k in params}
    items = (f"{k}={v}" for k, v in mapping.items())
    return ", ".join(items)
