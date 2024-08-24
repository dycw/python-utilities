from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from inspect import signature
from itertools import islice
from reprlib import (
    Repr,
    _possibly_sorted,  # pyright: ignore[reportAttributeAccessIssue]
)
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from utilities.functions import get_class_name

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


_REPR = Repr()
_REPR.maxother = 100


@dataclass(repr=False)
class ReprLocals:
    """An object for `repr`ing local variables."""

    locals: Mapping[str, Any]
    func: Callable[..., Any]
    include_underscore: bool = field(default=False, kw_only=True)
    include_none: bool = field(default=False, kw_only=True)
    fillvalue: str = field(default=_REPR.fillvalue, kw_only=True)
    maxlevel: int = field(default=_REPR.maxlevel, kw_only=True)
    maxtuple: int = field(default=_REPR.maxtuple, kw_only=True)
    maxlist: int = field(default=_REPR.maxlist, kw_only=True)
    maxarray: int = field(default=_REPR.maxarray, kw_only=True)
    maxdict: int = field(default=_REPR.maxdict, kw_only=True)
    maxset: int = field(default=_REPR.maxset, kw_only=True)
    maxfrozenset: int = field(default=_REPR.maxfrozenset, kw_only=True)
    maxdeque: int = field(default=_REPR.maxdeque, kw_only=True)
    maxstring: int = field(default=_REPR.maxstring, kw_only=True)
    maxlong: int = field(default=_REPR.maxlong, kw_only=True)
    maxother: int = field(default=_REPR.maxother, kw_only=True)

    @override
    def __repr__(self) -> str:
        mapping = _filter_mapping(
            self.locals,
            func=self.func,
            include_underscore=self.include_underscore,
            include_none=self.include_none,
        )
        return _custom_mapping_repr(
            mapping,
            fillvalue=self.fillvalue,
            maxlevel=self.maxlevel,
            maxtuple=self.maxtuple,
            maxlist=self.maxlist,
            maxarray=self.maxarray,
            maxdict=self.maxdict,
            maxset=self.maxset,
            maxfrozenset=self.maxfrozenset,
            maxdeque=self.maxdeque,
            maxstring=self.maxstring,
            maxlong=self.maxlong,
            maxother=self.maxother,
        )

    @override
    def __str__(self) -> str:
        return self.__repr__()


def _custom_mapping_repr(
    mapping: Mapping[str, Any],
    /,
    *,
    fillvalue: str = _REPR.fillvalue,
    maxlevel: int = _REPR.maxlevel,
    maxtuple: int = _REPR.maxtuple,
    maxlist: int = _REPR.maxlist,
    maxarray: int = _REPR.maxarray,
    maxdict: int = _REPR.maxdict,
    maxset: int = _REPR.maxset,
    maxfrozenset: int = _REPR.maxfrozenset,
    maxdeque: int = _REPR.maxdeque,
    maxstring: int = _REPR.maxstring,
    maxlong: int = _REPR.maxlong,
    maxother: int = _REPR.maxother,
) -> str:
    """Apply the custom representation to a mapping."""
    values = (
        custom_repr(
            v,
            fillvalue=fillvalue,
            maxlevel=maxlevel,
            maxtuple=maxtuple,
            maxlist=maxlist,
            maxarray=maxarray,
            maxdict=maxdict,
            maxset=maxset,
            maxfrozenset=maxfrozenset,
            maxdeque=maxdeque,
            maxstring=maxstring,
            maxlong=maxlong,
            maxother=maxother,
        )
        for v in mapping.values()
    )
    return ", ".join(f"{k}={v}" for k, v in zip(mapping, values, strict=True))


def custom_repr(
    obj: Any,
    /,
    *,
    fillvalue: str = _REPR.fillvalue,
    maxlevel: int = _REPR.maxlevel,
    maxtuple: int = _REPR.maxtuple,
    maxlist: int = _REPR.maxlist,
    maxarray: int = _REPR.maxarray,
    maxdict: int = _REPR.maxdict,
    maxset: int = _REPR.maxset,
    maxfrozenset: int = _REPR.maxfrozenset,
    maxdeque: int = _REPR.maxdeque,
    maxstring: int = _REPR.maxstring,
    maxlong: int = _REPR.maxlong,
    maxother: int = _REPR.maxother,
) -> str:
    """Apply the custom representation."""
    repr_obj = _CustomRepr(
        fillvalue=fillvalue,
        maxlevel=maxlevel,
        maxtuple=maxtuple,
        maxlist=maxlist,
        maxarray=maxarray,
        maxdict=maxdict,
        maxset=maxset,
        maxfrozenset=maxfrozenset,
        maxdeque=maxdeque,
        maxstring=maxstring,
        maxlong=maxlong,
        maxother=maxother,
    )
    return repr_obj.repr(obj)


class _CustomRepr(Repr):
    """Custom representation."""

    def __init__(
        self,
        *,
        fillvalue: str = _REPR.fillvalue,
        maxlevel: int = _REPR.maxlevel,
        maxtuple: int = _REPR.maxtuple,
        maxlist: int = _REPR.maxlist,
        maxarray: int = _REPR.maxarray,
        maxdict: int = _REPR.maxdict,
        maxset: int = _REPR.maxset,
        maxfrozenset: int = _REPR.maxfrozenset,
        maxdeque: int = _REPR.maxdeque,
        maxstring: int = _REPR.maxstring,
        maxlong: int = _REPR.maxlong,
        maxother: int = _REPR.maxother,
    ) -> None:
        super().__init__()
        self.fillvalue = fillvalue
        self.maxlevel = maxlevel
        self.maxtuple = maxtuple
        self.maxlist = maxlist
        self.maxarray = maxarray
        self.maxdict = maxdict
        self.maxset = maxset
        self.maxfrozenset = maxfrozenset
        self.maxdeque = maxdeque
        self.maxstring = maxstring
        self.maxlong = maxlong
        self.maxother = maxother

    @override
    def repr1(self, x: Any, level: int) -> str:
        if isinstance(x, Enum):
            if isinstance(x, StrEnum):
                return super().repr1(x.value, level)
            cls_name = get_class_name(x)
            return super().repr1(f"{cls_name}.{x.name}", level)
        return super().repr1(x, level)

    def repr_DataFrame(self, x: Any, level: int) -> str:  # noqa: N802
        try:
            from polars import DataFrame
        except ModuleNotFoundError:  # pragma: no cover
            return self.repr_instance(x, level)
        if isinstance(x, DataFrame):
            return repr(x)
        return self.repr_instance(x, level)

    def repr_Series(self, x: Any, level: int) -> str:  # noqa: N802
        try:
            from polars import Series
        except ModuleNotFoundError:  # pragma: no cover
            return self.repr_instance(x, level)
        if isinstance(x, Series):
            return repr(x)
        return self.repr_instance(x, level)

    @override
    def repr_dict(self, x: Mapping[str, Any], level: int) -> str:
        n = len(x)
        if n == 0:
            return ""
        if level <= 0:
            return f"({self.fillvalue})"  # pragma: no cover
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


_CUSTOM_REPR = _CustomRepr()


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


def custom_print(
    obj: Any,
    /,
    *,
    fillvalue: str = _REPR.fillvalue,
    maxlevel: int = _REPR.maxlevel,
    maxtuple: int = _REPR.maxtuple,
    maxlist: int = _REPR.maxlist,
    maxarray: int = _REPR.maxarray,
    maxdict: int = _REPR.maxdict,
    maxset: int = _REPR.maxset,
    maxfrozenset: int = _REPR.maxfrozenset,
    maxdeque: int = _REPR.maxdeque,
    maxstring: int = _REPR.maxstring,
    maxlong: int = _REPR.maxlong,
    maxother: int = _REPR.maxother,
) -> None:
    """Print the custom representation."""
    text = custom_repr(
        obj,
        fillvalue=fillvalue,
        maxlevel=maxlevel,
        maxtuple=maxtuple,
        maxlist=maxlist,
        maxarray=maxarray,
        maxdict=maxdict,
        maxset=maxset,
        maxfrozenset=maxfrozenset,
        maxdeque=maxdeque,
        maxstring=maxstring,
        maxlong=maxlong,
        maxother=maxother,
    )
    try:
        import rich
    except ModuleNotFoundError:  # pragma: no cover
        print(text)  # noqa: T201
    else:
        rich.print(text)


__all__ = ["ReprLocals", "custom_print", "custom_repr"]
