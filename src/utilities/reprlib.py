from __future__ import annotations

from enum import Enum, StrEnum
from itertools import islice
from reprlib import (
    Repr,
    _possibly_sorted,  # pyright: ignore[reportAttributeAccessIssue]
)
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from utilities.functions import get_class_name

if TYPE_CHECKING:
    from collections.abc import Mapping


_REPR = Repr()
_REPR.maxother = 100


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


__all__ = ["custom_print", "custom_repr"]
