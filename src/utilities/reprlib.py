from __future__ import annotations

from enum import Enum, StrEnum
from functools import partial
from itertools import islice
from re import sub
from reprlib import (
    Repr,
    _possibly_sorted,  # pyright: ignore[reportAttributeAccessIssue]
)
from types import (
    BuiltinFunctionType,
    FunctionType,
    MethodDescriptorType,
    MethodType,
    MethodWrapperType,
    WrapperDescriptorType,
)
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from utilities.datetime import is_zoned_datetime
from utilities.functions import get_class_name, get_func_name
from utilities.zoneinfo import get_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Mapping


_REPR = Repr()
_REPR.maxother *= 2


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
        if isinstance(x, type):
            return get_class_name(x)
        if isinstance(
            x,
            BuiltinFunctionType
            | FunctionType
            | MethodType
            | MethodDescriptorType
            | MethodWrapperType
            | WrapperDescriptorType
            | partial,
        ):
            return get_func_name(x)
        return self._drop_object_address(super().repr1(x, level))

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

    def repr_date(self, x: Any, level: int) -> str:
        try:
            from utilities.whenever import serialize_date
        except ModuleNotFoundError:  # pragma: no cover
            return self.repr_instance(x, level)
        return serialize_date(x)

    def repr_datetime(self, x: Any, level: int) -> str:
        if is_zoned_datetime(x):
            try:
                from utilities.whenever import serialize_zoned_datetime
            except ModuleNotFoundError:  # pragma: no cover
                return self.repr_instance(x, level)
            return serialize_zoned_datetime(x)
        try:
            from utilities.whenever import serialize_local_datetime
        except ModuleNotFoundError:  # pragma: no cover
            return self.repr_instance(x, level)
        return serialize_local_datetime(x)

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

    def repr_time(self, x: Any, level: int) -> str:
        try:
            from utilities.whenever import serialize_time
        except ModuleNotFoundError:  # pragma: no cover
            return self.repr_instance(x, level)
        return serialize_time(x)

    def repr_timedelta(self, x: Any, level: int) -> str:
        try:
            from utilities.whenever import serialize_timedelta
        except ModuleNotFoundError:  # pragma: no cover
            return self.repr_instance(x, level)
        return serialize_timedelta(x)

    def repr_ZoneInfo(self, x: Any, _: int) -> str:  # noqa: N802
        return get_time_zone_name(x)

    def _drop_object_address(self, text: str, /) -> str:
        return sub(" at 0x[0-9a-fA-F]+", "", text)


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
