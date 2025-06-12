from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, override

from typed_settings.converters import TSConverter
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    ZonedDateTime,
)

if TYPE_CHECKING:
    from collections.abc import Callable


_T = TypeVar("_T")


class ExtendedTSConverter(TSConverter):
    """An extension of the TSConverter for custom types."""

    @override
    def __init__(
        self,
        *,
        resolve_paths: bool = True,
        strlist_sep: str | Callable[[str], list] | None = ":",
    ) -> None:
        super().__init__(resolve_paths=resolve_paths, strlist_sep=strlist_sep)
        cases: list[tuple[type[Any], Callable[..., Any]]] = [
            (Date, Date.parse_common_iso),
            (DateDelta, DateDelta.parse_common_iso),
            (DateTimeDelta, DateTimeDelta.parse_common_iso),
            (PlainDateTime, PlainDateTime.parse_common_iso),
            (Time, Time.parse_common_iso),
            (TimeDelta, TimeDelta.parse_common_iso),
            (ZonedDateTime, ZonedDateTime.parse_common_iso),
        ]
        extras = {cls: _make_converter(cls, func) for cls, func in cases}
        self.scalar_converters |= extras


def _make_converter(
    cls: type[_T], parser: Callable[[str], _T], /
) -> Callable[[Any, type[Any]], Any]:
    def hook(value: _T | str, _: type[_T] = cls, /) -> Any:
        if not isinstance(value, (cls, str)):  # pragma: no cover
            msg = f"Invalid type {type(value).__name__!r}; expected '{cls.__name__}' or 'str'"
            raise TypeError(msg)
        if isinstance(value, str):
            return parser(value)
        return value

    return hook


__all__ = ["ExtendedTSConverter"]
