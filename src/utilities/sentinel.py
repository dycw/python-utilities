from __future__ import annotations

from dataclasses import dataclass
from re import IGNORECASE, search
from typing import Any, override


class _Meta(type):
    """Metaclass for the sentinel."""

    instance: Any = None

    @override
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls.instance is None:
            cls.instance = super().__call__(*args, **kwargs)
        return cls.instance


SENTINEL_REPR = "<sentinel>"


class Sentinel(metaclass=_Meta):
    """Base class for the sentinel object."""

    @override
    def __repr__(self) -> str:
        return SENTINEL_REPR

    @override
    def __str__(self) -> str:
        return repr(self)


sentinel = Sentinel()


##


def parse_sentinel(text: str, /) -> Sentinel:
    """Parse text into the Sentinel value."""
    if search("^(|sentinel|<sentinel>)$", text, flags=IGNORECASE):
        return sentinel
    raise ParseSentinelError(text=text)


@dataclass(kw_only=True, slots=True)
class ParseSentinelError(Exception):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse sentinel value; got {self.text!r}"


__all__ = [
    "SENTINEL_REPR",
    "ParseSentinelError",
    "Sentinel",
    "parse_sentinel",
    "sentinel",
]
