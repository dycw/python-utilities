from __future__ import annotations

from typing import Any

from typing_extensions import Self, override


class _Meta(type):
    """Metaclass for the sentinel."""

    instance: Any = None

    @override
    def __call__(cls: Any, *args: Any, **kwargs: Any) -> Any:
        if cls.instance is None:
            cls.instance = super().__call__(*args, **kwargs)
        return cls.instance


_REPR = "<sentinel>"


class Sentinel(metaclass=_Meta):
    """Base class for the sentinel object."""

    @override
    def __repr__(self: Self) -> str:
        return _REPR

    @override
    def __str__(self: Self) -> str:
        return repr(self)


sentinel = Sentinel()


__all__ = ["Sentinel", "sentinel"]
