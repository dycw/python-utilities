from __future__ import annotations

from typing import Any

from typing_extensions import override


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


__all__ = ["SENTINEL_REPR", "Sentinel", "sentinel"]
