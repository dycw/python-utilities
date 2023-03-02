from typing import Any

from beartype import beartype


class _Meta(type):
    """Metaclass for the sentinel."""

    instance: Any = None

    @beartype
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls.instance is None:
            cls.instance = super().__call__(*args, **kwargs)
        return cls.instance


_REPR = "<sentinel>"


class Sentinel(metaclass=_Meta):
    """Base class for the sentinel object."""

    @beartype
    def __repr__(self) -> str:
        return _REPR

    @beartype
    def __str__(self) -> str:
        return repr(self)


sentinel = Sentinel()
