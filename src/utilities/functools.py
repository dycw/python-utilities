from __future__ import annotations

from functools import partial as _partial
from typing import Any, TypeVar

from typing_extensions import override

_T = TypeVar("_T")


class partial(_partial[_T]):  # noqa: N801
    """Partial which accepts Ellipsis for positional arguments."""

    @override
    def __call__(self, *args: Any, **kwargs: Any) -> _T:
        iter_args = iter(args)
        head = (next(iter_args) if arg is ... else arg for arg in self.args)
        return self.func(*head, *iter_args, **{**self.keywords, **kwargs})


__all__ = ["partial"]
