from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter as _APIRouter
from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable

    from fastapi.types import DecoratedCallable

_PATTERN = re.compile(r"(^/$)|(^.+[^\/]$)")


class APIRouter(_APIRouter):
    """Subclass which handles paths with & without trailing slashes."""

    @override
    def api_route(  # type: ignore[]
        self, *, path: str, include_in_schema: bool = True, **kwargs: Any
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        """N/A."""
        if _PATTERN.search(path):
            return super().api_route(
                path, include_in_schema=include_in_schema, **kwargs
            )
        msg = f"Invalid route: {path}"
        raise ValueError(msg)


__all__ = ["APIRouter"]
