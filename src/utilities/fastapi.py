import re
from collections.abc import Callable
from typing import Any

from fastapi import APIRouter as _APIRouter
from fastapi.types import DecoratedCallable
from typing_extensions import override

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
