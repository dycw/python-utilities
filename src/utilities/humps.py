from __future__ import annotations

from re import search

from humps import decamelize


def snake_case(text: str, /) -> str:
    """Convert text into snake case."""
    text = decamelize(text)
    while search("__", text):
        text = text.replace("__", "_")
    return text.lower()


__all__ = ["snake_case"]
