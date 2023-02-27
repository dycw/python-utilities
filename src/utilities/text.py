from re import search
from re import sub
from textwrap import dedent
from typing import Any

from beartype import beartype


@beartype
def ensure_str(x: Any, /) -> str:
    """Ensure an object is a string."""
    if isinstance(x, str):
        return x
    msg = f"{x=}"
    raise NotAStringError(msg)


class NotAStringError(TypeError):
    """Raised when an object is not a string."""


@beartype
def snake_case(text: str, /) -> str:
    """Convert text into snake case."""
    text = text.replace(" ", "")
    text = "".join(c for c in text if str.isidentifier(c) or str.isdigit(c))
    while search("__", text):
        text = text.replace("__", "_")
    text = sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = sub(r"([a-z\d])([A-Z])", r"\1_\2", text)
    text = text.replace("-", "_")
    return text.lower()


@beartype
def strip_and_dedent(text: str, /) -> str:
    """Strip and dedent a string."""
    return dedent(text.strip("\n")).strip("\n")
