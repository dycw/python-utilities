from re import search

from beartype import beartype
from inflection import underscore


@beartype
def snake_case(text: str, /) -> str:
    """Convert text into `snake_case`."""

    text = text.replace(" ", "")
    text = "".join(c for c in text if str.isidentifier(c) or str.isdigit(c))
    while search("__", text):
        text = text.replace("__", "_")
    return underscore(text)
