from __future__ import annotations

from re import compile

from utilities.errors import redirect_error
from utilities.more_itertools import OneError, one


def extract_group(pattern: str, text: str, /) -> str:
    """Extract a group.

    The regex must have 1 capture group, and this must match exactly once.
    """
    compiled = compile(pattern)
    if compiled.groups == 1:
        results = compiled.findall(text)
        with redirect_error(OneError, ExtractGroupError(f"{pattern=}, {text=}")):
            return one(results)
    msg = f"{pattern=}, {text=}"
    raise ExtractGroupError(msg)


class ExtractGroupError(Exception):
    ...


def extract_groups(pattern: str, text: str, /) -> list[str]:
    """Extract multiple groups.

    The regex may have any number of capture groups, and they must collectively
    match exactly once.
    """
    compiled = compile(pattern)
    if (n_groups := compiled.groups) >= 1:
        results = compiled.findall(text)
        if len(results) == 1:
            return results if n_groups == 1 else list(results[0])
    msg = f"{pattern=}, {text=}"
    raise ExtractGroupsError(msg)


class ExtractGroupsError(Exception):
    ...


__all__ = ["ExtractGroupError", "ExtractGroupsError", "extract_group", "extract_groups"]
