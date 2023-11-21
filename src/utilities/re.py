from __future__ import annotations

from re import compile


def extract_group(pattern: str, text: str, /) -> str:
    """Extract a group.

    The regex must have 1 capture group, and this must match exactly once.
    """
    if compile(pattern).groups <= 1:
        (result,) = extract_groups(pattern, text)
        return result
    raise MultipleCaptureGroupsError(pattern)


class MultipleCaptureGroupsError(Exception):
    """Raised when multiple capture groups are found."""


def extract_groups(pattern: str, text: str, /) -> list[str]:
    """Extract multiple groups.

    The regex may have any number of capture groups, and they must collectively
    match exactly once.
    """
    compiled = compile(pattern)
    if (n_groups := compiled.groups) == 0:
        raise NoCaptureGroupsError(pattern)
    results = compiled.findall(text)
    msg = f"{pattern=}, {text=}"
    if (n_results := len(results)) == 0:
        raise NoMatchesError(msg)
    if n_results == 1:
        if n_groups == 1:
            return results
        (result,) = results
        return list(result)
    raise MultipleMatchesError(msg)


class NoCaptureGroupsError(Exception):
    """Raised when no capture groups are found."""


class NoMatchesError(Exception):
    """Raised when no matches are found."""


class MultipleMatchesError(Exception):
    """Raised when multiple matches are found."""


__all__ = [
    "extract_group",
    "extract_groups",
    "MultipleCaptureGroupsError",
    "MultipleMatchesError",
    "NoCaptureGroupsError",
    "NoMatchesError",
]
