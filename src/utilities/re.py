from __future__ import annotations

from dataclasses import dataclass
from re import compile

from typing_extensions import override

from utilities.iterables import OneEmptyError, OneNonUniqueError, one


def extract_group(pattern: str, text: str, /, *, flags: int = 0) -> str:
    """Extract a group.

    The regex must have 1 capture group, and this must match exactly once.
    """
    compiled = compile(pattern, flags=flags)
    match compiled.groups:
        case 0:
            raise _ExtractGroupNoCaptureGroupsError(pattern=pattern, text=text)
        case 1:
            matches: list[str] = compiled.findall(text)
            try:
                return one(matches)
            except OneEmptyError:
                raise _ExtractGroupNoMatchesError(pattern=pattern, text=text) from None
            except OneNonUniqueError:
                raise _ExtractGroupMultipleMatchesError(
                    pattern=pattern, text=text, matches=matches
                ) from None
        case _:
            raise _ExtractGroupMultipleCaptureGroupsError(pattern=pattern, text=text)


@dataclass(kw_only=True)
class ExtractGroupError(Exception):
    pattern: str
    text: str


@dataclass(kw_only=True)
class _ExtractGroupMultipleCaptureGroupsError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must contain exactly one capture group; it had multiple"


@dataclass(kw_only=True)
class _ExtractGroupMultipleMatchesError(ExtractGroupError):
    matches: list[str]

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text} exactly once; matches were {self.matches}"


@dataclass(kw_only=True)
class _ExtractGroupNoCaptureGroupsError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must contain exactly one capture group; it had none".format(
            self.pattern
        )


@dataclass(kw_only=True)
class _ExtractGroupNoMatchesError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text}"


def extract_groups(pattern: str, text: str, /, *, flags: int = 0) -> list[str]:
    """Extract multiple groups.

    The regex may have any number of capture groups, and they must collectively
    match exactly once.
    """
    compiled = compile(pattern, flags=flags)
    if (n_groups := compiled.groups) == 0:
        raise _ExtractGroupsNoCaptureGroupsError(pattern=pattern, text=text)
    matches: list[str] = compiled.findall(text)
    match len(matches), n_groups:
        case 0, _:
            raise _ExtractGroupsNoMatchesError(pattern=pattern, text=text)
        case 1, 1:
            return matches
        case 1, _:
            return list(one(matches))
        case _:
            raise _ExtractGroupsMultipleMatchesError(
                pattern=pattern, text=text, matches=matches
            )


@dataclass(kw_only=True)
class ExtractGroupsError(Exception):
    pattern: str
    text: str


@dataclass(kw_only=True)
class _ExtractGroupsMultipleMatchesError(ExtractGroupsError):
    matches: list[str]

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text} exactly once; matches were {self.matches}"


@dataclass(kw_only=True)
class _ExtractGroupsNoCaptureGroupsError(ExtractGroupsError):
    pattern: str
    text: str

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must contain at least one capture group"


@dataclass(kw_only=True)
class _ExtractGroupsNoMatchesError(ExtractGroupsError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text}"


__all__ = ["ExtractGroupError", "ExtractGroupsError", "extract_group", "extract_groups"]
