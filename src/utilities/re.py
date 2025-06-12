from __future__ import annotations

import re
from dataclasses import dataclass
from re import Pattern
from typing import TYPE_CHECKING, assert_never, override

if TYPE_CHECKING:
    from utilities.types import PatternLike


def ensure_pattern(pattern: PatternLike, /, *, flags: int = 0) -> Pattern[str]:
    """Ensure a pattern is returned."""
    match pattern:
        case Pattern():
            return pattern
        case str():
            return re.compile(pattern, flags=flags)
        case _ as never:
            assert_never(never)


##


def extract_group(pattern: PatternLike, text: str, /, *, flags: int = 0) -> str:
    """Extract a group.

    The regex must have 1 capture group, and this must match exactly once.
    """
    pattern_use = ensure_pattern(pattern, flags=flags)
    match pattern_use.groups:
        case 0:
            raise _ExtractGroupNoCaptureGroupsError(pattern=pattern_use, text=text)
        case 1:
            matches: list[str] = pattern_use.findall(text)
            match len(matches):
                case 0:
                    raise _ExtractGroupNoMatchesError(
                        pattern=pattern_use, text=text
                    ) from None
                case 1:
                    return matches[0]
                case _:
                    raise _ExtractGroupMultipleMatchesError(
                        pattern=pattern_use, text=text, matches=matches
                    ) from None
        case _:
            raise _ExtractGroupMultipleCaptureGroupsError(
                pattern=pattern_use, text=text
            )


@dataclass(kw_only=True, slots=True)
class ExtractGroupError(Exception):
    pattern: Pattern[str]
    text: str


@dataclass(kw_only=True, slots=True)
class _ExtractGroupMultipleCaptureGroupsError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must contain exactly one capture group; it had multiple"


@dataclass(kw_only=True, slots=True)
class _ExtractGroupMultipleMatchesError(ExtractGroupError):
    matches: list[str]

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text} exactly once; matches were {self.matches}"


@dataclass(kw_only=True, slots=True)
class _ExtractGroupNoCaptureGroupsError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must contain exactly one capture group; it had none".format(
            self.pattern
        )


@dataclass(kw_only=True, slots=True)
class _ExtractGroupNoMatchesError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text}"


##


def extract_groups(pattern: PatternLike, text: str, /, *, flags: int = 0) -> list[str]:
    """Extract multiple groups.

    The regex may have any number of capture groups, and they must collectively
    match exactly once.
    """
    pattern_use = ensure_pattern(pattern, flags=flags)
    if (n_groups := pattern_use.groups) == 0:
        raise _ExtractGroupsNoCaptureGroupsError(pattern=pattern_use, text=text)
    matches: list[str] = pattern_use.findall(text)
    match len(matches), n_groups:
        case 0, _:
            raise _ExtractGroupsNoMatchesError(pattern=pattern_use, text=text)
        case 1, 1:
            return matches
        case 1, _:
            return list(matches[0])
        case _:
            raise _ExtractGroupsMultipleMatchesError(
                pattern=pattern_use, text=text, matches=matches
            )


@dataclass(kw_only=True, slots=True)
class ExtractGroupsError(Exception):
    pattern: Pattern[str]
    text: str


@dataclass(kw_only=True, slots=True)
class _ExtractGroupsMultipleMatchesError(ExtractGroupsError):
    matches: list[str]

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text} exactly once; matches were {self.matches}"


@dataclass(kw_only=True, slots=True)
class _ExtractGroupsNoCaptureGroupsError(ExtractGroupsError):
    pattern: Pattern[str]
    text: str

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must contain at least one capture group"


@dataclass(kw_only=True, slots=True)
class _ExtractGroupsNoMatchesError(ExtractGroupsError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text}"


__all__ = [
    "ExtractGroupError",
    "ExtractGroupsError",
    "ensure_pattern",
    "extract_group",
    "extract_groups",
]
