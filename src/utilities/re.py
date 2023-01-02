from re import compile

from beartype import beartype


@beartype
def extract_group(pattern: str, text: str, /) -> str:
    """Extract a group.

    The regex must have 1 capture group, and this must match exactly once.
    """
    if compile(pattern).groups <= 1:
        (result,) = extract_groups(pattern, text)
        return result
    raise MultipleCaptureGroupsError(pattern)


class MultipleCaptureGroupsError(ValueError):
    """Raised when multiple capture groups are found."""


@beartype
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


class NoCaptureGroupsError(ValueError):
    """Raised when no capture groups are found."""


class NoMatchesError(ValueError):
    """Raised when no matches are found."""


class MultipleMatchesError(ValueError):
    """Raised when multiple matches are found."""
