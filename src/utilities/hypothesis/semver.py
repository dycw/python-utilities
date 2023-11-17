from __future__ import annotations

from hypothesis import assume
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import DrawFn, composite, integers
from semver import Version

from utilities.hypothesis.hypothesis import (
    MaybeSearchStrategy,
    lift_draw,
    lists_fixed_length,
)


@composite
def versions(  # noqa: PLR0912
    _draw: DrawFn,
    /,
    *,
    min_version: MaybeSearchStrategy[Version | None] = None,
    max_version: MaybeSearchStrategy[Version | None] = None,
) -> Version:
    """Strategy for generating `Version`s."""
    draw = lift_draw(_draw)
    min_version_, max_version_ = (draw(mv) for mv in (min_version, max_version))
    if isinstance(min_version_, Version) and isinstance(max_version_, Version):
        if min_version_ > max_version_:
            msg = f"{min_version_=}, {max_version_=}"
            raise InvalidArgument(msg)
        major = draw(integers(min_version_.major, max_version_.major))
        minor, patch = draw(lists_fixed_length(integers(min_value=0), 2))
        version = Version(major=major, minor=minor, patch=patch)
        _ = assume(min_version_ <= version <= max_version_)
        return version
    if isinstance(min_version_, Version) and (max_version_ is None):
        major = draw(integers(min_value=min_version_.major))
        if major > min_version_.major:
            minor, patch = draw(lists_fixed_length(integers(min_value=0), 2))
        else:
            minor = draw(integers(min_version_.minor))
            if minor > min_version_.minor:
                patch = draw(integers(min_value=0))  # pragma: no cover
            else:
                patch = draw(integers(min_value=min_version_.patch))
    elif (min_version_ is None) and isinstance(max_version_, Version):
        major = draw(integers(0, max_version_.major))
        if major < max_version_.major:
            minor, patch = draw(lists_fixed_length(integers(min_value=0), 2))
        else:
            minor = draw(integers(0, max_version_.minor))
            if minor < max_version_.minor:
                patch = draw(integers(min_value=0))  # pragma: no cover
            else:
                patch = draw(integers(0, max_version_.patch))
    elif (min_version_ is None) and (max_version_ is None):
        major, minor, patch = draw(lists_fixed_length(integers(min_value=0), 3))
    else:
        msg = "Invalid case"  # pragma: no cover
        raise RuntimeError(msg)  # pragma: no cover
    return Version(major=major, minor=minor, patch=patch)


__all__ = ["versions"]
