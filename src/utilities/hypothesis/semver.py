from typing import Any, Optional

from beartype import beartype
from hypothesis import assume
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import composite, integers
from semver import Version

from utilities.hypothesis import lift_draw, lists_fixed_length
from utilities.hypothesis.typing import MaybeSearchStrategy


@composite
@beartype
def versions(  # noqa: PLR0912
    _draw: Any,
    /,
    *,
    min_version: MaybeSearchStrategy[Optional[Version]] = None,
    max_version: MaybeSearchStrategy[Optional[Version]] = None,
) -> Version:
    """Strategy for generating `Version`s."""
    draw = lift_draw(_draw)
    min_version_, max_version_ = map(draw, [min_version, max_version])
    if (min_version_ is None) and (max_version_ is None):
        major, minor, patch = draw(lists_fixed_length(integers(min_value=0), 3))
    elif (min_version_ is not None) and (max_version_ is None):
        major = draw(integers(min_value=min_version_.major))
        if major > min_version_.major:
            minor, patch = draw(lists_fixed_length(integers(min_value=0), 2))
        else:
            minor = draw(integers(min_version_.minor))
            if minor > min_version_.minor:
                patch = draw(integers(min_value=0))  # pragma: no cover
            else:
                patch = draw(integers(min_value=min_version_.patch))
    elif (min_version_ is None) and (max_version_ is not None):
        major = draw(integers(0, max_version_.major))
        if major < max_version_.major:
            minor, patch = draw(lists_fixed_length(integers(min_value=0), 2))
        else:
            minor = draw(integers(0, max_version_.minor))
            if minor < max_version_.minor:
                patch = draw(integers(min_value=0))  # pragma: no cover
            else:
                patch = draw(integers(0, max_version_.patch))
    elif (min_version_ is not None) and (max_version_ is not None):
        if min_version_ > max_version_:
            msg = f"{min_version_=}, {max_version_=}"
            raise InvalidArgument(msg)
        major = draw(integers(min_version_.major, max_version_.major))
        minor, patch = draw(lists_fixed_length(integers(min_value=0), 2))
        version = Version(major=major, minor=minor, patch=patch)
        _ = assume(min_version_ <= version <= max_version_)
        return version
    else:
        msg = "Invalid case"  # pragma: no cover
        raise RuntimeError(msg)  # pragma: no cover
    return Version(major=major, minor=minor, patch=patch)
