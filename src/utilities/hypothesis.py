from __future__ import annotations

import datetime as dt
from collections.abc import Hashable, Iterable, Iterator
from contextlib import contextmanager
from os import environ, getenv
from pathlib import Path
from re import search
from string import printable
from subprocess import run
from typing import TypedDict, cast

from hypothesis import HealthCheck, Phase, Verbosity, assume, settings
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import (
    DrawFn,
    SearchStrategy,
    booleans,
    characters,
    composite,
    datetimes,
    integers,
    just,
    none,
)
from semver import Version

from utilities._hypothesis.common import (
    MaybeSearchStrategy,
    Shape,
    draw_text,
    floats_extra,
    lift_draw,
    lists_fixed_length,
    temp_dirs,
    temp_paths,
    text_ascii,
)
from utilities.datetime import UTC
from utilities.pathlib import temp_cwd
from utilities.text import ensure_str


@contextmanager
def assume_does_not_raise(
    *exceptions: type[Exception], match: str | None = None
) -> Iterator[None]:
    """Assume a set of exceptions are not raised.

    Optionally filter on the string representation of the exception.
    """
    try:
        yield
    except exceptions as caught:
        if match is None:
            _ = assume(condition=False)
        else:
            (msg,) = caught.args
            if search(match, ensure_str(msg)):
                _ = assume(condition=False)
            else:
                raise


@composite
def datetimes_utc(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] = dt.datetime.min,
    max_value: MaybeSearchStrategy[dt.datetime] = dt.datetime.max,
) -> dt.datetime:
    """Strategy for generating datetimes with the UTC timezone."""
    draw = lift_draw(_draw)
    return draw(
        datetimes(
            min_value=draw(min_value).replace(tzinfo=None),
            max_value=draw(max_value).replace(tzinfo=None),
            timezones=just(UTC),
        )
    )


@composite
def git_repos(
    _draw: DrawFn, /, *, branch: MaybeSearchStrategy[str | None] = None
) -> Path:
    draw = lift_draw(_draw)
    path = draw(temp_paths())
    with temp_cwd(path):
        _ = run(["git", "init"], check=True)  # noqa: S603, S607
        _ = run(
            ["git", "config", "user.name", "User"],  # noqa: S603, S607
            check=True,
        )
        _ = run(
            ["git", "config", "user.email", "a@z.com"],  # noqa: S603, S607
            check=True,
        )
        file = path.joinpath("file")
        file.touch()
        file_str = str(file)
        _ = run(["git", "add", file_str], check=True)  # noqa: S603, S607
        _ = run(["git", "commit", "-m", "add"], check=True)  # noqa: S603, S607
        _ = run(["git", "rm", file_str], check=True)  # noqa: S603, S607
        _ = run(["git", "commit", "-m", "rm"], check=True)  # noqa: S603, S607
        if (branch := draw(branch)) is not None:
            _ = run(
                ["git", "checkout", "-b", branch],  # noqa: S603, S607
                check=True,
            )
    return path


def hashables() -> SearchStrategy[Hashable]:
    """Strategy for generating hashable elements."""
    return booleans() | integers() | none() | text_ascii()


_MAX_EXAMPLES: str = "MAX_EXAMPLES"
_NO_SHRINK: str = "NO_SHRINK"


def setup_hypothesis_profiles(
    *,
    max_examples: str = _MAX_EXAMPLES,
    no_shrink: str = _NO_SHRINK,
    suppress_health_check: Iterable[HealthCheck] = (),
) -> None:
    """Set up the hypothesis profiles."""

    def yield_phases() -> Iterator[Phase]:
        yield Phase.explicit
        yield Phase.reuse
        yield Phase.generate
        yield Phase.target
        if not bool(int(getenv(no_shrink, default="0"))):
            yield Phase.shrink

    phases = set(yield_phases())

    class Kwargs(TypedDict, total=False):
        verbosity: Verbosity

    for name, default_max_examples, verbosity in [
        ("dev", 10, None),
        ("default", 100, None),
        ("ci", 1000, None),
        ("debug", 10, Verbosity.verbose),
    ]:
        try:
            env_var = environ[max_examples]
        except KeyError:
            max_examples_use = default_max_examples
        else:
            max_examples_use = int(env_var)
        verbosity_use = cast(
            Kwargs, {} if verbosity is None else {"verbosity": verbosity}
        )
        settings.register_profile(
            name,
            max_examples=max_examples_use,
            **verbosity_use,
            phases=phases,
            report_multiple_bugs=True,
            suppress_health_check=suppress_health_check,
            deadline=None,
            print_blob=True,
        )
    settings.load_profile(getenv("HYPOTHESIS_PROFILE", "default"))


@composite
def slices(
    _draw: DrawFn,
    iter_len: int,
    /,
    *,
    slice_len: MaybeSearchStrategy[int | None] = None,
) -> slice:
    """Strategy for generating continuous slices from an iterable."""
    draw = lift_draw(_draw)
    if (slice_len_ := draw(slice_len)) is None:
        slice_len_ = draw(integers(0, iter_len))
    elif not 0 <= slice_len_ <= iter_len:
        msg = f"Slice length {slice_len_} exceeds iterable length {iter_len}"
        raise InvalidArgument(msg)
    start = draw(integers(0, iter_len - slice_len_))
    stop = start + slice_len_
    return slice(start, stop)


def text_clean(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[str]:
    """Strategy for generating clean text."""
    return draw_text(
        characters(blacklist_categories=["Z", "C"]),
        min_size=min_size,
        max_size=max_size,
        disallow_na=disallow_na,
    )


def text_printable(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[str]:
    """Strategy for generating printable text."""
    return draw_text(
        characters(whitelist_categories=[], whitelist_characters=printable),
        min_size=min_size,
        max_size=max_size,
        disallow_na=disallow_na,
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


__all__ = [
    "MaybeSearchStrategy",
    "Shape",
    "assume_does_not_raise",
    "datetimes_utc",
    "draw_text",
    "floats_extra",
    "git_repos",
    "hashables",
    "lift_draw",
    "lists_fixed_length",
    "setup_hypothesis_profiles",
    "slices",
    "temp_dirs",
    "temp_paths",
    "text_ascii",
    "text_clean",
    "text_printable",
    "versions",
]


try:
    from utilities._hypothesis.luigi import namespace_mixins
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["namespace_mixins"]


try:
    from utilities._hypothesis.numpy import (
        bool_arrays,
        concatenated_arrays,
        datetime64_arrays,
        datetime64_dtypes,
        datetime64_indexes,
        datetime64_kinds,
        datetime64_units,
        datetime64D_indexes,
        datetime64s,
        datetime64us_indexes,
        float_arrays,
        int32s,
        int64s,
        int_arrays,
        str_arrays,
        uint32s,
        uint64s,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "bool_arrays",
        "concatenated_arrays",
        "datetime64D_indexes",
        "datetime64_arrays",
        "datetime64_dtypes",
        "datetime64_indexes",
        "datetime64_kinds",
        "datetime64_units",
        "datetime64s",
        "datetime64us_indexes",
        "float_arrays",
        "int32s",
        "int64s",
        "int_arrays",
        "str_arrays",
        "uint32s",
        "uint64s",
    ]


try:
    from utilities._hypothesis.pandas import (
        dates_pd,
        datetimes_pd,
        indexes,
        int_indexes,
        str_indexes,
        timestamps,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "dates_pd",
        "datetimes_pd",
        "indexes",
        "int_indexes",
        "str_indexes",
        "timestamps",
    ]


try:
    from utilities._hypothesis.sqlalchemy import sqlite_engines
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["sqlite_engines"]


try:
    from utilities._hypothesis.xarray import (
        bool_data_arrays,
        dicts_of_indexes,
        float_data_arrays,
        int_data_arrays,
        str_data_arrays,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "bool_data_arrays",
        "dicts_of_indexes",
        "float_data_arrays",
        "int_data_arrays",
        "str_data_arrays",
    ]
