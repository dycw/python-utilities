import builtins
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from os import getenv
from pathlib import Path
from re import search
from string import ascii_letters
from string import printable
from typing import Any
from typing import TypeVar
from uuid import UUID

from beartype import beartype
from hypothesis import Verbosity
from hypothesis import assume
from hypothesis import settings
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import characters
from hypothesis.strategies import fixed_dictionaries
from hypothesis.strategies import just
from hypothesis.strategies import lists
from hypothesis.strategies import text
from hypothesis.strategies import tuples
from hypothesis.strategies import uuids

from utilities.hypothesis.typing import MaybeSearchStrategy
from utilities.tempfile import TemporaryDirectory
from utilities.tempfile import gettempdir
from utilities.text import ensure_str


@contextmanager
@beartype
def assume_does_not_raise(
    *exceptions: type[Exception], match: str | None = None
) -> Iterator[None]:
    """Assume a set of exceptions are not raised. Optionally filter on the
    string representation of the exception.
    """

    try:
        yield
    except exceptions as caught:
        if match is None:
            _ = assume(False)
        else:
            (msg,) = caught.args
            if search(match, ensure_str(msg)):
                _ = assume(False)
            else:
                raise


_TD = TypeVar("_TD")


@beartype
def draw_and_map(
    func: Callable[..., _TD], *args: Any, **kwargs: Any
) -> SearchStrategy[_TD]:
    """Draw the elements and then pass them into a transformer."""

    return _lift_args_and_kwargs(*args, **kwargs).map(partial(_apply, func))


@beartype
def draw_and_flatmap(
    func: Callable[..., SearchStrategy[_TD]], *args: Any, **kwargs: Any
) -> SearchStrategy[_TD]:
    """Draw the elements and then flatmap them into a transformer."""

    return _lift_args_and_kwargs(*args, **kwargs).flatmap(partial(_apply, func))


@beartype
def _lift_args_and_kwargs(
    *args: Any, **kwargs: Any
) -> SearchStrategy[tuple[tuple[Any, ...], dict[str, Any]]]:
    lifted_args = tuples(*map(_lift, args))
    lifted_kwargs = fixed_dictionaries({k: _lift(v) for k, v in kwargs.items()})
    return tuples(lifted_args, lifted_kwargs)


@beartype
def _lift(x: Any, /) -> SearchStrategy[Any]:
    return x if isinstance(x, SearchStrategy) else just(x)


@beartype
def _apply(
    func: Callable[..., _TD], x: tuple[tuple[Any, ...], dict[str, Any]], /
) -> _TD:
    args, kwargs = x
    return func(*args, **kwargs)


_TLFL = TypeVar("_TLFL")


@beartype
def lists_fixed_length(
    strategy: SearchStrategy[_TLFL],
    size: int,
    /,
    *,
    unique: bool = False,
    sorted: bool = False,
) -> SearchStrategy[list[_TLFL]]:
    """Strategy for generating lists of a fixed length."""

    return draw_and_map(
        _draw_lists_fixed_length,
        lists(strategy, min_size=size, max_size=size, unique=unique),
        sorted=sorted,
    )


@beartype
def _draw_lists_fixed_length(
    elements: list[Any], /, *, sorted: bool = False
) -> list[Any]:
    if sorted:
        return builtins.sorted(elements)
    else:
        return elements


@beartype
def setup_hypothesis_profiles() -> None:
    """Set up the hypothesis profiles."""

    kwargs = {
        "deadline": None,
        "print_blob": True,
        "report_multiple_bugs": False,
    }
    settings.register_profile("default", max_examples=100, **kwargs)
    settings.register_profile("dev", max_examples=10, **kwargs)
    settings.register_profile("ci", max_examples=1000, **kwargs)
    settings.register_profile(
        "debug", max_examples=10, verbosity=Verbosity.verbose, **kwargs
    )
    settings.load_profile(getenv("HYPOTHESIS_PROFILE", "default"))


@beartype
def temp_dirs() -> SearchStrategy[TemporaryDirectory]:
    """Search strategy for temporary directories."""

    dir = gettempdir().joinpath("hypothesis")
    dir.mkdir(exist_ok=True)
    return draw_and_map(_draw_temp_dirs, uuids(), dir)


@beartype
def _draw_temp_dirs(uuid: UUID, dir: Path, /) -> TemporaryDirectory:
    return TemporaryDirectory(prefix=f"{uuid}__", dir=dir.as_posix())


@beartype
def temp_paths() -> SearchStrategy[Path]:
    """Search strategy for paths to temporary directories."""

    return draw_and_map(_draw_temp_paths, temp_dirs())


@beartype
def _draw_temp_paths(temp_dir: TemporaryDirectory, /) -> Path:
    class SubPath(type(root := temp_dir.name)):
        _temp_dir = temp_dir

    return SubPath(root)


@beartype
def text_ascii(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> SearchStrategy[str]:
    """Strategy for generating ASCII text."""

    return draw_and_flatmap(
        _draw_text,
        characters(whitelist_categories=[], whitelist_characters=ascii_letters),
        min_size=min_size,
        max_size=max_size,
    )


@beartype
def text_clean(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> SearchStrategy[str]:
    """Strategy for generating clean text."""

    return draw_and_flatmap(
        _draw_text,
        characters(blacklist_categories=["Z", "C"]),
        min_size=min_size,
        max_size=max_size,
    )


@beartype
def text_printable(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> SearchStrategy[str]:
    """Strategy for generating printable text."""

    return draw_and_flatmap(
        _draw_text,
        characters(whitelist_categories=[], whitelist_characters=printable),
        min_size=min_size,
        max_size=max_size,
    )


@beartype
def _draw_text(
    alphabet: MaybeSearchStrategy[str],
    /,
    *,
    min_size: int = 0,
    max_size: int | None = None,
) -> SearchStrategy[str]:
    return text(alphabet, min_size=min_size, max_size=max_size)
