import builtins
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import contextmanager
from os import getenv
from pathlib import Path
from re import search
from string import ascii_letters
from string import printable
from typing import Any
from typing import Optional
from typing import Protocol
from typing import TypeVar
from typing import cast
from typing import overload

from beartype import beartype
from hypothesis import Verbosity
from hypothesis import assume
from hypothesis import settings
from hypothesis.strategies import DrawFn
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import characters
from hypothesis.strategies import composite
from hypothesis.strategies import lists
from hypothesis.strategies import text
from hypothesis.strategies import uuids

from utilities.hypothesis.typing import MaybeSearchStrategy
from utilities.tempfile import TemporaryDirectory
from utilities.tempfile import gettempdir
from utilities.text import ensure_str


@contextmanager
@beartype
def assume_does_not_raise(
    *exceptions: type[Exception],
    match: Optional[str] = None,
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


_MDF = TypeVar("_MDF")


class _MaybeDrawFn(Protocol):
    @overload
    def __call__(self, obj: SearchStrategy[_MDF], /) -> _MDF:
        ...

    @overload
    def __call__(self, obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        ...

    def __call__(self, obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        raise NotImplementedError(obj)  # pragma: no cover


def lift_draw(draw: DrawFn, /) -> _MaybeDrawFn:
    """Lift the `draw` function to handle non-`SearchStrategy` types."""

    @beartype
    def func(obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        return draw(obj) if isinstance(obj, SearchStrategy) else obj

    return func


_TLFL = TypeVar("_TLFL")


@composite
@beartype
def lists_fixed_length(
    _draw: Any,
    strategy: SearchStrategy[_TLFL],
    size: MaybeSearchStrategy[int],
    /,
    *,
    unique: MaybeSearchStrategy[bool] = False,
    sorted: MaybeSearchStrategy[bool] = False,  # noqa: A002
) -> list[_TLFL]:
    """Strategy for generating lists of a fixed length."""
    draw = lift_draw(_draw)
    size_ = draw(size)
    elements = draw(
        lists(strategy, min_size=size_, max_size=size_, unique=draw(unique)),
    )
    if draw(sorted):
        return builtins.sorted(cast(Iterable[Any], elements))
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
        "debug",
        max_examples=10,
        verbosity=Verbosity.verbose,
        **kwargs,
    )
    settings.load_profile(getenv("HYPOTHESIS_PROFILE", "default"))


@composite
@beartype
def temp_dirs(_draw: Any, /) -> TemporaryDirectory:
    """Search strategy for temporary directories."""
    dir_ = gettempdir().joinpath("hypothesis")
    dir_.mkdir(exist_ok=True)
    uuid = _draw(uuids())
    return TemporaryDirectory(prefix=f"{uuid}__", dir=dir_.as_posix())


@composite
@beartype
def temp_paths(_draw: Any, /) -> Path:
    """Search strategy for paths to temporary directories."""
    temp_dir = _draw(temp_dirs())
    root = temp_dir.name
    cls = type(root)

    class SubPath(cls):
        _temp_dir = temp_dir

    return SubPath(root)


@beartype
def text_ascii(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
) -> SearchStrategy[str]:
    """Strategy for generating ASCII text."""
    return _draw_text(
        characters(whitelist_categories=[], whitelist_characters=ascii_letters),
        min_size=min_size,
        max_size=max_size,
    )


@beartype
def text_clean(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
) -> SearchStrategy[str]:
    """Strategy for generating clean text."""
    return _draw_text(
        characters(blacklist_categories=["Z", "C"]),
        min_size=min_size,
        max_size=max_size,
    )


@beartype
def text_printable(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
) -> SearchStrategy[str]:
    """Strategy for generating printable text."""
    return _draw_text(
        characters(whitelist_categories=[], whitelist_characters=printable),
        min_size=min_size,
        max_size=max_size,
    )


@composite
def _draw_text(
    _draw: DrawFn,
    alphabet: MaybeSearchStrategy[str],
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
) -> str:
    draw = lift_draw(_draw)
    return draw(
        text(alphabet, min_size=draw(min_size), max_size=draw(max_size)),
    )
