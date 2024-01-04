from __future__ import annotations

from collections.abc import Callable, Iterable
from functools import cache, wraps
from os import environ
from pathlib import Path
from sys import modules
from typing import Any

from utilities.atomicwrites import writer
from utilities.datetime import UTC, duration_to_float, get_now
from utilities.git import valid_path_repo
from utilities.hashlib import md5_hash
from utilities.pathvalidate import valid_path
from utilities.platform import (
    IS_LINUX,
    IS_MAC,
    IS_NOT_LINUX,
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    IS_WINDOWS,
)
from utilities.types import Duration, IterableStrs, PathLike

try:  # WARNING: this package cannot use unguarded `pytest` imports
    from _pytest.config import Config
    from _pytest.config.argparsing import Parser
    from _pytest.python import Function
    from pytest import mark, skip
except ModuleNotFoundError:  # pragma: no cover
    from typing import Any as Config
    from typing import Any as Function
    from typing import Any as Parser

    mark = (
        skip
    ) = (
        skipif_windows
    ) = (
        skipif_mac
    ) = skipif_linux = skipif_not_windows = skipif_not_mac = skipif_not_linux = None
else:
    skipif_windows = mark.skipif(IS_WINDOWS, reason="Skipped for Windows")
    skipif_mac = mark.skipif(IS_MAC, reason="Skipped for Mac")
    skipif_linux = mark.skipif(IS_LINUX, reason="Skipped for Linux")
    skipif_not_windows = mark.skipif(IS_NOT_WINDOWS, reason="Skipped for non-Windows")
    skipif_not_mac = mark.skipif(IS_NOT_MAC, reason="Skipped for non-Mac")
    skipif_not_linux = mark.skipif(IS_NOT_LINUX, reason="Skipped for non-Linux")


def add_pytest_addoption(parser: Parser, options: IterableStrs, /) -> None:
    """Add the `--slow`, etc options to pytest.

    Usage:

        def pytest_addoption(parser):
            add_pytest_addoption(parser, ["slow"])
    """
    for opt in options:
        _ = parser.addoption(
            f"--{opt}",
            action="store_true",
            default=False,
            help=f"run tests marked {opt!r}",
        )


def add_pytest_collection_modifyitems(
    config: Config, items: Iterable[Function], options: IterableStrs, /
) -> None:
    """Add the @mark.skips as necessary.

    Usage:

        def pytest_collection_modifyitems(config, items):
            add_pytest_collection_modifyitems(config, items, ["slow"])
    """
    options = list(options)
    missing = {opt for opt in options if not config.getoption(f"--{opt}")}
    for item in items:
        opts_on_item = [opt for opt in options if opt in item.keywords]
        if (len(missing & set(opts_on_item)) >= 1) and (  # pragma: no cover
            mark is not None
        ):
            flags = [f"--{opt}" for opt in opts_on_item]
            joined = " ".join(flags)
            _ = item.add_marker(mark.skip(reason=f"pass {joined}"))


def add_pytest_configure(config: Config, options: Iterable[tuple[str, str]], /) -> None:
    """Add the `--slow`, etc markers to pytest.

    Usage:
        def pytest_configure(config):
            add_pytest_configure(config, [("slow", "slow to run")])
    """
    for opt, desc in options:
        _ = config.addinivalue_line("markers", f"{opt}: mark test as {desc}")


def is_pytest() -> bool:
    """Check if `pytest` is running."""

    return "pytest" in modules


def throttle(
    *, root: PathLike | None = None, duration: Duration = 1.0, on_try: bool = False
) -> Any:
    """Throttle a test. On success by default, on try otherwise."""

    if root is None:
        root_use = valid_path_repo(".pytest_cache", "throttle")
    else:
        root_use = valid_path(root)

    def wrapper(func: Callable[..., Any], /) -> Callable[..., Any]:
        """Decorator to throttle a test function/method."""

        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            """The throttled test function/method."""
            test = environ["PYTEST_CURRENT_TEST"]
            if (path := valid_path(root_use, _throttle_md5_hash(test))).exists():
                with path.open(mode="r") as fh:
                    contents = fh.read()
                prev = float(contents)
            else:
                prev = None
            now = get_now(tz=UTC).timestamp()
            if (
                (skip is not None)
                and (prev is not None)
                and ((now - prev) < duration_to_float(duration))
            ):
                skip(reason=f"{test} throttled")
            if on_try:
                _throttle_write(path, now)
                return func(*args, **kwargs)
            out = func(*args, **kwargs)
            _throttle_write(path, now)
            return out

        return wrapped

    return wrapper


@cache
def _throttle_md5_hash(text: str, /) -> str:
    return md5_hash(text)


def _throttle_write(path: Path, now: float, /) -> None:
    with writer(path, overwrite=True) as temp, temp.open(mode="w") as fh:
        _ = fh.write(str(now))


__all__ = [
    "add_pytest_addoption",
    "add_pytest_collection_modifyitems",
    "add_pytest_configure",
    "is_pytest",
    "skipif_linux",
    "skipif_mac",
    "skipif_not_linux",
    "skipif_not_mac",
    "skipif_not_windows",
    "skipif_windows",
    "throttle",
]
