from __future__ import annotations

from functools import cache, wraps
from os import environ
from typing import TYPE_CHECKING, Any

from utilities.datetime import duration_to_float, get_now
from utilities.hashlib import md5_hash
from utilities.pathlib import ensure_path
from utilities.platform import (
    IS_LINUX,
    IS_MAC,
    IS_NOT_LINUX,
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    IS_WINDOWS,
)
from utilities.types import Duration, IterableStrs, PathLike, is_function_async
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

try:  # WARNING: this package cannot use unguarded `pytest` imports
    from _pytest.config import Config
    from _pytest.config.argparsing import Parser
    from _pytest.python import Function
    from pytest import mark, skip
except ModuleNotFoundError:  # pragma: no cover
    from typing import Any as Config
    from typing import Any as Function
    from typing import Any as Parser

    mark = skip = skipif_windows = skipif_mac = skipif_linux = skipif_not_windows = (
        skipif_not_mac
    ) = skipif_not_linux = None
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


def throttle(
    *,
    root: PathLike | None = None,
    duration: Duration = 1.0,
    on_try: bool = False,
    validate: bool = False,
) -> Any:
    """Throttle a test. On success by default, on try otherwise."""
    if root is None:
        root_use = ensure_path(".pytest_cache", "throttle", validate=validate)
    else:
        root_use = ensure_path(root, validate=validate)

    def wrapper(func: Callable[..., Any], /) -> Callable[..., Any]:
        """Throttle a test function/method."""
        if is_function_async(func):

            @wraps(func)
            async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
                """Call the throttled async test function/method."""
                path, now = _throttle_path_and_now(
                    root_use, duration=duration, validate=validate
                )
                if on_try:
                    _throttle_write(path, now)
                    return await func(*args, **kwargs)
                out = await func(*args, **kwargs)
                _throttle_write(path, now)
                return out

            return wrapped_async

        @wraps(func)
        def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
            """Call the throttled sync test function/method."""
            path, now = _throttle_path_and_now(
                root_use, duration=duration, validate=validate
            )
            if on_try:
                _throttle_write(path, now)
                return func(*args, **kwargs)
            out = func(*args, **kwargs)
            _throttle_write(path, now)
            return out

        return wrapped_sync

    return wrapper


def _throttle_path_and_now(
    root: Path, /, *, duration: Duration = 1.0, validate: bool = False
) -> tuple[Path, float]:
    test = environ["PYTEST_CURRENT_TEST"]
    path = ensure_path(root, _throttle_md5_hash(test), validate=validate)
    if path.exists():
        with path.open(mode="r") as fh:
            contents = fh.read()
        prev = float(contents)
    else:
        prev = None
    now = get_now(time_zone=UTC).timestamp()
    if (
        (skip is not None)
        and (prev is not None)
        and ((now - prev) < duration_to_float(duration))
    ):
        _ = skip(reason=f"{test} throttled")
    return path, now


@cache
def _throttle_md5_hash(text: str, /) -> str:
    return md5_hash(text)


def _throttle_write(path: Path, now: float, /) -> None:
    from utilities.atomicwrites import writer

    with writer(path, overwrite=True) as temp, temp.open(mode="w") as fh:
        _ = fh.write(str(now))


__all__ = [
    "add_pytest_addoption",
    "add_pytest_collection_modifyitems",
    "add_pytest_configure",
    "skipif_linux",
    "skipif_mac",
    "skipif_not_linux",
    "skipif_not_mac",
    "skipif_not_windows",
    "skipif_windows",
    "throttle",
]
