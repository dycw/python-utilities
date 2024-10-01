from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from inspect import iscoroutinefunction
from itertools import chain
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from utilities.datetime import duration_to_float, get_now
from utilities.functools import cache
from utilities.hashlib import md5_hash
from utilities.platform import (
    IS_LINUX,
    IS_MAC,
    IS_NOT_LINUX,
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    IS_WINDOWS,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from utilities.types import Duration, PathLike

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


def add_pytest_addoption(parser: Parser, options: Sequence[str], /) -> None:
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
    config: Config, items: Iterable[Function], options: Sequence[str], /
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
    return "PYTEST_VERSION" in environ


def node_id_to_path(
    node_id: str, /, *, head: PathLike | None = None, suffix: str = ".csv"
) -> Path:
    """Map a node ID to a path."""
    path_file, *parts = node_id.split("::")
    path_file = Path(path_file)
    if path_file.suffix != ".py":
        raise NodeIdToPathError(node_id=node_id)
    path = path_file.with_suffix("")
    if head is not None:
        path = path.relative_to(head)
    path = Path(".".join(path.parts), "__".join(parts))
    if not ((len(path.suffixes) >= 1) and (path.suffixes[-1] == suffix)):
        path = path.with_suffix("".join(chain(path.suffixes, [suffix])))
    return path


@dataclass(kw_only=True, slots=True)
class NodeIdToPathError(Exception):
    node_id: str

    @override
    def __str__(self) -> str:
        return f"Node ID must be a Python file; got {self.node_id!r}"


def throttle(
    *, root: PathLike | None = None, duration: Duration = 1.0, on_try: bool = False
) -> Any:
    """Throttle a test. On success by default, on try otherwise."""
    root_use = Path(".pytest_cache", "throttle") if root is None else Path(root)

    def wrapper(func: Callable[..., Any], /) -> Callable[..., Any]:
        """Throttle a test function/method."""
        if iscoroutinefunction(func):

            @wraps(func)
            async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
                """Call the throttled async test function/method."""
                path, now = _throttle_path_and_now(root_use, duration=duration)
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
            path, now = _throttle_path_and_now(root_use, duration=duration)
            if on_try:
                _throttle_write(path, now)
                return func(*args, **kwargs)
            out = func(*args, **kwargs)
            _throttle_write(path, now)
            return out

        return wrapped_sync

    return wrapper


def _throttle_path_and_now(
    root: Path, /, *, duration: Duration = 1.0
) -> tuple[Path, float]:
    test = environ["PYTEST_CURRENT_TEST"]
    path = Path(root, _throttle_md5_hash(test))
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
    "NodeIdToPathError",
    "add_pytest_addoption",
    "add_pytest_collection_modifyitems",
    "add_pytest_configure",
    "is_pytest",
    "node_id_to_path",
    "skipif_linux",
    "skipif_mac",
    "skipif_not_linux",
    "skipif_not_mac",
    "skipif_not_windows",
    "skipif_windows",
    "throttle",
]
