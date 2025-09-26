from __future__ import annotations

from dataclasses import dataclass
from functools import partial, wraps
from inspect import iscoroutinefunction
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, assert_never, cast, override

from whenever import ZonedDateTime

from utilities.atomicwrites import writer
from utilities.functools import cache
from utilities.hashlib import md5_hash
from utilities.os import get_env_var
from utilities.pathlib import (
    _GetTailEmptyError,
    ensure_suffix,
    get_root,
    get_tail,
    module_path,
)
from utilities.platform import (
    IS_LINUX,
    IS_MAC,
    IS_NOT_LINUX,
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    IS_WINDOWS,
)
from utilities.random import bernoulli
from utilities.text import to_bool
from utilities.types import MaybeCallableBoolLike, MaybeCoro, Seed
from utilities.whenever import SECOND, get_now_local

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from utilities.types import Coro, Delta, PathLike

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


def add_pytest_addoption(parser: Parser, options: list[str], /) -> None:
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


##


def add_pytest_collection_modifyitems(
    config: Config, items: Iterable[Function], options: list[str], /
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


##


def add_pytest_configure(config: Config, options: Iterable[tuple[str, str]], /) -> None:
    """Add the `--slow`, etc markers to pytest.

    Usage:
        def pytest_configure(config):
            add_pytest_configure(config, [("slow", "slow to run")])
    """
    for opt, desc in options:
        _ = config.addinivalue_line("markers", f"{opt}: mark test as {desc}")


##


def node_id_path(
    node_id: str, /, *, root: PathLike | None = None, suffix: str | None = None
) -> Path:
    """Get the path of a node ID."""
    path_file, *parts = node_id.split("::")
    path_file = Path(path_file)
    if path_file.suffix != ".py":
        raise _NodeIdToPathNotPythonFileError(node_id=node_id)
    path = path_file.with_suffix("")
    if root is not None:
        try:
            path = get_tail(path, root)
        except _GetTailEmptyError as error:
            raise _NodeIdToPathNotGetTailError(
                node_id=node_id, path=error.path, root=error.root
            ) from None
    path = Path(module_path(path), "__".join(parts))
    if suffix is not None:
        path = ensure_suffix(path, suffix)
    return path


@dataclass(kw_only=True, slots=True)
class NodeIdToPathError(Exception):
    node_id: str


@dataclass(kw_only=True, slots=True)
class _NodeIdToPathNotPythonFileError(NodeIdToPathError):
    @override
    def __str__(self) -> str:
        return f"Node ID must be a Python file; got {self.node_id!r}"


@dataclass(kw_only=True, slots=True)
class _NodeIdToPathNotGetTailError(NodeIdToPathError):
    path: PathLike
    root: PathLike

    @override
    def __str__(self) -> str:
        return (
            f"Unable to get the tail of {str(self.path)!r} with root {str(self.root)!r}"
        )


##


def run_frac[F: Callable[..., MaybeCoro[None]]](
    *,
    predicate: MaybeCallableBoolLike | None = None,
    frac: float = 0.5,
    seed: Seed | None = None,
) -> Callable[[F], F]:
    """Run a test only a fraction of the time.."""
    return cast(
        "Any", partial(_run_frac_inner, predicate=predicate, frac=frac, seed=seed)
    )


def _run_frac_inner[F: Callable[..., MaybeCoro[None]]](
    func: F,
    /,
    *,
    predicate: MaybeCallableBoolLike | None = None,
    frac: float = 0.5,
    seed: Seed | None = None,
) -> F:
    match bool(iscoroutinefunction(func)):
        case False:

            @wraps(func)
            def run_frac_sync(*args: Any, **kwargs: Any) -> None:
                _skipif_frac(predicate=predicate, frac=frac, seed=seed)
                cast("Callable[..., None]", func)(*args, **kwargs)

            return cast("Any", run_frac_sync)

        case True:

            @wraps(func)
            async def run_frac_async(*args: Any, **kwargs: Any) -> None:
                _skipif_frac(predicate=predicate, frac=frac, seed=seed)
                await cast("Callable[..., Coro[None]]", func)(*args, **kwargs)

            return cast("Any", run_frac_async)

        case never:
            assert_never(never)


def _skipif_frac(
    *,
    predicate: MaybeCallableBoolLike | None = None,
    frac: float = 0.5,
    seed: Seed | None = None,
) -> None:
    if skip is None:
        return  # pragma: no cover
    if ((predicate is None) or to_bool(predicate)) and bernoulli(
        true=1 - frac, seed=seed
    ):
        _ = skip(reason=f"{_get_name()} skipped (run {frac:.0%})")


##


def throttle[F: Callable[..., MaybeCoro[None]]](
    *, root: PathLike | None = None, delta: Delta = SECOND, on_try: bool = False
) -> Callable[[F], F]:
    """Throttle a test. On success by default, on try otherwise."""
    return cast("Any", partial(_throttle_inner, root=root, delta=delta, on_try=on_try))


def _throttle_inner[F: Callable[..., MaybeCoro[None]]](
    func: F,
    /,
    *,
    root: PathLike | None = None,
    delta: Delta = SECOND,
    on_try: bool = False,
) -> F:
    if get_env_var("THROTTLE", nullable=True) is not None:
        return func
    match bool(iscoroutinefunction(func)), on_try:
        case False, False:

            @wraps(func)
            def throttle_sync_on_pass(*args: Any, **kwargs: Any) -> None:
                _skipif_recent(root=root, delta=delta)
                cast("Callable[..., None]", func)(*args, **kwargs)
                _write(root)

            return cast("Any", throttle_sync_on_pass)

        case False, True:

            @wraps(func)
            def throttle_sync_on_try(*args: Any, **kwargs: Any) -> None:
                _skipif_recent(root=root, delta=delta)
                _write(root)
                cast("Callable[..., None]", func)(*args, **kwargs)

            return cast("Any", throttle_sync_on_try)

        case True, False:

            @wraps(func)
            async def throttle_async_on_pass(*args: Any, **kwargs: Any) -> None:
                _skipif_recent(root=root, delta=delta)
                await cast("Callable[..., Coro[None]]", func)(*args, **kwargs)
                _write(root)

            return cast("Any", throttle_async_on_pass)

        case True, True:

            @wraps(func)
            async def throttle_async_on_try(*args: Any, **kwargs: Any) -> None:
                _skipif_recent(root=root, delta=delta)
                _write(root)
                await cast("Callable[..., Coro[None]]", func)(*args, **kwargs)

            return cast("Any", throttle_async_on_try)

        case never:
            assert_never(never)


def _skipif_recent(*, root: PathLike | None = None, delta: Delta = SECOND) -> None:
    if skip is None:
        return  # pragma: no cover
    path = _get_path(root)
    try:
        contents = path.read_text()
    except FileNotFoundError:
        return
    try:
        last = ZonedDateTime.parse_iso(contents)
    except ValueError:
        return
    now = get_now_local()
    if (now - delta) < last:
        age = now - last
        _ = skip(reason=f"{_get_name()} throttled (age {age})")


def _get_path(root: PathLike | None = None, /) -> Path:
    if root is None:
        root_use = get_root().joinpath(".pytest_cache", "throttle")  # pragma: no cover
    else:
        root_use = root
    return Path(root_use, _md5_hash_cached(_get_name()))


@cache
def _md5_hash_cached(text: str, /) -> str:
    return md5_hash(text)


def _get_name() -> str:
    return environ["PYTEST_CURRENT_TEST"]


def _write(root: PathLike | None = None, /) -> None:
    path = _get_path(root)
    with writer(path, overwrite=True) as temp:
        _ = temp.write_text(get_now_local().format_iso())


__all__ = [
    "NodeIdToPathError",
    "add_pytest_addoption",
    "add_pytest_collection_modifyitems",
    "add_pytest_configure",
    "node_id_path",
    "run_frac",
    "skipif_linux",
    "skipif_mac",
    "skipif_not_linux",
    "skipif_not_mac",
    "skipif_not_windows",
    "skipif_windows",
    "throttle",
]
