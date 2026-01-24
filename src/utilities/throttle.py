from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial, wraps
from inspect import iscoroutinefunction
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, assert_never, cast, override

from whenever import ZonedDateTime

from utilities.constants import SECOND
from utilities.core import duration_to_seconds, get_env, get_now_local, write_text
from utilities.pathlib import to_path
from utilities.types import Duration, MaybeCallablePathLike, MaybeCoro

if TYPE_CHECKING:
    from utilities.types import Coro


_DEFAULT_THROTTLE: Duration = SECOND


def throttle[F: Callable[..., MaybeCoro[None]]](
    *,
    on_try: bool = False,
    duration: Duration = _DEFAULT_THROTTLE,
    path: MaybeCallablePathLike = Path.cwd,
    raiser: Callable[[], NoReturn] | None = None,
) -> Callable[[F], F]:
    """Throttle a function. On success by default, on try otherwise."""
    return cast(
        "Any",
        partial(
            _throttle_inner, on_try=on_try, duration=duration, path=path, raiser=raiser
        ),
    )


def _throttle_inner[F: Callable[..., MaybeCoro[None]]](
    func: F,
    /,
    *,
    on_try: bool = False,
    duration: Duration = _DEFAULT_THROTTLE,
    path: MaybeCallablePathLike = Path.cwd,
    raiser: Callable[[], NoReturn] | None = None,
) -> F:
    match bool(iscoroutinefunction(func)), on_try:
        case False, False:

            @wraps(func)
            def throttle_sync_on_pass(*args: Any, **kwargs: Any) -> None:
                path_use = to_path(path)
                if _is_throttle(path=path_use, duration=duration):
                    _try_raise(raiser=raiser)
                else:
                    cast("Callable[..., None]", func)(*args, **kwargs)
                    _write_throttle(path=path_use)

            return cast("Any", throttle_sync_on_pass)

        case False, True:

            @wraps(func)
            def throttle_sync_on_try(*args: Any, **kwargs: Any) -> None:
                path_use = to_path(path)
                if _is_throttle(path=path_use, duration=duration):
                    _try_raise(raiser=raiser)
                else:
                    _write_throttle(path=path_use)
                    cast("Callable[..., None]", func)(*args, **kwargs)

            return cast("Any", throttle_sync_on_try)

        case True, False:

            @wraps(func)
            async def throttle_async_on_pass(*args: Any, **kwargs: Any) -> None:
                path_use = to_path(path)
                if _is_throttle(path=path_use, duration=duration):
                    _try_raise(raiser=raiser)
                else:
                    await cast("Callable[..., Coro[None]]", func)(*args, **kwargs)
                    _write_throttle(path=path_use)

            return cast("Any", throttle_async_on_pass)

        case True, True:

            @wraps(func)
            async def throttle_async_on_try(*args: Any, **kwargs: Any) -> None:
                path_use = to_path(path)
                if _is_throttle(path=path_use, duration=duration):
                    _try_raise(raiser=raiser)
                else:
                    _write_throttle(path=path_use)
                    await cast("Callable[..., Coro[None]]", func)(*args, **kwargs)

            return cast("Any", throttle_async_on_try)

        case never:
            assert_never(never)


def _is_throttle(
    *, path: MaybeCallablePathLike = Path.cwd, duration: Duration = _DEFAULT_THROTTLE
) -> bool:
    if get_env("THROTTLE", nullable=True):
        return False
    path = to_path(path)
    if path.is_file():
        text = path.read_text().rstrip("\n")
        if text == "":
            path.unlink(missing_ok=True)
            return False
        try:
            last = ZonedDateTime.parse_iso(text)
        except ValueError:
            raise _ThrottleParseZonedDateTimeError(path=path, text=text) from None
        threshold = get_now_local() - duration_to_seconds(duration) * SECOND
        return threshold <= last
    if not path.exists():
        return False
    raise _ThrottleMarkerFileError(path=path)


def _try_raise(*, raiser: Callable[[], NoReturn] | None = None) -> None:
    if raiser is not None:
        raiser()


def _write_throttle(*, path: MaybeCallablePathLike = Path.cwd) -> None:
    path_use = to_path(path)
    write_text(path_use, get_now_local().format_iso(), overwrite=True)


@dataclass(kw_only=True, slots=True)
class ThrottleError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ThrottleParseZonedDateTimeError(ThrottleError):
    path: Path
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse the contents {self.text!r} of {str(self.path)!r} to a ZonedDateTime"


@dataclass(kw_only=True, slots=True)
class _ThrottleMarkerFileError(ThrottleError):
    path: Path

    @override
    def __str__(self) -> str:
        return f"Invalid marker file {str(self.path)!r}"


__all__ = ["ThrottleError", "throttle"]
