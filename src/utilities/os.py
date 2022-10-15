from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from contextlib import suppress
from os import environ
from os import getenv
from typing import cast

from beartype import beartype


@contextmanager
@beartype
def temp_environ(
    env: Mapping[str, str | None] | None = None, **env_kwargs: str | None
) -> Iterator[None]:
    """Context manager with temporary environment variable set."""

    all_env = (
        cast(dict[str, str | None], {}) if env is None else env
    ) | env_kwargs
    prev = list(zip(all_env, map(getenv, all_env)))
    _apply_environment(all_env.items())
    try:
        yield
    finally:
        _apply_environment(prev)


@beartype
def _apply_environment(items: Iterable[tuple[str, str | None]], /) -> None:
    for key, value in items:
        if value is None:
            with suppress(KeyError):
                del environ[key]
        else:
            environ[key] = value
