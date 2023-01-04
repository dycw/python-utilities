from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from contextlib import suppress
from os import environ
from os import getenv
from typing import Optional
from typing import cast

from beartype import beartype


@contextmanager
@beartype
def temp_environ(
    env: Optional[Mapping[str, Optional[str]]] = None,
    **env_kwargs: Optional[str],
) -> Iterator[None]:
    """Context manager with temporary environment variable set."""
    all_env = (
        cast(dict[str, Optional[str]], {}) if env is None else env
    ) | env_kwargs
    prev = list(zip(all_env, map(getenv, all_env)))
    _apply_environment(all_env.items())
    try:
        yield
    finally:
        _apply_environment(prev)


@beartype
def _apply_environment(items: Iterable[tuple[str, Optional[str]]], /) -> None:
    for key, value in items:
        if value is None:
            with suppress(KeyError):
                del environ[key]
        else:
            environ[key] = value
