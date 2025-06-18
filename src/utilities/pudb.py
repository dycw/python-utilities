from __future__ import annotations

from asyncio import iscoroutinefunction
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, NoReturn, cast, overload

from utilities.os import GetEnvVarError, get_env_var

if TYPE_CHECKING:
    from collections.abc import Callable


_ENV_VAR = "DEBUG"


@overload
def call_pudb[F: Callable](func: F, /, *, env_var: str = _ENV_VAR) -> F: ...
@overload
def call_pudb[F: Callable](
    func: None = None, /, *, env_var: str = _ENV_VAR
) -> Callable[[F], F]: ...
def call_pudb[F: Callable](
    func: F | None = None, /, *, env_var: str = _ENV_VAR
) -> F | Callable[[F], F]:
    """Call `pudb` upon failure, if the required environment variable is set."""
    if func is None:
        result = partial(call_pudb, env_var=env_var)
        return cast("Callable[[F], F]", result)

    if not iscoroutinefunction(func):

        @wraps(func)
        def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as error:  # noqa: BLE001
                _call_pudb(error, env_var=env_var)

        return cast("F", wrapped_sync)

    @wraps(func)
    async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
        try:
            return await func(*args, **kwargs)
        except Exception as error:  # noqa: BLE001
            _call_pudb(error, env_var=env_var)

    return cast("F", wrapped_async)


def _call_pudb(error: Exception, /, *, env_var: str = _ENV_VAR) -> NoReturn:
    try:
        _ = get_env_var(env_var)
    except GetEnvVarError:
        raise error from None
    from pudb import post_mortem  # pragma: no cover

    post_mortem()  # pragma: no cover
    raise error  # pragma: no cover


__all__ = ["call_pudb"]
