from __future__ import annotations

from asyncio import iscoroutinefunction
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, NoReturn, cast, overload

from pudb import post_mortem

from utilities.os import GetEnvVarError, get_env_var

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.types import TCallable


_ENV_VAR = "DEBUG"


@overload
def call_pudb(func: TCallable, /, *, env_var: str = _ENV_VAR) -> TCallable: ...
@overload
def call_pudb(
    func: None = None, /, *, env_var: str = _ENV_VAR
) -> Callable[[TCallable], TCallable]: ...
def call_pudb(
    func: TCallable | None = None, /, *, env_var: str = _ENV_VAR
) -> TCallable | Callable[[TCallable], TCallable]:
    """Call `pudb` upon failure, if the required environment variable is set."""
    if func is None:
        result = partial(call_pudb, env_var=env_var)
        return cast("Callable[[TCallable], TCallable]", result)

    if not iscoroutinefunction(func):

        @wraps(func)
        def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as error:  # noqa: BLE001
                _call_pudb(error, env_var=env_var)

        return cast("TCallable", wrapped_sync)

    @wraps(func)
    async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
        try:
            return await func(*args, **kwargs)
        except Exception as error:  # noqa: BLE001
            _call_pudb(error, env_var=env_var)

    return cast("TCallable", wrapped_async)


def _call_pudb(error: Exception, /, *, env_var: str = _ENV_VAR) -> NoReturn:
    try:
        _ = get_env_var(env_var)
    except GetEnvVarError:
        raise error from None
    post_mortem()
    raise error


__all__ = ["call_pudb"]
