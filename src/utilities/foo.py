from __future__ import annotations

from asyncio import TaskGroup, run, sleep
from contextvars import ContextVar
from functools import wraps
from typing import TYPE_CHECKING, Any, cast

from pyrsistent import PVector, plist, pvector, v

from utilities.asyncio import timeout_td
from utilities.types import StrMapping
from utilities.whenever import SECOND

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyrsistent import PList

    from utilities.types import Coro


type _Args = tuple[Any, ...]
type _Kwargs = dict[str, Any]
type _Triple = tuple[Callable[..., Coro[Any]], _Args, _Kwargs]
_ASYNCIO_CALL_STACK: ContextVar[PVector[_Triple]] = ContextVar(
    "caller", default=pvector()
)


def log_call_stack[**P, R](func: Callable[P, Coro[R]], /) -> Callable[P, Coro[R]]:
    @wraps(func)
    async def log_call_stack_func(*args: P.args, **kwargs: P.kwargs) -> R:
        token = _ASYNCIO_CALL_STACK.set(
            _ASYNCIO_CALL_STACK.get().append((func, args, kwargs))
        )
        try:
            return await func(*args, **kwargs)
        finally:
            _ASYNCIO_CALL_STACK.reset(token)

    return log_call_stack_func


@log_call_stack
async def inner(success: bool) -> None:
    await sleep(0.1)
    await sleep(0.1)
    async with timeout_td(
        SECOND,
        error=TimeoutError(
            f"Timeout running {inner.__name__}; caller = {_ASYNCIO_CALL_STACK.get()}"
        ),
    ):
        await sleep(0.1 if success else 1.0)


@log_call_stack
async def middle(success: bool) -> None:
    await sleep(0.1)
    await sleep(0.1)
    await inner(True)
    await sleep(0.1)
    await sleep(0.1)
    await inner(success)


@log_call_stack
async def outer(success: bool) -> None:
    await sleep(0.1)
    await sleep(0.1)
    await middle(True)
    await sleep(0.1)
    await sleep(0.1)
    await middle(success)


@log_call_stack
async def task_inner(success: bool) -> None:
    await sleep(0.1)
    await sleep(0.1)
    async with TaskGroup() as tg:
        _ = tg.create_task(outer(success))


if __name__ == "__main__":
    run(task_inner(False))
