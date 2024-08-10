from __future__ import annotations

from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from tqdm.asyncio import tqdm

if TYPE_CHECKING:
    from collections.abc import Mapping
    from io import StringIO

_T = TypeVar("_T")


def tqdm_async(
    iterable: AsyncIterable[_T],
    /,
    *,
    desc: str | None = None,
    total: float | None = None,
    leave: bool = True,
    file: StringIO | None = None,
    ncols: int | None = None,
    mininterval: float = 0.1,
    maxinterval: float = 10.0,
    miniters: float | None = None,
    ascii: bool | str | None = None,  # noqa: A002
    disable: bool = False,
    unit: str = "it",
    unit_scale: bool | float = False,
    dynamic_ncols: bool = False,
    smoothing: float = 0.3,
    bar_format: str | None = None,
    initial: float = 0,
    position: int | None = None,
    postfix: Mapping[str, Any] | None = None,
    unit_divisor: float = 1000,
    write_bytes: bool = False,
    lock_args: tuple[Any, ...] | None = None,
    nrows: int | None = None,
    colour: str | None = None,
    delay: float = 0.0,
    gui: bool = False,
    **kwargs: Any,
) -> AsyncIterable[_T]:
    return cast(
        AsyncIterable[_T],
        tqdm(
            iterable=cast(Any, iterable),
            desc=desc,
            total=total,
            leave=leave,
            file=file,
            ncols=ncols,
            mininterval=mininterval,
            maxinterval=maxinterval,
            miniters=miniters,
            ascii=ascii,
            disable=disable,
            unit=unit,
            unit_scale=unit_scale,
            dynamic_ncols=dynamic_ncols,
            smoothing=smoothing,
            bar_format=bar_format,
            initial=initial,
            position=position,
            postfix=postfix,
            unit_divisor=unit_divisor,
            write_bytes=write_bytes,
            lock_args=lock_args,
            nrows=nrows,
            colour=colour,
            delay=delay,
            gui=gui,
            **kwargs,
        ),
    )


__all__ = ["tqdm_async"]
