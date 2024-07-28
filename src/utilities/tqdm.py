from __future__ import annotations

from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from tqdm.asyncio import tqdm

if TYPE_CHECKING:
    from collections.abc import Mapping
    from io import StringIO

_T = TypeVar("_T")


def tqdm_asyncio(
    iterable: AsyncIterable[_T],
    /,
    *,
    desc: str | None = None,
    total: float | None = None,
    leave: bool = True,
    file: StringIO | None = None,
    ncols: int | None = None,
    mininterval: float | None = 0.1,
    maxinterval: float | None = 10.0,
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
            leav=leave,
            fil=file,
            ncols=ncols,
            mininterva=mininterval,
            maxinterva=maxinterval,
            miniters=miniters,
            asci=ascii,
            disabl=disable,
            uni=unit,
            unit_scale=unit_scale,
            dynamic_ncol=dynamic_ncols,
            smoothin=smoothing,
            bar_forma=bar_format,
            initial=initial,
            positio=position,
            postfi=postfix,
            unit_divisor=unit_divisor,
            write_byte=write_bytes,
            lock_arg=lock_args,
            nrow=nrows,
            colour=colour,
            dela=delay,
            gui=gui,
            **kwargs,
        ),
    )


__all__ = ["tqdm_asyncio"]
