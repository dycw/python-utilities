from collections.abc import Iterable
from collections.abc import Mapping
from io import StringIO
from io import TextIOWrapper
from typing import Any

from tqdm import tqdm as _tqdm

from utilities.pytest import is_pytest


class tqdm(_tqdm):
    def __init__(
        self,
        iterable: Iterable[Any] | None = None,
        desc: str | None = None,
        total: int | float | None = None,
        leave: bool | None = True,
        file: TextIOWrapper | StringIO | None = None,
        ncols: int | None = None,
        mininterval: float | None = 0.1,
        maxinterval: float | None = 10,
        miniters: int | float | None = None,
        ascii: bool | str | None = None,
        unit: str | None = "it",
        unit_scale: bool | int | str | None = False,
        dynamic_ncols: bool | None = False,
        smoothing: float | None = 0.3,
        bar_format: str | None = None,
        initial: int | float | None = 0,
        position: int | None = None,
        postfix: Mapping[str, Any] | None = None,
        unit_divisor: float | None = 1000,
        write_bytes: bool | None = None,
        lock_args: tuple[Any, ...] | None = None,
        nrows: int | None = None,
        colour: str | None = None,
        delay: float | None = 0,
        gui: bool | None = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            iterable=iterable,
            desc=desc,
            total=total,
            leave=leave,
            file=file,
            ncols=ncols,
            mininterval=mininterval,
            maxinterval=maxinterval,
            miniters=miniters,
            ascii=ascii,
            disable=is_pytest(),
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
        )
