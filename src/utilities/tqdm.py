from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from io import StringIO
from io import TextIOWrapper
from typing import Any
from typing import cast

from beartype import beartype
from tqdm import tqdm as _tqdm

from utilities.pytest import is_pytest


@dataclass
@beartype
class _Defaults:
    desc: str | None = None
    total: int | float | None = None
    leave: bool | None = True
    file: TextIOWrapper | StringIO | None = None
    ncols: int | None = None
    mininterval: float | None = 0.1
    maxinterval: float | None = 10.0
    miniters: int | float | None = None
    ascii: bool | str | None = None
    unit: str | None = "it"
    unit_scale: bool | int | str | None = False
    dynamic_ncols: bool | None = False
    smoothing: float | None = 0.3
    bar_format: str | None = None
    initial: int | float | None = 0
    position: int | None = None
    postfix: Mapping[str, Any] | None = None
    unit_divisor: float | None = 1000.0
    write_bytes: bool | None = None
    lock_args: tuple[Any, ...] | None = None
    nrows: int | None = None
    colour: str | None = None
    delay: float | None = 0.0
    gui: bool | None = False


_DEFAULTS = _Defaults()


class tqdm(_tqdm):
    """Sub-class of `tqdm` which is disabled during pytest."""

    @beartype
    def __init__(
        self,
        iterable: Iterable[Any] | None = None,
        desc: str | None = _DEFAULTS.desc,
        total: int | float | None = _DEFAULTS.total,
        leave: bool | None = _DEFAULTS.leave,
        file: TextIOWrapper | StringIO | None = _DEFAULTS.file,
        ncols: int | None = _DEFAULTS.ncols,
        mininterval: float | None = _DEFAULTS.mininterval,
        maxinterval: float | None = _DEFAULTS.maxinterval,
        miniters: int | float | None = _DEFAULTS.miniters,
        ascii: bool | str | None = _DEFAULTS.ascii,
        unit: str | None = _DEFAULTS.unit,
        unit_scale: bool | int | str | None = _DEFAULTS.unit_scale,
        dynamic_ncols: bool | None = _DEFAULTS.dynamic_ncols,
        smoothing: float | None = _DEFAULTS.smoothing,
        bar_format: str | None = _DEFAULTS.bar_format,
        initial: int | float | None = 0,
        position: int | None = _DEFAULTS.position,
        postfix: Mapping[str, Any] | None = _DEFAULTS.postfix,
        unit_divisor: float | None = _DEFAULTS.unit_divisor,
        write_bytes: bool | None = _DEFAULTS.write_bytes,
        lock_args: tuple[Any, ...] | None = _DEFAULTS.lock_args,
        nrows: int | None = _DEFAULTS.nrows,
        colour: str | None = _DEFAULTS.colour,
        delay: float | None = _DEFAULTS.delay,
        gui: bool | None = _DEFAULTS.gui,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            iterable=cast(Any, iterable),
            desc=desc,
            total=total,
            leave=leave,
            file=file,
            ncols=ncols,
            mininterval=cast(Any, mininterval),
            maxinterval=cast(Any, maxinterval),
            miniters=miniters,
            ascii=ascii,
            disable=is_pytest(),
            unit=cast(Any, unit),
            unit_scale=cast(Any, unit_scale),
            dynamic_ncols=cast(Any, dynamic_ncols),
            smoothing=cast(Any, smoothing),
            bar_format=bar_format,
            initial=cast(Any, initial),
            position=position,
            postfix=postfix,
            unit_divisor=cast(Any, unit_divisor),
            write_bytes=write_bytes,
            lock_args=lock_args,
            nrows=nrows,
            colour=colour,
            delay=delay,
            gui=cast(Any, gui),
            **kwargs,
        )
