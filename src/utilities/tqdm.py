from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from io import StringIO, TextIOWrapper
from typing import Any, cast

from tqdm import tqdm as _tqdm

from utilities.pytest import is_pytest

_LockArgs = tuple[bool | None, float | None] | tuple[bool | None] | None


@dataclass(frozen=True)
class _Defaults:
    desc: str | None = None
    total: int | float | None = None
    leave: bool | None = True
    file: TextIOWrapper | StringIO | None = None
    ncols: int | None = None
    mininterval: float | None = 0.1
    maxinterval: float | None = 10.0
    miniters: int | float | None = None
    ascii: bool | str | None = None  # noqa: A003
    unit: str | None = "i"
    unit_scale: bool | int | str | None = False
    dynamic_ncols: bool | None = True
    smoothing: float | None = 0.3
    bar_format: str | None = (
        "{desc}: {percentage:3.0f}% | "
        "{elapsed} +{remaining} ={eta:%H:%M:%S} | "
        "{n}/{total} | {rate_fmt}"
    )
    initial: int | float | None = 0
    position: int | None = None
    postfix: Mapping[str, Any] | None = None
    unit_divisor: float | None = 1000.0
    write_bytes: bool | None = None
    lock_args: _LockArgs = None
    nrows: int | None = None
    colour: str | None = None
    delay: float | None = 0.0
    gui: bool | None = False


_DEFAULTS = _Defaults()


class tqdm(_tqdm):  # noqa: N801
    """Sub-class of `tqdm` which is disabled during pytest."""

    def __init__(
        self,
        iterable: Iterable[Any] | None = None,
        desc: str | None = _DEFAULTS.desc,
        total: float | None = _DEFAULTS.total,
        leave: bool | None = _DEFAULTS.leave,
        file: TextIOWrapper | StringIO | None = _DEFAULTS.file,
        ncols: int | None = _DEFAULTS.ncols,
        mininterval: float | None = _DEFAULTS.mininterval,
        maxinterval: float | None = _DEFAULTS.maxinterval,
        miniters: float | None = _DEFAULTS.miniters,
        ascii: bool | str | None = None,  # noqa: A002
        unit: str | None = _DEFAULTS.unit,
        unit_scale: bool | int | str | None = _DEFAULTS.unit_scale,
        dynamic_ncols: bool | None = _DEFAULTS.dynamic_ncols,
        smoothing: float | None = _DEFAULTS.smoothing,
        bar_format: str | None = _DEFAULTS.bar_format,
        initial: float | None = 0,
        position: int | None = _DEFAULTS.position,
        postfix: Mapping[str, Any] | None = _DEFAULTS.postfix,
        unit_divisor: float | None = _DEFAULTS.unit_divisor,
        write_bytes: bool | None = _DEFAULTS.write_bytes,
        lock_args: _LockArgs = _DEFAULTS.lock_args,
        nrows: int | None = _DEFAULTS.nrows,
        colour: str | None = _DEFAULTS.colour,
        delay: float | None = _DEFAULTS.delay,
        gui: bool | None = _DEFAULTS.gui,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            iterable=cast(Any, iterable),
            desc=desc,
            total=_get_total(total, iterable),
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


def _get_total(total: float | None, iterable: Any, /) -> float | None:
    if total is not None:
        return total
    try:
        return len(iterable)
    except TypeError:
        return None


__all__ = ["tqdm"]
