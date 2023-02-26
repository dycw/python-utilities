from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from io import StringIO
from io import TextIOWrapper
from typing import Any
from typing import Optional
from typing import Union
from typing import cast

from beartype import beartype
from tqdm import tqdm as _tqdm

from utilities.pytest import is_pytest


@beartype
@dataclass(frozen=True)
class _Defaults:
    desc: Optional[str] = None
    total: Optional[Union[int, float]] = None
    leave: Optional[bool] = True
    file: Optional[Union[TextIOWrapper, StringIO]] = None
    ncols: Optional[int] = None
    mininterval: Optional[float] = 0.1
    maxinterval: Optional[float] = 10.0
    miniters: Optional[Union[int, float]] = None
    ascii: Union[bool, Optional[str]] = None  # noqa: A003
    unit: Optional[str] = "it"
    unit_scale: Union[bool, int, Optional[str]] = False
    dynamic_ncols: Optional[bool] = False
    smoothing: Optional[float] = 0.3
    bar_format: Optional[str] = None
    initial: Optional[Union[int, float]] = 0
    position: Optional[int] = None
    postfix: Optional[Mapping[str, Any]] = None
    unit_divisor: Optional[float] = 1000.0
    write_bytes: Optional[bool] = None
    lock_args: Optional[tuple[Any, ...]] = None
    nrows: Optional[int] = None
    colour: Optional[str] = None
    delay: Optional[float] = 0.0
    gui: Optional[bool] = False


_DEFAULTS = _Defaults()


class tqdm(_tqdm):  # noqa: N801
    """Sub-class of `tqdm` which is disabled during pytest."""

    @beartype
    def __init__(
        self,
        iterable: Optional[Iterable[Any]] = None,
        desc: Optional[str] = _DEFAULTS.desc,
        total: Optional[Union[int, float]] = _DEFAULTS.total,
        leave: Optional[bool] = _DEFAULTS.leave,
        file: Optional[Union[TextIOWrapper, StringIO]] = _DEFAULTS.file,
        ncols: Optional[int] = _DEFAULTS.ncols,
        mininterval: Optional[float] = _DEFAULTS.mininterval,
        maxinterval: Optional[float] = _DEFAULTS.maxinterval,
        miniters: Optional[Union[int, float]] = _DEFAULTS.miniters,
        ascii: Union[bool, Optional[str]] = None,  # noqa: A002
        unit: Optional[str] = _DEFAULTS.unit,
        unit_scale: Union[bool, int, Optional[str]] = _DEFAULTS.unit_scale,
        dynamic_ncols: Optional[bool] = _DEFAULTS.dynamic_ncols,
        smoothing: Optional[float] = _DEFAULTS.smoothing,
        bar_format: Optional[str] = _DEFAULTS.bar_format,
        initial: Optional[Union[int, float]] = 0,
        position: Optional[int] = _DEFAULTS.position,
        postfix: Optional[Mapping[str, Any]] = _DEFAULTS.postfix,
        unit_divisor: Optional[float] = _DEFAULTS.unit_divisor,
        write_bytes: Optional[bool] = _DEFAULTS.write_bytes,
        lock_args: Optional[tuple[Any, ...]] = _DEFAULTS.lock_args,
        nrows: Optional[int] = _DEFAULTS.nrows,
        colour: Optional[str] = _DEFAULTS.colour,
        delay: Optional[float] = _DEFAULTS.delay,
        gui: Optional[bool] = _DEFAULTS.gui,
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
