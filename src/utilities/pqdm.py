from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from io import StringIO, TextIOWrapper
from multiprocessing import cpu_count
from typing import Any, Literal, TypeVar, cast

from pqdm import processes

from utilities.class_name import get_class_name
from utilities.sentinel import Sentinel, sentinel
from utilities.tqdm import _DEFAULTS as _TQDM_DEFAULTS
from utilities.tqdm import _get_total, _LockArgs, tqdm


@dataclass(frozen=True)
class _Defaults:
    parallelism: Literal["processes", "threads"] = "processes"
    n_jobs: int | None = None
    bounded: bool = False
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "immediate"


_PQDM_DEFAULTS = _Defaults()


_T = TypeVar("_T")


def pmap(
    func: Callable[..., _T],
    /,
    *iterables: Iterable[Any],
    parallelism: Literal["processes", "threads"] = _PQDM_DEFAULTS.parallelism,
    n_jobs: int | None = _PQDM_DEFAULTS.n_jobs,
    bounded: bool = _PQDM_DEFAULTS.bounded,
    exception_behaviour: Literal[
        "ignore", "immediate", "deferred"
    ] = _PQDM_DEFAULTS.exception_behaviour,
    desc: str | None | Sentinel = sentinel,
    total: float | None = _TQDM_DEFAULTS.total,
    leave: bool | None = _TQDM_DEFAULTS.leave,
    file: TextIOWrapper | StringIO | None = _TQDM_DEFAULTS.file,
    ncols: int | None = _TQDM_DEFAULTS.ncols,
    mininterval: float | None = _TQDM_DEFAULTS.mininterval,
    maxinterval: float | None = _TQDM_DEFAULTS.maxinterval,
    miniters: float | None = _TQDM_DEFAULTS.miniters,
    ascii: bool | str | None = _TQDM_DEFAULTS.ascii,  # noqa: A002
    unit: str | None = _TQDM_DEFAULTS.unit,
    unit_scale: bool | int | str | None = _TQDM_DEFAULTS.unit_scale,
    dynamic_ncols: bool | None = _TQDM_DEFAULTS.dynamic_ncols,
    smoothing: float | None = _TQDM_DEFAULTS.smoothing,
    bar_format: str | None = _TQDM_DEFAULTS.bar_format,
    initial: float | None = 0,
    position: int | None = _TQDM_DEFAULTS.position,
    postfix: Mapping[str, Any] | None = _TQDM_DEFAULTS.postfix,
    unit_divisor: float | None = _TQDM_DEFAULTS.unit_divisor,
    write_bytes: bool | None = _TQDM_DEFAULTS.write_bytes,
    lock_args: _LockArgs = _TQDM_DEFAULTS.lock_args,
    nrows: int | None = _TQDM_DEFAULTS.nrows,
    colour: str | None = _TQDM_DEFAULTS.colour,
    delay: float | None = _TQDM_DEFAULTS.delay,
    gui: bool | None = _TQDM_DEFAULTS.gui,
    **kwargs: Any,
) -> list[_T]:
    """Parallel map, powered by `pqdm`."""
    return pstarmap(
        func,
        zip(*iterables, strict=True),
        parallelism=parallelism,
        n_jobs=n_jobs,
        bounded=bounded,
        exception_behaviour=exception_behaviour,
        desc=desc,
        total=total,
        leave=leave,
        file=file,
        ncols=ncols,
        mininterval=mininterval,
        maxinterval=maxinterval,
        miniters=miniters,
        ascii=ascii,
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


def pstarmap(
    func: Callable[..., _T],
    iterable: Iterable[tuple[Any, ...]],
    /,
    *,
    parallelism: Literal["processes", "threads"] = _PQDM_DEFAULTS.parallelism,
    n_jobs: int | None = _PQDM_DEFAULTS.n_jobs,
    bounded: bool = _PQDM_DEFAULTS.bounded,
    exception_behaviour: Literal[
        "ignore", "immediate", "deferred"
    ] = _PQDM_DEFAULTS.exception_behaviour,
    desc: str | None | Sentinel = sentinel,
    total: float | None = _TQDM_DEFAULTS.total,
    leave: bool | None = _TQDM_DEFAULTS.leave,
    file: TextIOWrapper | StringIO | None = _TQDM_DEFAULTS.file,
    ncols: int | None = _TQDM_DEFAULTS.ncols,
    mininterval: float | None = _TQDM_DEFAULTS.mininterval,
    maxinterval: float | None = _TQDM_DEFAULTS.maxinterval,
    miniters: float | None = _TQDM_DEFAULTS.miniters,
    ascii: bool | str | None = _TQDM_DEFAULTS.ascii,  # noqa: A002
    unit: str | None = _TQDM_DEFAULTS.unit,
    unit_scale: bool | int | str | None = _TQDM_DEFAULTS.unit_scale,
    dynamic_ncols: bool | None = _TQDM_DEFAULTS.dynamic_ncols,
    smoothing: float | None = _TQDM_DEFAULTS.smoothing,
    bar_format: str | None = _TQDM_DEFAULTS.bar_format,
    initial: float | None = 0,
    position: int | None = _TQDM_DEFAULTS.position,
    postfix: Mapping[str, Any] | None = _TQDM_DEFAULTS.postfix,
    unit_divisor: float | None = _TQDM_DEFAULTS.unit_divisor,
    write_bytes: bool | None = _TQDM_DEFAULTS.write_bytes,
    lock_args: tuple[Any, ...] | None = _TQDM_DEFAULTS.lock_args,
    nrows: int | None = _TQDM_DEFAULTS.nrows,
    colour: str | None = _TQDM_DEFAULTS.colour,
    delay: float | None = _TQDM_DEFAULTS.delay,
    gui: bool | None = _TQDM_DEFAULTS.gui,
    **kwargs: Any,
) -> list[_T]:
    """Parallel starmap, powered by `pqdm`."""
    n_jobs = _get_n_jobs(n_jobs)
    tqdm_class = cast(Any, tqdm)
    desc_kwargs = _get_desc(desc, func)
    total = _get_total(total, iterable)
    if parallelism == "processes":
        result = processes.pqdm(
            iterable,
            partial(_starmap_helper, func),
            n_jobs=n_jobs,
            argument_type="args",
            bounded=bounded,
            exception_behaviour=exception_behaviour,
            tqdm_class=tqdm_class,
            **desc_kwargs,
            total=total,
            leave=leave,
            file=file,
            ncols=ncols,
            mininterval=mininterval,
            maxinterval=maxinterval,
            miniters=miniters,
            ascii=ascii,
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
    else:
        result = processes.pqdm(
            iterable,
            partial(_starmap_helper, func),
            n_jobs=n_jobs,
            argument_type="args",
            bounded=bounded,
            exception_behaviour=exception_behaviour,
            tqdm_class=tqdm_class,
            **desc_kwargs,
            total=total,
            leave=leave,
            file=file,
            ncols=ncols,
            mininterval=mininterval,
            maxinterval=maxinterval,
            miniters=miniters,
            ascii=ascii,
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
    return list(result)


def _get_n_jobs(n_jobs: int | None, /) -> int:
    if (n_jobs is None) or (n_jobs <= 0):
        return cpu_count()  # pragma: no cover
    return n_jobs


def _get_desc(
    desc: str | None | Sentinel, func: Callable[..., Any], /
) -> dict[str, str]:
    if isinstance(desc, Sentinel):
        if isinstance(func, partial):
            return _get_desc(desc, func.func)
        try:
            desc_use = func.__name__
        except AttributeError:
            desc_use = get_class_name(func) if isinstance(func, object) else None
    else:
        desc_use = desc
    return {} if desc_use is None else {"desc": desc_use}


def _starmap_helper(func: Callable[..., _T], *args: Any) -> _T:
    return func(*args)


__all__ = ["pmap", "pstarmap"]
