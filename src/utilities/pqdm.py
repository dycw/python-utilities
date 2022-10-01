from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from functools import partial
from io import StringIO
from io import TextIOWrapper
from multiprocessing import cpu_count
from typing import Any
from typing import Literal
from typing import TypeVar
from typing import cast

from beartype import beartype
from pqdm import processes

from utilities.tqdm import _DEFAULTS
from utilities.tqdm import tqdm


_T = TypeVar("_T")


@beartype
def pmap(
    func: Callable[..., _T],
    /,
    *iterables: Iterable[Any],
    parallelism: Literal["processes", "threads"] = "processes",
    n_jobs: int | None = None,
    bounded: bool = False,
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "ignore",
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
) -> list[_T]:
    """Parallel map, powered by `pqdm`."""

    return pstarmap(
        func,
        zip(*iterables),
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


@beartype
def pstarmap(
    func: Callable[..., _T],
    iterable: Iterable[tuple[Any, ...]],
    /,
    *,
    parallelism: Literal["processes", "threads"] = "processes",
    n_jobs: int | None = None,
    bounded: bool = False,
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "ignore",
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
) -> list[_T]:
    """Parallel starmap, powered by `pqdm`."""

    n_jobs = _get_n_jobs(n_jobs)
    tqdm_class = cast(Any, tqdm)
    if parallelism == "processes":
        result = processes.pqdm(
            iterable,
            partial(_starmap_helper, func),
            n_jobs=n_jobs,
            argument_type="args",
            bounded=bounded,
            exception_behaviour=exception_behaviour,
            tqdm_class=tqdm_class,
            **({} if desc is None else {"desc": desc}),
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
            **({} if desc is None else {"desc": desc}),
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


@beartype
def _get_n_jobs(n_jobs: int | None, /) -> int:
    if (n_jobs is None) or (n_jobs <= 0):
        return cpu_count()  # pragma: no cover
    else:
        return n_jobs


@beartype
def _starmap_helper(func: Callable[..., _T], *args: Any) -> _T:
    return func(*args)
