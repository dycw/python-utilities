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

from pqdm import processes

from utilities.tqdm import tqdm


_T = TypeVar("_T")


def pmap(
    func: Callable[..., _T],
    /,
    *iterables: Iterable[Any],
    parallelism: Literal["processes", "threads"] = "processes",
    n_jobs: int | None = None,
    bounded: bool = False,
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "ignore",
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


def pstarmap(
    func: Callable[..., _T],
    iterable: Iterable[tuple[Any, ...]],
    /,
    *,
    parallelism: Literal["processes", "threads"] = "processes",
    n_jobs: int | None = None,
    bounded: bool = False,
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "ignore",
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
) -> list[_T]:
    """Parallel map, powered by `pqdm`."""

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


def _get_n_jobs(n_jobs: int | None, /) -> int:
    if (n_jobs is None) or (n_jobs <= 0):
        return cpu_count()  # pragma: no cover
    else:
        return n_jobs


def _starmap_helper(func: Callable[..., _T], *args: Any) -> _T:
    return func(*args)
