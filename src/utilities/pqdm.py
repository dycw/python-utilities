from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from functools import partial
from io import StringIO
from io import TextIOWrapper
from multiprocessing import cpu_count
from typing import Any
from typing import Literal
from typing import Optional
from typing import TypeVar
from typing import Union
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
    n_jobs: Optional[int] = None,
    bounded: bool = False,
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "ignore",
    desc: Optional[str] = _DEFAULTS.desc,
    total: Optional[Union[int, float]] = _DEFAULTS.total,
    leave: Optional[bool] = _DEFAULTS.leave,
    file: Optional[Union[TextIOWrapper, StringIO]] = _DEFAULTS.file,
    ncols: Optional[int] = _DEFAULTS.ncols,
    mininterval: Optional[float] = _DEFAULTS.mininterval,
    maxinterval: Optional[float] = _DEFAULTS.maxinterval,
    miniters: Optional[Union[int, float]] = _DEFAULTS.miniters,
    ascii: Union[bool, Optional[str]] = _DEFAULTS.ascii,  # noqa: A002
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
    n_jobs: Optional[int] = None,
    bounded: bool = False,
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "ignore",
    desc: Optional[str] = _DEFAULTS.desc,
    total: Optional[Union[int, float]] = _DEFAULTS.total,
    leave: Optional[bool] = _DEFAULTS.leave,
    file: Optional[Union[TextIOWrapper, StringIO]] = _DEFAULTS.file,
    ncols: Optional[int] = _DEFAULTS.ncols,
    mininterval: Optional[float] = _DEFAULTS.mininterval,
    maxinterval: Optional[float] = _DEFAULTS.maxinterval,
    miniters: Optional[Union[int, float]] = _DEFAULTS.miniters,
    ascii: Union[bool, Optional[str]] = _DEFAULTS.ascii,  # noqa: A002
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
def _get_n_jobs(n_jobs: Optional[int], /) -> int:
    if (n_jobs is None) or (n_jobs <= 0):
        return cpu_count()  # pragma: no cover
    return n_jobs


@beartype
def _starmap_helper(func: Callable[..., _T], *args: Any) -> _T:
    return func(*args)
