from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from io import StringIO, TextIOWrapper
from multiprocessing import cpu_count
from typing import Any, Literal, Optional, TypeVar, Union, cast

from beartype import beartype
from pqdm import processes

from utilities.class_name import get_class_name
from utilities.sentinel import Sentinel, sentinel
from utilities.tqdm import _DEFAULTS as _TQDM_DEFAULTS
from utilities.tqdm import _get_total, tqdm


@beartype
@dataclass(frozen=True)
class _Defaults:
    parallelism: Literal["processes", "threads"] = "processes"
    n_jobs: Optional[int] = None
    bounded: bool = False
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "immediate"


_PQDM_DEFAULTS = _Defaults()


_T = TypeVar("_T")


@beartype
def pmap(
    func: Callable[..., _T],
    /,
    *iterables: Iterable[Any],
    parallelism: Literal["processes", "threads"] = _PQDM_DEFAULTS.parallelism,
    n_jobs: Optional[int] = _PQDM_DEFAULTS.n_jobs,
    bounded: bool = _PQDM_DEFAULTS.bounded,
    exception_behaviour: Literal[
        "ignore", "immediate", "deferred"
    ] = _PQDM_DEFAULTS.exception_behaviour,
    desc: Union[Optional[str], Sentinel] = sentinel,
    total: Optional[Union[int, float]] = _TQDM_DEFAULTS.total,
    leave: Optional[bool] = _TQDM_DEFAULTS.leave,
    file: Optional[Union[TextIOWrapper, StringIO]] = _TQDM_DEFAULTS.file,
    ncols: Optional[int] = _TQDM_DEFAULTS.ncols,
    mininterval: Optional[float] = _TQDM_DEFAULTS.mininterval,
    maxinterval: Optional[float] = _TQDM_DEFAULTS.maxinterval,
    miniters: Optional[Union[int, float]] = _TQDM_DEFAULTS.miniters,
    ascii: Union[bool, Optional[str]] = _TQDM_DEFAULTS.ascii,  # noqa: A002
    unit: Optional[str] = _TQDM_DEFAULTS.unit,
    unit_scale: Union[bool, int, Optional[str]] = _TQDM_DEFAULTS.unit_scale,
    dynamic_ncols: Optional[bool] = _TQDM_DEFAULTS.dynamic_ncols,
    smoothing: Optional[float] = _TQDM_DEFAULTS.smoothing,
    bar_format: Optional[str] = _TQDM_DEFAULTS.bar_format,
    initial: Optional[Union[int, float]] = 0,
    position: Optional[int] = _TQDM_DEFAULTS.position,
    postfix: Optional[Mapping[str, Any]] = _TQDM_DEFAULTS.postfix,
    unit_divisor: Optional[float] = _TQDM_DEFAULTS.unit_divisor,
    write_bytes: Optional[bool] = _TQDM_DEFAULTS.write_bytes,
    lock_args: Optional[tuple[Any, ...]] = _TQDM_DEFAULTS.lock_args,
    nrows: Optional[int] = _TQDM_DEFAULTS.nrows,
    colour: Optional[str] = _TQDM_DEFAULTS.colour,
    delay: Optional[float] = _TQDM_DEFAULTS.delay,
    gui: Optional[bool] = _TQDM_DEFAULTS.gui,
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
    parallelism: Literal["processes", "threads"] = _PQDM_DEFAULTS.parallelism,
    n_jobs: Optional[int] = _PQDM_DEFAULTS.n_jobs,
    bounded: bool = _PQDM_DEFAULTS.bounded,
    exception_behaviour: Literal[
        "ignore", "immediate", "deferred"
    ] = _PQDM_DEFAULTS.exception_behaviour,
    desc: Union[Optional[str], Sentinel] = sentinel,
    total: Optional[Union[int, float]] = _TQDM_DEFAULTS.total,
    leave: Optional[bool] = _TQDM_DEFAULTS.leave,
    file: Optional[Union[TextIOWrapper, StringIO]] = _TQDM_DEFAULTS.file,
    ncols: Optional[int] = _TQDM_DEFAULTS.ncols,
    mininterval: Optional[float] = _TQDM_DEFAULTS.mininterval,
    maxinterval: Optional[float] = _TQDM_DEFAULTS.maxinterval,
    miniters: Optional[Union[int, float]] = _TQDM_DEFAULTS.miniters,
    ascii: Union[bool, Optional[str]] = _TQDM_DEFAULTS.ascii,  # noqa: A002
    unit: Optional[str] = _TQDM_DEFAULTS.unit,
    unit_scale: Union[bool, int, Optional[str]] = _TQDM_DEFAULTS.unit_scale,
    dynamic_ncols: Optional[bool] = _TQDM_DEFAULTS.dynamic_ncols,
    smoothing: Optional[float] = _TQDM_DEFAULTS.smoothing,
    bar_format: Optional[str] = _TQDM_DEFAULTS.bar_format,
    initial: Optional[Union[int, float]] = 0,
    position: Optional[int] = _TQDM_DEFAULTS.position,
    postfix: Optional[Mapping[str, Any]] = _TQDM_DEFAULTS.postfix,
    unit_divisor: Optional[float] = _TQDM_DEFAULTS.unit_divisor,
    write_bytes: Optional[bool] = _TQDM_DEFAULTS.write_bytes,
    lock_args: Optional[tuple[Any, ...]] = _TQDM_DEFAULTS.lock_args,
    nrows: Optional[int] = _TQDM_DEFAULTS.nrows,
    colour: Optional[str] = _TQDM_DEFAULTS.colour,
    delay: Optional[float] = _TQDM_DEFAULTS.delay,
    gui: Optional[bool] = _TQDM_DEFAULTS.gui,
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


@beartype
def _get_n_jobs(n_jobs: Optional[int], /) -> int:
    if (n_jobs is None) or (n_jobs <= 0):
        return cpu_count()  # pragma: no cover
    return n_jobs


@beartype
def _get_desc(
    desc: Union[Optional[str], Sentinel], func: Callable[..., Any], /
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


@beartype
def _starmap_helper(func: Callable[..., _T], *args: Any) -> _T:
    return func(*args)
