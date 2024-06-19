from __future__ import annotations

from functools import partial
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from pqdm import processes, threads
from tqdm.auto import tqdm as tqdm_auto
from typing_extensions import assert_never

from utilities.sentinel import Sentinel, sentinel
from utilities.types import get_class_name

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from tqdm import tqdm as tqdm_type

_T = TypeVar("_T")


def pmap(
    func: Callable[..., _T],
    /,
    *iterables: Iterable[Any],
    parallelism: Literal["processes", "threads"] = "processes",
    n_jobs: int | None = None,
    bounded: bool = False,
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "immediate",
    tqdm_class: tqdm_type = tqdm_auto,  # type: ignore[]
    desc: str | None | Sentinel = sentinel,
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
        tqdm_class=tqdm_class,
        desc=desc,
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
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "immediate",
    tqdm_class: tqdm_type = tqdm_auto,  # type: ignore[]
    desc: str | None | Sentinel = sentinel,
    **kwargs: Any,
) -> list[_T]:
    """Parallel starmap, powered by `pqdm`."""
    n_jobs = _get_n_jobs(n_jobs)
    match parallelism:
        case "processes":
            result = processes.pqdm(
                iterable,
                partial(_starmap_helper, func),
                n_jobs=n_jobs,
                argument_type="args",
                bounded=bounded,
                exception_behaviour=exception_behaviour,
                tqdm_class=tqdm_class,
                **_get_desc(desc, func),
                **kwargs,
            )
        case "threads":
            result = threads.pqdm(
                iterable,
                partial(_starmap_helper, func),
                n_jobs=n_jobs,
                argument_type="args",
                bounded=bounded,
                exception_behaviour=exception_behaviour,
                tqdm_class=tqdm_class,
                **_get_desc(desc, func),
                **kwargs,
            )
        case _ as never:  # type: ignore[]
            assert_never(never)
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
