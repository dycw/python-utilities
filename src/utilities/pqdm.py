from __future__ import annotations

from functools import partial
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypeVar, assert_never

from pqdm import processes, threads
from tqdm.auto import tqdm as tqdm_auto

from utilities.functions import get_func_name
from utilities.iterables import apply_starmap
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from tqdm import tqdm as tqdm_type

    from utilities.concurrent import _Parallelism


_T = TypeVar("_T")
_ExceptionBehaviour: TypeAlias = Literal["ignore", "immediate", "deferred"]


def pmap(
    func: Callable[..., _T],
    /,
    *iterables: Iterable[Any],
    parallelism: _Parallelism = "processes",
    n_jobs: int | None = None,
    bounded: bool = False,
    exception_behaviour: _ExceptionBehaviour = "immediate",
    tqdm_class: tqdm_type = tqdm_auto,  # pyright: ignore[reportArgumentType]
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
    parallelism: _Parallelism = "processes",
    n_jobs: int | None = None,
    bounded: bool = False,
    exception_behaviour: _ExceptionBehaviour = "immediate",
    tqdm_class: tqdm_type = tqdm_auto,  # pyright: ignore[reportArgumentType]
    desc: str | None | Sentinel = sentinel,
    **kwargs: Any,
) -> list[_T]:
    """Parallel starmap, powered by `pqdm`."""
    n_jobs = _get_n_jobs(n_jobs)
    match parallelism:
        case "processes":
            result = processes.pqdm(
                iterable,
                partial(apply_starmap, func),
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
                partial(apply_starmap, func),
                n_jobs=n_jobs,
                argument_type="args",
                bounded=bounded,
                exception_behaviour=exception_behaviour,
                tqdm_class=tqdm_class,
                **_get_desc(desc, func),
                **kwargs,
            )
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    return list(result)


def _get_n_jobs(n_jobs: int | None, /) -> int:
    if (n_jobs is None) or (n_jobs <= 0):
        return cpu_count()  # pragma: no cover
    return n_jobs


def _get_desc(
    desc: str | None | Sentinel, func: Callable[..., Any], /
) -> dict[str, str]:
    desc_use = get_func_name(func) if isinstance(desc, Sentinel) else desc
    return {} if desc_use is None else {"desc": desc_use}


__all__ = ["pmap", "pstarmap"]
