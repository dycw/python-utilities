from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Any, assert_never

from utilities.iterables import apply_to_tuple
from utilities.os import get_cpu_use
from utilities.types import Parallelism

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from multiprocessing.context import BaseContext

    from utilities.os import IntOrAll


def concurrent_map[T](
    func: Callable[..., T],
    /,
    *iterables: Iterable[Any],
    parallelism: Parallelism = "processes",
    max_workers: IntOrAll = "all",
    mp_context: BaseContext | None = None,
    initializer: Callable[[], object] | None = None,
    initargs: tuple[Any, ...] = (),
    max_tasks_per_child: int | None = None,
    thread_name_prefix: str = "",
    timeout: float | None = None,
    chunksize: int = 1,
) -> list[T]:
    """Concurrent map."""
    return concurrent_starmap(
        func,
        zip(*iterables, strict=True),
        parallelism=parallelism,
        max_workers=max_workers,
        mp_context=mp_context,
        initializer=initializer,
        initargs=initargs,
        max_tasks_per_child=max_tasks_per_child,
        thread_name_prefix=thread_name_prefix,
        timeout=timeout,
        chunksize=chunksize,
    )


##


def concurrent_starmap[T](
    func: Callable[..., T],
    iterable: Iterable[tuple[Any, ...]],
    /,
    *,
    parallelism: Parallelism = "processes",
    max_workers: IntOrAll = "all",
    mp_context: BaseContext | None = None,
    initializer: Callable[[], object] | None = None,
    initargs: tuple[Any, ...] = (),
    max_tasks_per_child: int | None = None,
    thread_name_prefix: str = "",
    timeout: float | None = None,
    chunksize: int = 1,
) -> list[T]:
    """Concurrent map."""
    max_workers_use = get_cpu_use(n=max_workers)
    apply = partial(apply_to_tuple, func)
    match parallelism:
        case "processes":
            with ProcessPoolExecutor(
                max_workers=max_workers_use,
                mp_context=mp_context,
                initializer=initializer,
                initargs=initargs,
                max_tasks_per_child=max_tasks_per_child,
            ) as pool:
                result = pool.map(apply, iterable, timeout=timeout, chunksize=chunksize)
        case "threads":
            with ThreadPoolExecutor(
                max_workers=max_workers_use,
                thread_name_prefix=thread_name_prefix,
                initializer=initializer,
                initargs=initargs,
            ) as pool:
                result = pool.map(apply, iterable, timeout=timeout, chunksize=chunksize)
        case _ as never:
            assert_never(never)
    return list(result)


__all__ = ["Parallelism", "concurrent_map", "concurrent_starmap"]
