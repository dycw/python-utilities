from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypeVar, assert_never

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from multiprocessing.context import BaseContext


_T = TypeVar("_T")
_PARALLELISM: TypeAlias = Literal["processes", "threads"]


def concurrent_map(
    func: Callable[..., _T],
    /,
    *iterables: Iterable[Any],
    parallelism: _PARALLELISM = "processes",
    max_workers: int | None = None,
    mp_context: BaseContext | None = None,
    initializer: Callable[[], object] | None = None,
    initargs: tuple[Any, ...] = (),
    max_tasks_per_child: int | None = None,
    thread_name_prefix: str = "",
    timeout: float | None = None,
    chunksize: int = 1,
) -> list[_T]:
    """Concurrent map."""
    match parallelism:
        case "processes":
            with ProcessPoolExecutor(
                max_workers=max_workers,
                mp_context=mp_context,
                initializer=initializer,
                initargs=initargs,
                max_tasks_per_child=max_tasks_per_child,
            ) as pool:
                result = pool.map(
                    func, *iterables, timeout=timeout, chunksize=chunksize
                )
        case "threads":
            with ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix=thread_name_prefix,
                initializer=initializer,
                initargs=initargs,
            ) as pool:
                result = pool.map(
                    func, *iterables, timeout=timeout, chunksize=chunksize
                )
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    return list(result)


__all__ = ["concurrent_map"]
