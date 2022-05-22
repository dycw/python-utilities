from functools import partial
from multiprocessing import cpu_count
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Literal
from typing import Optional
from typing import TypeVar
from typing import cast

from pqdm import processes

from dycw_utilities.tqdm import tqdm


_T = TypeVar("_T")


def pmap(
    func: Callable[..., _T],
    /,
    *iterables: Iterable[Any],
    parallelism: Literal["processes", "threads"] = "processes",
    n_jobs: Optional[int] = None,
    bounded: bool = False,
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "ignore",
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
        **kwargs,
    )


def pstarmap(
    func: Callable[..., _T],
    iterable: Iterable[tuple[Any, ...]],
    /,
    *,
    parallelism: Literal["processes", "threads"] = "processes",
    n_jobs: Optional[int] = None,
    bounded: bool = False,
    exception_behaviour: Literal["ignore", "immediate", "deferred"] = "ignore",
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
            **kwargs,
        )
    return list(result)


def _get_n_jobs(n_jobs: Optional[int], /) -> int:
    if (n_jobs is None) or (n_jobs <= 0):
        return cpu_count()
    else:
        return n_jobs


def _starmap_helper(func: Callable[..., _T], *args: Any) -> _T:
    return func(*args)
