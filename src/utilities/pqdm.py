from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any, Literal, TypeVar, assert_never

from pqdm import processes, threads
from tqdm.auto import tqdm as tqdm_auto

from utilities.functions import get_func_name
from utilities.iterables import apply_to_varargs
from utilities.os import get_cpu_use
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from tqdm import tqdm as tqdm_type

    from utilities.os import IntOrAll
    from utilities.types import Parallelism


_T = TypeVar("_T")
type _ExceptionBehaviour = Literal["ignore", "immediate", "deferred"]


def pqdm_map(
    func: Callable[..., _T],
    /,
    *iterables: Iterable[Any],
    parallelism: Parallelism = "processes",
    n_jobs: IntOrAll = "all",
    bounded: bool = False,
    exception_behaviour: _ExceptionBehaviour = "immediate",
    tqdm_class: tqdm_type = tqdm_auto,  # pyright: ignore[reportArgumentType]
    desc: str | None | Sentinel = sentinel,
    **kwargs: Any,
) -> list[_T]:
    """Parallel map, powered by `pqdm`."""
    return pqdm_starmap(
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


def pqdm_starmap(
    func: Callable[..., _T],
    iterable: Iterable[tuple[Any, ...]],
    /,
    *,
    parallelism: Parallelism = "processes",
    n_jobs: IntOrAll = "all",
    bounded: bool = False,
    exception_behaviour: _ExceptionBehaviour = "immediate",
    tqdm_class: tqdm_type = tqdm_auto,  # pyright: ignore[reportArgumentType]
    desc: str | None | Sentinel = sentinel,
    **kwargs: Any,
) -> list[_T]:
    """Parallel starmap, powered by `pqdm`."""
    apply = partial(apply_to_varargs, func)
    n_jobs_use = get_cpu_use(n=n_jobs)
    match parallelism:
        case "processes":
            result = processes.pqdm(
                iterable,
                apply,
                n_jobs=n_jobs_use,
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
                apply,
                n_jobs=n_jobs_use,
                argument_type="args",
                bounded=bounded,
                exception_behaviour=exception_behaviour,
                tqdm_class=tqdm_class,
                **_get_desc(desc, func),
                **kwargs,
            )
        case _ as never:
            assert_never(never)
    return list(result)


def _get_desc(
    desc: str | None | Sentinel, func: Callable[..., Any], /
) -> dict[str, str]:
    desc_use = get_func_name(func) if isinstance(desc, Sentinel) else desc
    return {} if desc_use is None else {"desc": desc_use}


__all__ = ["pqdm_map", "pqdm_starmap"]
