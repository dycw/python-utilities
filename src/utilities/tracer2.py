from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from functools import partial, wraps
from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NotRequired,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

from treelib import Tree

from utilities.datetime import get_now
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Iterable
    from zoneinfo import ZoneInfo

    from treelib import Node

    from utilities.types import StrMapping

# types


_F = TypeVar("_F", bound=Callable[..., Any])


class _TracerData(TypedDict):
    trees: list[Tree]
    tree: NotRequired[Tree]
    node: NotRequired[Node]


# context vars


_DEFAULT: _TracerData = {"trees": []}
_TRACER_CONTEXT: ContextVar[_TracerData] = ContextVar(
    "_CURRENT_TRACER_NODE", default=_DEFAULT
)


class _NodeData(TypedDict):
    module: str
    name: str
    kwargs: StrMapping
    start_time: dt.datetime
    end_time: NotRequired[dt.datetime]
    duration: NotRequired[dt.timedelta]
    outcome: Literal["success", "failure"]
    error: NotRequired[type[Exception]]


@overload
def tracer2(func: _F, /, *, time_zone: ZoneInfo | str = ...) -> _F: ...
@overload
def tracer2(
    func: None = None, /, *, time_zone: ZoneInfo | str = ...
) -> Callable[[_F], _F]: ...
def tracer2(
    func: _F | None = None, *, time_zone: ZoneInfo | str = UTC
) -> _F | Callable[[_F], _F]:
    """Context manager for tracing function calls."""
    if func is None:
        result = partial(tracer2, time_zone=time_zone)
        return cast(Callable[[_F], _F], result)

    if iscoroutinefunction(func):
        raise NotImplementedError

    @wraps(func)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        start_time = get_now(time_zone=time_zone)
        node_data = _NodeData(
            module=func.__module__,
            name=func.__name__,
            kwargs=kwargs,
            start_time=start_time,
            outcome="failure",
        )
        tag = ":".join([node_data["module"], node_data["name"]])
        tracer_data: _TracerData = _TRACER_CONTEXT.get()
        if (tree := tracer_data.get("tree")) is None:
            tree_use = tracer_data["tree"] = Tree()
            tracer_data["trees"].append(tree_use)
        else:
            tree_use = tree
        parent_node = tracer_data.get("node")
        child = tree_use.create_node(tag=tag, parent=parent_node, data=node_data)
        token = _TRACER_CONTEXT.set(
            _TracerData(trees=tracer_data["trees"], tree=tree_use, node=child)
        )
        try:
            result = func(*args, **kwargs)
        except Exception as error:
            node_data["outcome"] = "failure"
            node_data["error"] = type(error)
            raise
        else:
            node_data["outcome"] = "success"
            return result
        finally:
            end_time = node_data["end_time"] = get_now(time_zone=time_zone)
            node_data["duration"] = end_time - start_time
            if tree is None:
                del tracer_data["tree"]
            _TRACER_CONTEXT.reset(token)

    return cast(Any, wrapped)


def get_tracer_trees() -> list[Tree]:
    """Get the tracer trees."""
    return _TRACER_CONTEXT.get()["trees"]


def set_tracer_trees(trees: Iterable[Tree], /) -> None:
    """Set the tracer tree."""
    _ = _TRACER_CONTEXT.set(_TracerData(trees=list(trees)))


__all__ = ["get_tracer_trees", "set_tracer_trees", "tracer2"]
