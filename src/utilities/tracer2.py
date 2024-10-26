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
from utilities.sentinel import sentinel
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
    qualname: str
    kwargs: StrMapping
    start_time: dt.datetime
    end_time: dt.datetime
    duration: dt.timedelta
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

    base_data = _NodeData(
        module=func.__module__,
        qualname=func.__qualname__,
        kwargs=sentinel,
        start_time=sentinel,
        end_time=sentinel,
        duration=sentinel,
        outcome=sentinel,
    )
    tag = ":".join([base_data["module"], base_data["qualname"]])

    if iscoroutinefunction(func):

        @wraps(func)
        async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
            node_data = base_data.copy()
            start_time = node_data["start_time"] = get_now(time_zone=time_zone)
            node_data["kwargs"] = kwargs
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
                result = await func(*args, **kwargs)
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

        return cast(Any, wrapped_async)

    @wraps(func)
    def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
        node_data = base_data.copy()
        start_time = node_data["start_time"] = get_now(time_zone=time_zone)
        node_data["kwargs"] = kwargs
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

    return cast(Any, wrapped_sync)


def get_tracer_trees() -> list[Tree]:
    """Get the tracer trees."""
    return _TRACER_CONTEXT.get()["trees"]


def set_tracer_trees(trees: Iterable[Tree], /) -> None:
    """Set the tracer tree."""
    _ = _TRACER_CONTEXT.set(_TracerData(trees=list(trees)))


__all__ = ["get_tracer_trees", "set_tracer_trees", "tracer2"]
