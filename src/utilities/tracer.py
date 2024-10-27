from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar, Token
from functools import partial, wraps
from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NoReturn,
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
_T = TypeVar("_T")


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
def tracer(func: _F, /, *, time_zone: ZoneInfo | str = ...) -> _F: ...
@overload
def tracer(
    func: None = None, /, *, time_zone: ZoneInfo | str = ...
) -> Callable[[_F], _F]: ...
def tracer(
    func: _F | None = None, *, time_zone: ZoneInfo | str = UTC
) -> _F | Callable[[_F], _F]:
    """Context manager for tracing function calls."""
    if func is None:
        result = partial(tracer, time_zone=time_zone)
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

    if iscoroutinefunction(func):

        @wraps(func)
        async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
            node_data, tree, tracer_data, token = _initialize(
                base_data, time_zone=time_zone, **kwargs
            )
            try:
                result = await func(*args, **kwargs)
            except Exception as error:  # noqa: BLE001
                _handle_error(node_data, error)
            else:
                return _handle_success(node_data, result)
            finally:
                _cleanup(node_data, tracer_data, token, time_zone=time_zone, tree=tree)

        return cast(Any, wrapped_async)

    @wraps(func)
    def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
        node_data, tree, tracer_data, token = _initialize(
            base_data, time_zone=time_zone, **kwargs
        )
        try:
            result = func(*args, **kwargs)
        except Exception as error:  # noqa: BLE001
            _handle_error(node_data, error)
        else:
            return _handle_success(node_data, result)
        finally:
            _cleanup(node_data, tracer_data, token, time_zone=time_zone, tree=tree)

    return cast(Any, wrapped_sync)


def get_tracer_trees() -> list[Tree]:
    """Get the tracer trees."""
    return _TRACER_CONTEXT.get()["trees"]


def set_tracer_trees(trees: Iterable[Tree], /) -> None:
    """Set the tracer tree."""
    _ = _TRACER_CONTEXT.set(_TracerData(trees=list(trees)))


def _initialize(
    node_data: _NodeData, /, *, time_zone: ZoneInfo | str = UTC, **kwargs: Any
) -> tuple[_NodeData, Tree | None, _TracerData, Token[_TracerData]]:
    new_node_data = node_data.copy()
    new_node_data["start_time"] = get_now(time_zone=time_zone)
    new_node_data["kwargs"] = kwargs
    tracer_data: _TracerData = _TRACER_CONTEXT.get()
    if (tree := tracer_data.get("tree")) is None:
        tree_use = tracer_data["tree"] = Tree()
        tracer_data["trees"].append(tree_use)
    else:
        tree_use = tree
    tag = ":".join([node_data["module"], node_data["qualname"]])
    parent_node = tracer_data.get("node")
    child = tree_use.create_node(tag=tag, parent=parent_node, data=new_node_data)
    token = _TRACER_CONTEXT.set(
        _TracerData(trees=tracer_data["trees"], tree=tree_use, node=child)
    )
    return new_node_data, tree, tracer_data, token


def _handle_error(node_data: _NodeData, error: Exception, /) -> NoReturn:
    node_data["outcome"] = "failure"
    node_data["error"] = type(error)
    raise error


def _handle_success(node_data: _NodeData, result: _T, /) -> _T:
    node_data["outcome"] = "success"
    return result


def _cleanup(
    node_data: _NodeData,
    tracer_data: _TracerData,
    token: Token[_TracerData],
    /,
    *,
    time_zone: ZoneInfo | str = UTC,
    tree: Tree | None = None,
) -> None:
    end_time = node_data["end_time"] = get_now(time_zone=time_zone)
    node_data["duration"] = end_time - node_data["start_time"]
    if tree is None:
        del tracer_data["tree"]
    _TRACER_CONTEXT.reset(token)


__all__ = ["get_tracer_trees", "set_tracer_trees", "tracer"]
