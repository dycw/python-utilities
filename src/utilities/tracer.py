from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from functools import partial, wraps
from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    NoReturn,
    TypeVar,
    cast,
    overload,
)

import treelib
from treelib.exceptions import NodeIDAbsentError

from utilities.datetime import get_now
from utilities.functions import get_class_name
from utilities.sentinel import Sentinel, sentinel
from utilities.treelib import Tree, filter_tree
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Iterable
    from zoneinfo import ZoneInfo

    from utilities.treelib import Node
    from utilities.types import StrMapping

# types


_F = TypeVar("_F", bound=Callable[..., Any])
_T = TypeVar("_T")


@dataclass(kw_only=True, slots=True)
class _TracerData(Generic[_T]):
    trees: list[_TreeNodeData[_T]] = field(default_factory=list)
    tree: _TreeNodeData[_T] | None = None
    node: Node[NodeData[_T]] | None = None


@dataclass(kw_only=True, slots=True)
class NodeData(Generic[_T]):
    """A collection of data at each call."""

    module: str
    qualname: str
    args: tuple[Any, ...] | None = None
    kwargs: StrMapping | None = None
    start_time: dt.datetime
    end_time: dt.datetime | None = None
    outcome: Literal["success", "failure", "suppressed"] | None = None
    result: _T | Sentinel = sentinel
    error: Exception | None = None

    @property
    def desc(self) -> str:
        terms: list[Any] = []
        if (self.outcome == "failure") and (self.error is not None):
            terms.append(get_class_name(self.error))
        terms.append(self.duration)
        joined = ", ".join(map(str, terms))
        return f"{self.tag} ({joined})"

    @property
    def duration(self) -> dt.timedelta | None:
        return None if self.end_time is None else (self.end_time - self.start_time)

    @property
    def tag(self) -> str:
        return f"{self.module}:{self.qualname}"


_TreeNodeData = Tree[NodeData[_T]]


# context vars


_TRACER_CONTEXT: ContextVar[_TracerData[Any]] = ContextVar(
    "_CURRENT_TRACER_NODE", default=_TracerData()
)


@overload
def tracer(
    func: _F,
    /,
    *,
    time_zone: ZoneInfo | str = ...,
    add_args: bool = ...,
    pre_call: Callable[..., None] | None = ...,
    suppress: type[Exception] | tuple[type[Exception], ...] | None = ...,
    post_error: Callable[[NodeData[Any]], None] | None = ...,
    add_result: bool = ...,
    post_result: Callable[[NodeData[Any], Any], None] | None = ...,
) -> _F: ...
@overload
def tracer(
    func: None = None,
    /,
    *,
    time_zone: ZoneInfo | str = ...,
    add_args: bool = ...,
    pre_call: Callable[..., None] | None = ...,
    suppress: type[Exception] | tuple[type[Exception], ...] | None = ...,
    post_error: Callable[[NodeData[Any]], None] | None = ...,
    add_result: bool = ...,
    post_result: Callable[[NodeData[Any], Any], None] | None = ...,
) -> Callable[[_F], _F]: ...
def tracer(
    func: _F | None = None,
    /,
    *,
    time_zone: ZoneInfo | str = UTC,
    add_args: bool = False,
    pre_call: Callable[..., None] | None = None,
    suppress: type[Exception] | tuple[type[Exception], ...] | None = None,
    post_error: Callable[[NodeData[Any]], None] | None = None,
    add_result: bool = False,
    post_result: Callable[[NodeData[Any], Any], None] | None = None,
) -> _F | Callable[[_F], _F]:
    """Context manager for tracing function calls."""
    if func is None:
        result = partial(
            tracer,
            add_args=add_args,
            time_zone=time_zone,
            pre_call=pre_call,
            suppress=suppress,
            post_error=post_error,
            add_result=add_result,
            post_result=post_result,
        )
        return cast(Callable[[_F], _F], result)

    if iscoroutinefunction(func):

        @wraps(func)
        async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
            node_data, tree, tracer_data, token = _initialize(
                func, args, kwargs, time_zone=time_zone, add_args=add_args
            )
            if pre_call is not None:
                pre_call(node_data, *args, **kwargs)
            try:
                result = await func(*args, **kwargs)
            except Exception as error:  # noqa: BLE001
                _handle_error(
                    node_data,
                    args,
                    kwargs,
                    error,
                    time_zone=time_zone,
                    suppress=suppress,
                    post_error=post_error,
                )
            else:
                return _handle_result(
                    node_data,
                    result,
                    time_zone=time_zone,
                    add_result=add_result,
                    post_result=post_result,
                )
            finally:
                _cleanup(tracer_data, token, tree=tree)

        return cast(Any, wrapped_async)

    @wraps(func)
    def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
        node_data, tree, tracer_data, token = _initialize(
            func, args, kwargs, time_zone=time_zone, add_args=add_args
        )
        if pre_call is not None:
            pre_call(node_data, *args, **kwargs)
        try:
            result = func(*args, **kwargs)
        except Exception as error:  # noqa: BLE001
            _handle_error(
                node_data,
                args,
                kwargs,
                error,
                time_zone=time_zone,
                suppress=suppress,
                post_error=post_error,
            )
        else:
            return _handle_result(
                node_data,
                result,
                time_zone=time_zone,
                add_result=add_result,
                post_result=post_result,
            )
        finally:
            _cleanup(tracer_data, token, tree=tree)

    return cast(Any, wrapped_sync)


def get_tracer_trees() -> list[_TreeNodeData[Any]]:
    """Get the tracer trees."""
    return _TRACER_CONTEXT.get().trees


def set_tracer_trees(trees: Iterable[_TreeNodeData[Any]], /) -> None:
    """Set the tracer tree."""
    _ = _TRACER_CONTEXT.set(_TracerData(trees=list(trees)))


def _initialize(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: StrMapping,
    /,
    *,
    time_zone: ZoneInfo | str = UTC,
    add_args: bool = False,
) -> tuple[NodeData[_T], _TreeNodeData | None, _TracerData[_T], Token[_TracerData[_T]]]:
    node_data: NodeData[_T] = NodeData(
        module=func.__module__,
        qualname=func.__qualname__,
        start_time=get_now(time_zone=time_zone),
    )
    if add_args:
        node_data.args = args
        node_data.kwargs = kwargs
    tracer_data: _TracerData[_T] = _TRACER_CONTEXT.get()
    if (tree := tracer_data.tree) is None:
        tree_use = tracer_data.tree = Tree()
        tracer_data.trees.append(tree_use)
    else:
        tree_use = tree
    child = tree_use.create_node(
        tag=node_data.tag, parent=tracer_data.node, data=node_data
    )
    token = _TRACER_CONTEXT.set(
        _TracerData(trees=tracer_data.trees, tree=tree_use, node=child)
    )
    return node_data, tree, tracer_data, token


def _handle_error(
    node_data: NodeData[_T],
    args: tuple[Any, ...],
    kwargs: StrMapping,
    error: Exception,
    /,
    *,
    time_zone: ZoneInfo | str = UTC,
    suppress: type[Exception] | tuple[type[Exception], ...] | None = None,
    post_error: Callable[[NodeData[_T]], None] | None = None,
) -> NoReturn:
    node_data.args = args
    node_data.kwargs = kwargs
    if (suppress is not None) and isinstance(error, suppress):
        outcome = "suppressed"
    else:
        outcome = "failure"
    _set_end_time_and_outcome(node_data, outcome, time_zone=time_zone)
    node_data.error = error
    if post_error is not None:
        post_error(node_data)
    raise error


def _handle_result(
    node_data: NodeData[_T],
    result: _T,
    /,
    *,
    time_zone: ZoneInfo | str = UTC,
    add_result: bool = False,
    post_result: Callable[[NodeData[_T], _T], None] | None = None,
) -> _T:
    _set_end_time_and_outcome(node_data, "success", time_zone=time_zone)
    if add_result:
        node_data.result = result
    if post_result is not None:
        post_result(node_data, result)
    return result


def _set_end_time_and_outcome(
    node_data: NodeData[Any],
    outcome: Literal["success", "failure", "suppressed"],
    /,
    *,
    time_zone: ZoneInfo | str = UTC,
) -> None:
    node_data.end_time = get_now(time_zone=time_zone)
    node_data.outcome = outcome


def _cleanup(
    tracer_data: _TracerData,
    token: Token[_TracerData],
    /,
    *,
    tree: _TreeNodeData | None = None,
) -> None:
    if tree is None:
        tracer_data.tree = None
    _TRACER_CONTEXT.reset(token)


@overload
def filter_failures(tree: _TreeNodeData[_T], /) -> _TreeNodeData[_T] | None: ...
@overload
def filter_failures(
    tree: Iterable[_TreeNodeData[_T]], /
) -> list[_TreeNodeData[_T]]: ...
def filter_failures(
    tree: _TreeNodeData[_T] | Iterable[_TreeNodeData[_T]], /
) -> _TreeNodeData[_T] | list[_TreeNodeData[_T]] | None:
    """Filter a tree down to the failures."""
    if isinstance(tree, treelib.Tree):
        result = filter_tree(tree, data=lambda x: x.outcome == "failure")
        try:
            result[result.root]
        except NodeIDAbsentError:
            return None
        return result
    trees = list(map(filter_failures, tree))
    return [t for t in trees if t is not None]


__all__ = [
    "NodeData",
    "filter_failures",
    "get_tracer_trees",
    "set_tracer_trees",
    "tracer",
]
