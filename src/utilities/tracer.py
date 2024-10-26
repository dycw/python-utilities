from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict

from treelib import Tree

from utilities.sys import get_caller

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from treelib import Node

    from utilities.types import StrMapping

# types


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
    line_num: int
    name: str
    kwargs: StrMapping
    start_time: float
    end_time: NotRequired[float]
    outcome: Literal["success", "failure"]
    error: NotRequired[type[Exception]]


@contextmanager
def tracer(*, depth: int = 2, **kwargs: Any) -> Iterator[None]:
    """Context manager for tracing function calls."""
    caller = get_caller(depth=depth + 1)
    tag = ":".join([caller["module"], caller["name"]])
    node_data = _NodeData(
        module=caller["module"],
        line_num=caller["line_num"],
        name=caller["name"],
        kwargs=kwargs,
        start_time=perf_counter(),
        outcome="success",
    )
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
        yield None
    except Exception as error:
        node_data["outcome"] = "failure"
        node_data["error"] = type(error)
        raise
    finally:
        node_data["end_time"] = perf_counter()
        if tree is None:
            del tracer_data["tree"]
        _TRACER_CONTEXT.reset(token)


def get_tracer_trees() -> list[Tree]:
    """Get the tracer trees."""
    return _TRACER_CONTEXT.get()["trees"]


def set_tracer_trees(trees: Iterable[Tree], /) -> None:
    """Set the tracer tree."""
    _ = _TRACER_CONTEXT.set(_TracerData(trees=list(trees)))


__all__ = ["get_tracer_trees", "set_tracer_trees", "tracer"]
