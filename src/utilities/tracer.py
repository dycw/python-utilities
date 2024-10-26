from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from time import perf_counter
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

from treelib import Tree

from utilities.sys import get_caller

if TYPE_CHECKING:
    from collections.abc import Iterator

    from treelib import Node

    from utilities.types import StrMapping

# types


class _TreeAndNode(TypedDict):
    tree: Tree
    node: NotRequired[Node]


# context vars


_TRACER_CONTEXT: ContextVar[_TreeAndNode] = ContextVar("_CURRENT_TRACER_NODE")


class _NodeData(TypedDict):
    module: str
    line_num: int
    name: str
    kwargs: StrMapping
    start_time: float
    end_time: NotRequired[float]


@contextmanager
def tracer(*, depth: int = 2, **kwargs: Any) -> Iterator[None]:
    """Context manager for tracing function calls."""
    caller = get_caller(depth=depth + 1)
    data = _NodeData(
        module=caller["module"],
        line_num=caller["line_num"],
        name=caller["name"],
        kwargs=kwargs,
        start_time=perf_counter(),
    )
    try:
        curr: _TreeAndNode = _TRACER_CONTEXT.get()
    except LookupError:
        curr = _TreeAndNode(tree=Tree())
        _ = _TRACER_CONTEXT.set(curr)
    tree, parent = curr["tree"], curr.get("node")
    child = tree.create_node(
        tag=f"{data['module']}:{data['name']}", parent=parent, data=data
    )
    prev = _TRACER_CONTEXT.set(_TreeAndNode(tree=tree, node=child))
    data["start_time"] = perf_counter()
    try:
        yield None
    finally:
        data["end_time"] = perf_counter()
        _TRACER_CONTEXT.reset(prev)


def get_tracer_tree() -> Tree:
    """Get the tracer tree."""
    try:
        return _TRACER_CONTEXT.get()["tree"]
    except LookupError:
        return Tree()


def set_tracer_tree(tree: Tree, /) -> None:
    """Set the tracer tree."""
    _ = _TRACER_CONTEXT.set(_TreeAndNode(tree=tree))


__all__ = ["get_tracer_tree", "set_tracer_tree", "tracer"]
