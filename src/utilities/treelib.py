from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

import treelib
from typing_extensions import override

from utilities.text import ensure_str

if TYPE_CHECKING:
    from collections.abc import Callable

_T = TypeVar("_T")


class Tree(treelib.Tree, Generic[_T]):
    """Typed version of `Tree`."""

    @override
    def get_node(self, nid: str) -> Node[_T] | None:
        return super().get_node(nid)


class Node(treelib.Node, Generic[_T]):
    """Typed version of `Node`."""

    @property
    @override
    def tag(self) -> str | None:
        return super().tag

    @tag.setter
    @override
    def tag(self, value: str | None) -> None:
        super().__setattr__("tag", value)


def filter_tree(
    tree: Tree[_T],
    /,
    *,
    tag: Callable[[str], bool] | None = None,
    identifier: Callable[[str], bool] | None = None,
    data: Callable[[_T], bool] | None = None,
) -> Tree[_T]:
    """Filter a tree."""
    subtree = Tree()
    _filter_tree_add(
        tree, subtree, ensure_str(tree.root), tag=tag, identifier=identifier, data=data
    )
    return subtree


def _filter_tree_add(
    old: Tree[_T],
    new: Tree[_T],
    node_id: str,
    /,
    *,
    parent_id: str | None = None,
    tag: Callable[[str], bool] | None = None,
    identifier: Callable[[str], bool] | None = None,
    data: Callable[[_T], bool] | None = None,
) -> None:
    node = old.get_node(node_id)
    predicates: set[bool] = set()
    if tag is not None:
        predicates.add(tag(ensure_str(node.tag)))
    if identifier is not None:
        predicates.add(identifier(ensure_str(node.identifier)))
    if data is not None:
        predicates.add(data(node.data))
    if all(predicates):
        _ = new.create_node(node.tag, node.identifier, parent=parent_id, data=node.data)
        for child in old.children(node_id):
            _filter_tree_add(
                old,
                new,
                child.identifier,
                parent_id=node.identifier,
                tag=tag,
                identifier=identifier,
                data=data,
            )


__all__ = ["filter_tree"]
