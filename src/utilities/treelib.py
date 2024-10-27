from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from treelib import Node, Tree

from utilities.text import ensure_str

if TYPE_CHECKING:
    from collections.abc import Callable


def filter_tree(
    tree: Tree,
    /,
    *,
    tag: Callable[[str], bool] | None = None,
    identifier: Callable[[str], bool] | None = None,
    data: Callable[[Any], bool] | None = None,
) -> Tree:
    """Filter a tree."""
    subtree = Tree()
    _filter_tree_add(
        tree, subtree, ensure_str(tree.root), tag=tag, identifier=identifier, data=data
    )
    return subtree


def _filter_tree_add(
    old: Tree,
    new: Tree,
    node_id: str,
    /,
    *,
    parent_id: str | None = None,
    tag: Callable[[str], bool] | None = None,
    identifier: Callable[[str], bool] | None = None,
    data: Callable[[Any], bool] | None = None,
) -> None:
    node = cast(Node, old.get_node(node_id))
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
