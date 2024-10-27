from __future__ import annotations

from typing import TYPE_CHECKING, cast

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
) -> Tree:
    """Filter a tree."""
    subtree = Tree()
    _filter_tree_add(
        tree, subtree, ensure_str(tree.root), tag=tag, identifier=identifier
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
) -> None:
    node = cast(Node, old.get_node(node_id))
    if (tag is not None) and tag(ensure_str(node.tag)):
        _ = new.create_node(node.tag, node.identifier, parent=parent_id, data=node.data)
        for child in old.children(node_id):
            _filter_tree_add(
                old,
                new,
                child.identifier,
                parent_id=node.identifier,
                tag=tag,
                identifier=ident,
            )


__all__ = ["filter_tree"]
