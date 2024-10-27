from __future__ import annotations

from pytest import CaptureFixture, fixture
from treelib import Tree

from utilities.text import strip_and_dedent
from utilities.treelib import filter_tree


@fixture
def tree() -> Tree:
    tree = Tree()
    _ = tree.create_node("Root", "root")
    _ = tree.create_node("Child1", "child1", parent="root")
    _ = tree.create_node("Child2", "child2", parent="root")
    _ = tree.create_node("Grandchild1", "grandchild1", parent="child1")
    _ = tree.create_node("Grandchild2", "grandchild2", parent="child1")
    _ = tree.create_node("Grandchild3", "grandchild3", parent="child2")
    return tree


class TestFilterTree:
    def test_tag(self, *, tree: Tree, capsys: CaptureFixture) -> None:
        subtree = filter_tree(tree, tag=lambda t: t != "Grandchild3")
        print(str(subtree))  # noqa: T201
        out = capsys.readouterr().out.strip("\n")
        expected = strip_and_dedent("""
            Root
            ├── Child1
            │   ├── Grandchild1
            │   └── Grandchild2
            └── Child2
        """)
        assert out == expected

    def test_tag(self, *, tree: Tree, capsys: CaptureFixture) -> None:
        subtree = filter_tree(tree, tag=lambda t: t != "Grandchild3")
        print(str(subtree))  # noqa: T201
        out = capsys.readouterr().out.strip("\n")
        expected = strip_and_dedent("""
            Root
            ├── Child1
            │   ├── Grandchild1
            │   └── Grandchild2
            └── Child2
        """)
        assert out == expected
