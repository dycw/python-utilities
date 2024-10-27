from __future__ import annotations

from pytest import CaptureFixture, fixture
from treelib import Tree

from utilities.text import strip_and_dedent
from utilities.treelib import filter_tree


@fixture
def tree() -> Tree:
    tree = Tree()
    _ = tree.create_node("Root", "r", data={"num": 1})
    _ = tree.create_node("Child1", "c1", parent="r", data={"num": 2})
    _ = tree.create_node("Child2", "c2", parent="r", data={"num": 3})
    _ = tree.create_node("Grandchild1", "gc1", parent="c1", data={"num": 4})
    _ = tree.create_node("Grandchild2", "gc2", parent="c1", data={"num": 5})
    _ = tree.create_node("Grandchild3", "gc3", parent="c2", data={"num": 6})
    return tree


class TestFilterTree:
    def test_no_filter(self, *, tree: Tree, capsys: CaptureFixture) -> None:
        subtree = filter_tree(tree)
        print(str(subtree))  # noqa: T201
        out = capsys.readouterr().out.strip("\n")
        expected = strip_and_dedent("""
            Root
            ├── Child1
            │   ├── Grandchild1
            │   └── Grandchild2
            └── Child2
                └── Grandchild3
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

    def test_identifier(self, *, tree: Tree, capsys: CaptureFixture) -> None:
        subtree = filter_tree(tree, identifier=lambda id_: id_ != "gc3")
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

    def test_data(self, *, tree: Tree, capsys: CaptureFixture) -> None:
        subtree = filter_tree(tree, data=lambda data: data["num"] <= 5)
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
