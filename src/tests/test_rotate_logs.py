from pathlib import Path

from click.testing import CliRunner

from utilities.rotate_logs import (
    _key,
    _rotate_item,
    _rotate_items,
    _yield_items,
    main,
)
from utilities.rotate_logs.classes import Item


class TestRotateLogs:
    def test_cli(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.log")
        path.touch()
        runner = CliRunner()
        args = ["--path", tmp_path.as_posix(), "--size", "0"]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    def test_dry_run(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.log")
        path.touch()
        runner = CliRunner()
        args = ["--path", tmp_path.as_posix(), "--size", "0", "--dry-run"]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    def test_rotate_items_one(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.log")
        path.touch()
        (items,) = _yield_items(path=tmp_path, size=0)
        _rotate_items(items, keep=1)
        contents = set(tmp_path.iterdir())
        expected = {tmp_path.joinpath("file.log.1")}
        assert contents == expected

    def test_rotate_items_two_growing(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.log")
        path.touch()
        tail = tmp_path.joinpath("file.log.1")
        tail.touch()
        (items,) = _yield_items(path=tmp_path, size=0)
        _rotate_items(items, keep=2)
        contents = set(tmp_path.iterdir())
        expected = {tail, tmp_path.joinpath("file.log.2")}
        assert contents == expected

    def test_rotate_items_two_at_limit(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.log")
        path.touch()
        tail = tmp_path.joinpath("file.log.1")
        tail.touch()
        (items,) = _yield_items(path=tmp_path, size=0)
        _rotate_items(items, keep=1)
        contents = set(tmp_path.iterdir())
        expected = {tail}
        assert contents == expected

    def test_rotate_item_moved(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.log")
        path.touch()
        ((item,),) = _yield_items(path=tmp_path, size=0)
        _rotate_item(item, keep=1)
        contents = set(tmp_path.iterdir())
        expected = {tmp_path.joinpath("file.log.1")}
        assert contents == expected

    def test_rotate_item_deleted(self, tmp_path: Path) -> None:
        head = tmp_path.joinpath("file.log")
        head.touch()
        tail = tmp_path.joinpath("file.log.1")
        tail.touch()
        (items,) = _yield_items(path=tmp_path, size=0)
        _, item = sorted(items, key=_key)
        _rotate_item(item, keep=1)
        contents = set(tmp_path.iterdir())
        expected = {head}
        assert contents == expected

    def test_yield_items_no_base(self, tmp_path: Path) -> None:
        items = frozenset(_yield_items(path=tmp_path, size=0))
        expected = frozenset()
        assert items == expected

    def test_yield_items_one_base_one_item(self, tmp_path: Path) -> None:
        head = tmp_path.joinpath("file.log")
        head.touch()
        items = frozenset(_yield_items(path=tmp_path, size=0))
        expected = frozenset([frozenset([Item(head, head)])])
        assert items == expected

    def test_yield_items_one_base_two_items(self, tmp_path: Path) -> None:
        head = tmp_path.joinpath("file.log")
        head.touch()
        tail = tmp_path.joinpath("file.log.1")
        tail.touch()
        items = frozenset(_yield_items(path=tmp_path, size=0))
        expected = frozenset(
            [frozenset([Item(head, head), Item(tail, head, num=1)])],
        )
        assert items == expected

    def test_yield_items_two_bases(self, tmp_path: Path) -> None:
        foo_head = tmp_path.joinpath("foo.log")
        foo_head.touch()
        foo_tail = tmp_path.joinpath("foo.log.1")
        foo_tail.touch()
        bar_head = tmp_path.joinpath("bar.log")
        bar_head.touch()
        items = frozenset(_yield_items(path=tmp_path, size=0))
        expected = frozenset(
            [
                frozenset(
                    [Item(foo_head, foo_head), Item(foo_tail, foo_head, num=1)],
                ),
                frozenset([Item(bar_head, bar_head)]),
            ],
        )
        assert items == expected
