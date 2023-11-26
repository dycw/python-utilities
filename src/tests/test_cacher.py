from __future__ import annotations

from pathlib import Path
from time import sleep

from utilities.cacher import cache_to_disk
from utilities.more_itertools import one


class TestCacheToDisk:
    def test_main(self, *, tmp_path: Path) -> None:
        path_inc = tmp_path.joinpath("increment")

        @cache_to_disk(root=tmp_path)
        def increment(x: int, /) -> int:
            return x + 1

        assert len(list(tmp_path.iterdir())) == 0
        assert increment(0) == 1
        assert set(tmp_path.iterdir()) == {path_inc}
        assert len(list(path_inc.iterdir())) == 1
        assert increment(0) == 1
        assert len(list(path_inc.iterdir())) == 1
        assert increment(1) == 2
        assert len(list(path_inc.iterdir())) == 2

    def test_ttl(self, *, tmp_path: Path) -> None:
        ttl = 0.1
        path_inc = tmp_path.joinpath("increment")

        @cache_to_disk(root=tmp_path, ttl=ttl)
        def increment(x: int, /) -> int:
            return x + 1

        assert len(list(tmp_path.iterdir())) == 0
        assert increment(0) == 1
        assert set(tmp_path.iterdir()) == {path_inc}
        path = one(path_inc.iterdir())
        orig = path.stat().st_mtime
        assert increment(0) == 1
        assert path.stat().st_mtime == orig
        sleep(2 * ttl)
        assert increment(0) == 1
        assert path.stat().st_mtime > orig

    def test_args_and_kwargs_resolved(self, *, tmp_path: Path) -> None:
        path_inc = tmp_path.joinpath("add")

        @cache_to_disk(root=tmp_path)
        def add(x: int, y: int) -> int:
            return x + y

        assert add(0, 0) == 0
        assert len(list(path_inc.iterdir())) == 1
        assert add(0, y=0) == 0
        assert len(list(path_inc.iterdir())) == 1
        assert add(x=0, y=0) == 0
        assert len(list(path_inc.iterdir())) == 1
        assert add(x=1, y=1) == 2
        assert len(list(path_inc.iterdir())) == 2
