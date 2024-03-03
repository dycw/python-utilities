from __future__ import annotations

from operator import itemgetter
from time import sleep
from typing import TYPE_CHECKING

import pytest

from tests.conftest import FLAKY
from utilities.cacher import cache_to_disk
from utilities.functions import identity
from utilities.iterables import one
from utilities.types import EnsureClassError

if TYPE_CHECKING:
    from pathlib import Path


class TestCacheToDisk:
    def test_main(self, *, tmp_path: Path) -> None:
        counter = 0

        @cache_to_disk(root=tmp_path)
        def func(x: int, /) -> int:
            nonlocal counter
            counter += 1
            return x

        assert len(list(tmp_path.iterdir())) == 0
        assert func(0) == 0
        path = one(one(tmp_path.iterdir()).iterdir())
        assert path.name == "func"
        assert len(list(path.iterdir())) == counter == 1
        assert func(0) == 0
        assert len(list(path.iterdir())) == counter == 1
        for _ in range(2):
            assert func(1) == 1
            assert len(list(path.iterdir())) == counter == 2
        for _ in range(2):
            assert func(2) == 2
            assert len(list(path.iterdir())) == 3

    def test_max_size(self, *, tmp_path: Path) -> None:
        func = cache_to_disk(root=tmp_path, max_size=2)(identity)
        assert func(0) == 0
        path = one(one(tmp_path.iterdir()).iterdir())
        assert len(list(path.iterdir())) == 1
        sleep(duration := 0.01)
        assert func(1) == 1
        assert len(list(path.iterdir())) == 2
        mapping = {p: p.stat().st_mtime for p in path.iterdir()}
        as_list = sorted(mapping.items(), key=itemgetter(1))
        pre_old, pre_new = (time for _, time in as_list)
        sleep(duration)
        assert func(2) == 2
        assert len(list(path.iterdir())) == 2
        mapping = {p: p.stat().st_mtime for p in path.iterdir()}
        as_list = sorted(mapping.items(), key=itemgetter(1))
        post_old, post_new = (time for _, time in as_list)
        assert pre_old < pre_new == post_old < post_new

    def test_skip(self, *, tmp_path: Path) -> None:
        func = cache_to_disk(root=tmp_path, skip=True)(identity)
        assert len(list(tmp_path.iterdir())) == 0
        assert func(0) == 0
        assert len(list(tmp_path.iterdir())) == 0

    @FLAKY
    def test_max_duration(self, *, tmp_path: Path) -> None:
        max_duration = 0.1
        func = cache_to_disk(root=tmp_path, max_duration=max_duration)(identity)
        assert func(0) == 0
        path = one(one(tmp_path.iterdir()).iterdir())
        orig = path.stat().st_mtime
        assert func(0) == 0
        assert path.stat().st_mtime == orig
        sleep(2 * max_duration)
        assert func(1) == 1
        assert path.stat().st_mtime > orig

    def test_args_and_kwargs_resolved(self, *, tmp_path: Path) -> None:
        @cache_to_disk(root=tmp_path)
        def add(x: int, y: int) -> int:
            return x + y

        assert add(0, 0) == 0
        path = one(one(tmp_path.iterdir()).iterdir())
        assert len(list(path.iterdir())) == 1
        assert add(0, y=0) == 0
        assert len(list(path.iterdir())) == 1
        assert add(x=0, y=0) == 0
        assert len(list(path.iterdir())) == 1
        assert add(x=1, y=1) == 2
        assert len(list(path.iterdir())) == 2

    def test_rerun(self, *, tmp_path: Path) -> None:
        func = cache_to_disk(root=tmp_path)(identity)
        assert func(0) == 0
        path = one(one(tmp_path.iterdir()).iterdir())
        assert len(list(path.iterdir())) == 1
        pre = path.stat().st_mtime
        sleep(0.01)
        assert func(0, rerun=True) == 0  # type: ignore[]
        post = path.stat().st_mtime
        assert pre < post

    def test_rerun_not_a_boolean(self, *, tmp_path: Path) -> None:
        func = cache_to_disk(root=tmp_path)(identity)
        with pytest.raises(EnsureClassError):
            _ = func(0, rerun=None)  # type: ignore[]
