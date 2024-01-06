from __future__ import annotations

from types import NoneType

from utilities.ipython import check_ipython_class, is_ipython


class TestCheckIPythonClass:
    def test_main(self) -> None:
        assert not check_ipython_class(NoneType)


class TestIsIPython:
    def test_main(self) -> None:
        assert not is_ipython()
