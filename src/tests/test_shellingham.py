from __future__ import annotations

from utilities.shellingham import SHELL, Shell, get_shell
from utilities.typing import get_args


class TestGetShell:
    def test_function(self) -> None:
        assert get_shell() in get_args(Shell)

    def test_constant(self) -> None:
        assert SHELL in get_args(Shell)
