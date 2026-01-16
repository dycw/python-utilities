from __future__ import annotations

from pytest import mark, param

from utilities.shellingham import SHELL, Shell, _get_shell_match, get_shell
from utilities.typing import get_args


class TestGetShell:
    def test_function(self) -> None:
        assert get_shell() in get_args(Shell)

    def test_constant(self) -> None:
        assert SHELL in get_args(Shell)


class TestGetShellMatch:
    @mark.parametrize(
        ("shell", "candidate", "expected"),
        [
            param("sh", "sh", True),
            param("sh", "bash", False),
            param("bash", "bash", True),
            param("bash", "sh", False),
            param("/bin/sh", "sh", True),
            param("/bin/sh", "bash", False),
            param("/bin/bash", "sh", False),
            param("/bin/bash", "bash", True),
        ],
    )
    def test_main(self, *, shell: str, candidate: Shell, expected: bool) -> None:
        result = _get_shell_match(shell, candidate)
        assert result is expected
