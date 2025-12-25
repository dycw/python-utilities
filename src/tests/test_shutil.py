from __future__ import annotations

from pathlib import Path

from pytest import raises

from utilities.shutil import WhichError, which


class TestWhich:
    def test_main(self) -> None:
        result = which("bash")
        expected = [
            Path("/bin/bash"),
            Path("/usr/bin/bash"),
            Path("/opt/homebrew/bin/bash"),
        ]
        assert result in expected

    def test_error(self) -> None:
        with raises(WhichError, match="'invalid' not found"):
            _ = which("invalid")
