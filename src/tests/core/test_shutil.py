from __future__ import annotations

from pathlib import Path

from pytest import raises

from utilities.constants import EFFECTIVE_GROUP_NAME, EFFECTIVE_USER_NAME
from utilities.core import WhichError, chown, get_file_group, get_file_owner, which


class TestChOwn:
    def test_none(self, *, temp_file: Path) -> None:
        chown(temp_file)

    def test_recursive(self, *, temp_file: Path) -> None:
        chown(temp_file, recursive=True)

    def test_user(self, *, temp_file: Path) -> None:
        chown(temp_file, user=EFFECTIVE_USER_NAME)
        assert get_file_owner(temp_file) == EFFECTIVE_USER_NAME

    def test_group(self, *, temp_file: Path) -> None:
        chown(temp_file, group=EFFECTIVE_GROUP_NAME)
        assert get_file_group(temp_file) == EFFECTIVE_GROUP_NAME

    def test_user_and_group(self, *, temp_file: Path) -> None:
        chown(temp_file, user=EFFECTIVE_USER_NAME, group=EFFECTIVE_GROUP_NAME)
        assert get_file_owner(temp_file) == EFFECTIVE_USER_NAME
        assert get_file_group(temp_file) == EFFECTIVE_GROUP_NAME


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
