from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.constants import EFFECTIVE_GROUP_NAME, EFFECTIVE_USER_NAME
from utilities.core import chown, get_file_group, get_file_owner

if TYPE_CHECKING:
    from pathlib import Path


class TestChOwn:
    def test_none(self, *, temp_file: Path) -> None:
        chown(temp_file)

    def test_recursive(self, *, temp_file: Path) -> None:
        chown(temp_file, recursive=True)

    def test_user(self, *, temp_file: Path) -> None:
        chown(temp_file, user=EFFECTIVE_USER_NAME)
        result = get_file_owner(temp_file)
        assert result == EFFECTIVE_USER_NAME

    def test_group(self, *, temp_file: Path) -> None:
        chown(temp_file, group=EFFECTIVE_GROUP_NAME)
        result = get_file_group(temp_file)
        assert result == EFFECTIVE_GROUP_NAME

    def test_user_and_group(self, *, temp_file: Path) -> None:
        chown(temp_file, user=EFFECTIVE_USER_NAME, group=EFFECTIVE_GROUP_NAME)
        owner = get_file_owner(temp_file)
        assert owner == EFFECTIVE_USER_NAME
        group = get_file_group(temp_file)
        assert group == EFFECTIVE_GROUP_NAME
