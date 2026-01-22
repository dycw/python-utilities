from __future__ import annotations

from typing import TYPE_CHECKING, assert_never

from utilities.constants import SYSTEM
from utilities.core import get_file_group

if TYPE_CHECKING:
    from pathlib import Path


class TestGetFileGroup:
    def test_group(self, *, temp_file: Path) -> None:
        result = get_file_group(temp_file)
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert result is None
            case "mac" | "linux":  # skipif-windows
                assert result is not None
            case never:
                assert_never(never)
