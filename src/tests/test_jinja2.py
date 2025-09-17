from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


class TestEnhancedTemplate:
    def test_main(self, *, tmp_path: Path) -> None:
        pass
