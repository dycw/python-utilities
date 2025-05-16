from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import sampled_from

from utilities.importlib import is_valid_import


class TestIsValidImport:
    @given(
        case=sampled_from([
            ("utilities.importlib", "is_valid_import", True),
            ("utilities.importlib", "invalid", False),
            ("invalid", "invalid", False),
        ])
    )
    def test_main(self, *, case: tuple[str, str, bool]) -> None:
        module, name, expected = case
        result = is_valid_import(module, name)
        assert result is expected
