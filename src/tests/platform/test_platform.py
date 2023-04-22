from beartype import beartype
from hypothesis import given
from hypothesis.strategies import sets

from utilities.hypothesis import text_ascii
from utilities.platform import SYSTEM, System, _get_system, maybe_yield_lower_case
from utilities.typing import never


class TestMaybeYieldLowerCase:
    @given(text=sets(text_ascii()))
    @beartype
    def test_main(self, text: set[str]) -> None:
        result = set(maybe_yield_lower_case(text))
        if SYSTEM is System.windows:  # noqa: SIM114 # pragma: os-ne-windows
            assert all(text == text.lower() for text in result)
        elif SYSTEM is System.mac_os:  # pragma: os-ne-macos
            assert all(text == text.lower() for text in result)
        elif SYSTEM is System.linux:  # pragma: os-ne-linux
            assert result == text
        else:  # pragma: no cover
            never(SYSTEM)


class TestSystem:
    @beartype
    def test_function(self) -> None:
        assert isinstance(_get_system(), System)

    @beartype
    def test_constant(self) -> None:
        assert isinstance(SYSTEM, System)
