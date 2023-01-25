from pytest import raises

from utilities.typing import NeverError, never


class TestNever:
    def test_main(self) -> None:
        with raises(NeverError):
            never(None)  # type: ignore[]
