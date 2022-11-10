from pytest import raises

from utilities.typing import Never
from utilities.typing import never


class TestNever:
    def test_main(self) -> None:
        with raises(Never):
            never(None)  # type: ignore
