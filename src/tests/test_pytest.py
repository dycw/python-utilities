from os import environ

from utilities.pytest import is_pytest


class TestIsPytest:
    def test_main(self) -> None:
        assert is_pytest()

    def test_disable(self) -> None:
        del environ["PYTEST_CURRENT_TEST"]
        assert not is_pytest()
