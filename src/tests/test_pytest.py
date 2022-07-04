from tempenv import TemporaryEnvironment

from utilities.pytest import is_pytest


class TestIsPytest:
    def test_main(self) -> None:
        assert is_pytest()

    def test_disable(self) -> None:
        with TemporaryEnvironment({"PYTEST_CURRENT_TEST": None}):
            assert not is_pytest()
