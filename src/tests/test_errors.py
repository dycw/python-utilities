from __future__ import annotations

from pytest import raises

from utilities.errors import ImpossibleCaseError, repr_error


class TestImpossibleCaseError:
    def test_main(self) -> None:
        x = None
        with raises(ImpossibleCaseError, match=r"Case must be possible: x=None\."):
            raise ImpossibleCaseError(case=[f"{x=}"])


class TestReprError:
    def test_class(self) -> None:
        class CustomError(Exception): ...

        result = repr_error(CustomError)
        expected = "CustomError"
        assert result == expected

    def test_instance(self) -> None:
        class CustomError(Exception): ...

        result = repr_error(CustomError())
        expected = "CustomError()"
        assert result == expected
