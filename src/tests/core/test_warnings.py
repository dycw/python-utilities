from __future__ import annotations

from warnings import warn

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from
from pytest import raises, warns

from utilities.core import suppress_warnings, yield_warnings_as_errors


class TestSuppressWarnings:
    def test_main(self) -> None:
        with suppress_warnings():
            warn("", stacklevel=2)

    def test_unbound_variables(self) -> None:
        with suppress_warnings():
            x = None
        assert x is None

    def test_one_warning(self) -> None:
        class CustomWarning(UserWarning): ...

        with warns(CustomWarning):
            warn("", category=CustomWarning, stacklevel=2)
        with suppress_warnings(category=CustomWarning):
            warn("", category=CustomWarning, stacklevel=2)

    @given(data=data())
    def test_multiple_warnings(self, data: DataObject) -> None:
        class FirstWarning(UserWarning): ...

        class SecondWarning(UserWarning): ...

        category = data.draw(sampled_from([FirstWarning, SecondWarning]))
        with warns(category):
            warn("", category=category, stacklevel=2)
        with suppress_warnings(category=(FirstWarning, SecondWarning)):
            warn("", category=category, stacklevel=2)


class TestYieldWarningsAsErrors:
    def test_main(self) -> None:
        with raises(UserWarning), yield_warnings_as_errors():
            warn("", stacklevel=2)

    def test_unbound_variables(self) -> None:
        with yield_warnings_as_errors():
            x = None
        assert x is None

    def test_one_warning(self) -> None:
        class CustomWarning(UserWarning): ...

        with warns(CustomWarning):
            warn("", category=CustomWarning, stacklevel=2)
        with raises(CustomWarning), yield_warnings_as_errors(category=CustomWarning):
            warn("", category=CustomWarning, stacklevel=2)

    @given(data=data())
    def test_multiple_warnings(self, data: DataObject) -> None:
        class FirstWarning(UserWarning): ...

        class SecondWarning(UserWarning): ...

        category = data.draw(sampled_from([FirstWarning, SecondWarning]))
        with warns(category):
            warn("", category=category, stacklevel=2)
        with (
            raises(category),
            yield_warnings_as_errors(category=(FirstWarning, SecondWarning)),
        ):
            warn("", category=category, stacklevel=2)
