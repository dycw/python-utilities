from warnings import warn

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from hypothesis.strategies import sampled_from
from pytest import raises
from pytest import warns

from utilities.warnings import catch_warnings_as_errors
from utilities.warnings import suppress_warnings


class TestCatchWarningsAsErrors:
    def test_main(self) -> None:
        with raises(UserWarning), catch_warnings_as_errors():
            warn("")

    def test_unbound_variables(self) -> None:
        with catch_warnings_as_errors():
            x = None
        assert x is None

    def test_one_warning(self) -> None:
        class CustomWarning(UserWarning):
            ...

        with warns(CustomWarning):
            warn("", category=CustomWarning)
        with raises(CustomWarning), catch_warnings_as_errors(
            category=CustomWarning,
        ):
            warn("", category=CustomWarning)

    @given(data=data())
    def test_multiple_warnings(self, data: DataObject) -> None:
        class FirstWarning(UserWarning):
            ...

        class SecondWarning(UserWarning):
            ...

        category = data.draw(sampled_from([FirstWarning, SecondWarning]))
        with warns(category):
            warn("", category=category)
        with raises(category), catch_warnings_as_errors(
            category=(FirstWarning, SecondWarning),
        ):
            warn("", category=category)


class TestSuppressWarnings:
    def test_main(self) -> None:
        with suppress_warnings():
            warn("")

    def test_unbound_variables(self) -> None:
        with suppress_warnings():
            x = None
        assert x is None

    def test_one_warning(self) -> None:
        class CustomWarning(UserWarning):
            ...

        with warns(CustomWarning):
            warn("", category=CustomWarning)
        with suppress_warnings(category=CustomWarning):
            warn("", category=CustomWarning)

    @given(data=data())
    def test_multiple_warnings(self, data: DataObject) -> None:
        class FirstWarning(UserWarning):
            ...

        class SecondWarning(UserWarning):
            ...

        category = data.draw(sampled_from([FirstWarning, SecondWarning]))
        with warns(category):
            warn("", category=category)
        with suppress_warnings(category=(FirstWarning, SecondWarning)):
            warn("", category=category)
