from __future__ import annotations

from warnings import warn

import pytest
from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from

from utilities.warnings import catch_warnings_as_errors, suppress_warnings


class TestCatchWarningsAsErrors:
    def test_main(self) -> None:
        with pytest.raises(UserWarning), catch_warnings_as_errors():
            warn("", stacklevel=2)

    def test_unbound_variables(self) -> None:
        with catch_warnings_as_errors():
            x = None
        assert x is None

    def test_one_warning(self) -> None:
        class CustomWarning(UserWarning): ...

        with pytest.warns(CustomWarning):
            warn("", category=CustomWarning, stacklevel=2)
        with (
            pytest.raises(CustomWarning),
            catch_warnings_as_errors(category=CustomWarning),
        ):
            warn("", category=CustomWarning, stacklevel=2)

    @given(data=data())
    def test_multiple_warnings(self, data: DataObject) -> None:
        class FirstWarning(UserWarning): ...

        class SecondWarning(UserWarning): ...

        category = data.draw(sampled_from([FirstWarning, SecondWarning]))
        with pytest.warns(category):
            warn("", category=category, stacklevel=2)
        with (
            pytest.raises(category),
            catch_warnings_as_errors(category=(FirstWarning, SecondWarning)),
        ):
            warn("", category=category, stacklevel=2)


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

        with pytest.warns(CustomWarning):
            warn("", category=CustomWarning, stacklevel=2)
        with suppress_warnings(category=CustomWarning):
            warn("", category=CustomWarning, stacklevel=2)

    @given(data=data())
    def test_multiple_warnings(self, data: DataObject) -> None:
        class FirstWarning(UserWarning): ...

        class SecondWarning(UserWarning): ...

        category = data.draw(sampled_from([FirstWarning, SecondWarning]))
        with pytest.warns(category):
            warn("", category=category, stacklevel=2)
        with suppress_warnings(category=(FirstWarning, SecondWarning)):
            warn("", category=category, stacklevel=2)
