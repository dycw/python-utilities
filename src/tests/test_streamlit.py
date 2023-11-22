from __future__ import annotations

from pytest import mark, param
from streamlit.testing.v1 import AppTest

from utilities.os import temp_environ
from utilities.streamlit import _PASSWORD_CORRECT


class TestEnsureLoggedIn:
    @mark.parametrize("skip", [param(True), param(False)])
    @mark.parametrize("before_form", [param(True), param(False)])
    @mark.parametrize("after_form", [param(True), param(False)])
    def test_setup(self, *, skip: bool, before_form: bool, after_form: bool) -> None:
        def func() -> None:
            from os import getenv

            from streamlit import write

            from utilities.streamlit import ensure_logged_in

            skip = getenv("SKIP") == "1"
            if getenv("BEFORE_FORM") == "1":

                def _before_form() -> None:
                    _ = write("Before form")

                before_form = _before_form
            else:
                before_form = None
            if getenv("AFTER_FORM") == "1":

                def _after_form() -> None:
                    _ = write("after form")

                after_form = _after_form
            else:
                after_form = None
            ensure_logged_in(skip=skip, before_form=before_form, after_form=after_form)

        with temp_environ(
            SKIP="1" if skip else "0",
            BEFORE_FORM="1" if before_form else "0",
            AFTER_FORM="1" if after_form else "0",
        ):
            at = AppTest.from_function(func)
            _ = at.run()
            assert not at.exception

    @mark.parametrize("is_logged_in", [param(True), param(False)])
    def test_after_logged_in(self, *, is_logged_in: bool) -> None:
        def func() -> None:
            from utilities.streamlit import ensure_logged_in

            ensure_logged_in()

        at = AppTest.from_function(func)
        at.session_state[_PASSWORD_CORRECT] = is_logged_in
        _ = at.run()
        assert not at.exception


class TestStop:
    def test_main(self) -> None:
        def func() -> None:
            from utilities.streamlit import stop

            stop()

        at = AppTest.from_function(func)
        _ = at.run()
        assert not at.exception
