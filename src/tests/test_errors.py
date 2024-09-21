from __future__ import annotations

from pytest import raises

from utilities.errors import ImpossibleCaseError, RedirectErrorError, redirect_error


class TestImpossibleCaseError:
    def test_main(self) -> None:
        x = None
        with raises(ImpossibleCaseError, match=r"Case must be possible: x=None\."):
            raise ImpossibleCaseError(case=[f"{x=}"])


class TestRedirectError:
    def test_redirect(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        with raises(SecondError), redirect_error(FirstError, SecondError):
            raise FirstError

    def test_no_redirect(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        class ThirdError(Exception): ...

        with raises(FirstError, match=""), redirect_error(SecondError, ThirdError):
            raise FirstError

    def test_match_and_redirect(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        def run_test() -> None:
            new = SecondError("second")
            with redirect_error(FirstError, new, match="first"):
                msg = "first"
                raise FirstError(msg)

        with raises(SecondError, match="second"):
            run_test()

    def test_match_and_args_empty_error(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        def run_test() -> None:
            with redirect_error(FirstError, SecondError, match="match"):
                raise FirstError

        with raises(
            RedirectErrorError, match=r"Error must contain a unique argument; got .*\."
        ):
            run_test()

    def test_match_and_args_non_unique_error(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        def run_test() -> None:
            with redirect_error(FirstError, SecondError, match="match"):
                raise FirstError(1, 2)

        with raises(
            RedirectErrorError, match=r"Error must contain a unique argument; got .*\."
        ):
            run_test()

    def test_match_and_arg_not_string_error(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        def run_test() -> None:
            with redirect_error(FirstError, SecondError, match="match"):
                raise FirstError(None)

        with raises(
            RedirectErrorError, match=r"Error argument must be a string; got None\."
        ):
            run_test()

    def test_match_and_no_redirect(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        def run_test() -> None:
            with redirect_error(FirstError, SecondError, match="something else"):
                msg = "initial"
                raise FirstError(msg)

        with raises(FirstError, match="initial"):
            run_test()
