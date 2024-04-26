from __future__ import annotations

from pytest import mark, param, raises

from utilities.errors import (
    ImpossibleCaseError,
    RedirectErrorError,
    redirect_error,
    retry,
)
from utilities.iterables import one


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


class TestRetry:
    @mark.parametrize("use_predicate", [param(None), param(True), param(False)])
    def test_main(self, *, use_predicate: bool | None) -> None:
        class TooLargeError(Exception): ...

        def increment() -> int:
            nonlocal n
            n += 1
            if n >= 3:
                raise TooLargeError(n)
            return n

        n = 0
        assert increment() == 1
        assert increment() == 2
        with raises(TooLargeError):
            _ = increment()

        def reset(_error: TooLargeError, /) -> None:
            nonlocal n
            n = 0

        if use_predicate is None:
            retry_inc = retry(increment, TooLargeError, reset)
        else:

            def predicate(error: TooLargeError, /) -> bool:
                if use_predicate:
                    return one(error.args) >= 3
                return one(error.args) >= 4

            retry_inc = retry(increment, TooLargeError, reset, predicate=predicate)

        n = 0
        assert retry_inc() == 1
        assert retry_inc() == 2
        if (use_predicate is None) or (use_predicate is True):
            assert retry_inc() == 1
        else:
            with raises(TooLargeError):
                _ = retry_inc()
