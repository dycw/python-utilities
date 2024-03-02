from __future__ import annotations

import pytest

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
        with pytest.raises(
            ImpossibleCaseError, match=r"Case must be possible: x=None\."
        ):
            raise ImpossibleCaseError(case=[f"{x=}"])


class TestRedirectError:
    def test_redirect(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        with pytest.raises(SecondError), redirect_error(FirstError, SecondError):
            raise FirstError

    def test_no_redirect(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        class ThirdError(Exception): ...

        with pytest.raises(FirstError), redirect_error(SecondError, ThirdError):
            raise FirstError

    def test_match_and_redirect(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        with (
            pytest.raises(SecondError),
            redirect_error(FirstError, SecondError, match="text"),
        ):
            msg = "text"
            raise FirstError(msg)

    def test_match_and_args_empty_error(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        with (
            pytest.raises(RedirectErrorError),
            redirect_error(FirstError, SecondError, match="match"),
        ):
            raise FirstError()  # noqa: RSE102

    def test_match_and_args_non_unique_error(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        with (
            pytest.raises(RedirectErrorError),
            redirect_error(FirstError, SecondError, match="match"),
        ):
            args = "x", "y"
            raise FirstError(args)

    def test_match_and_arg_not_string_error(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        with (
            pytest.raises(RedirectErrorError),
            redirect_error(FirstError, SecondError, match="match"),
        ):
            arg = 0
            raise FirstError(arg)

    def test_match_and_no_redirect(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        with (
            pytest.raises(FirstError),
            redirect_error(FirstError, SecondError, match="something else"),
        ):
            msg = "text"
            raise FirstError(msg)


class TestRetry:
    @pytest.mark.parametrize(
        "use_predicate", [pytest.param(None), pytest.param(True), pytest.param(False)]
    )
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
        with pytest.raises(TooLargeError):
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
            with pytest.raises(TooLargeError):
                _ = retry_inc()
