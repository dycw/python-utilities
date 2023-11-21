from __future__ import annotations

from typing import NoReturn

from pytest import mark, param, raises

from utilities.errors import NoUniqueArgError, redirect_error, retry
from utilities.itertools import one


class TestRedirectError:
    class _CustomError(Exception):
        ...

    def test_generic_redirected_to_custom(self) -> None:
        with raises(self._CustomError):
            self._raises_custom("generic error")

    def test_generic_not_redirected_to_custom(self) -> None:
        with raises(ValueError, match="generic error"):
            self._raises_custom("something else")

    def _raises_custom(self, pattern: str, /) -> NoReturn:
        try:
            msg = "generic error"
            raise ValueError(msg)  # noqa: TRY301
        except ValueError as error:
            redirect_error(error, pattern, self._CustomError)

    def test_generic_with_no_unique_arg(self) -> None:
        with raises(NoUniqueArgError):
            try:
                raise ValueError(0, 1)  # noqa: TRY301
            except ValueError as error:
                redirect_error(error, "error", RuntimeError)


class TestRetry:
    @mark.parametrize("use_predicate", [param(None), param(True), param(False)])
    def test_main(self, *, use_predicate: bool | None) -> None:
        class TooLargeError(Exception):
            ...

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
