from typing import NoReturn

from pytest import raises

from utilities.errors import NoUniqueArgError, redirect_error


class TestRedirectError:
    def test_generic_redirected_to_custom(self) -> None:
        with raises(self._CustomError):
            self._raises_custom("generic error")

    def test_generic_not_redirected_to_custom(self) -> None:
        with raises(ValueError, match="generic error"):
            self._raises_custom("something else")

    def _raises_custom(self, pattern: str, /) -> NoReturn:
        def raise_error() -> NoReturn:
            msg = "generic error"
            raise ValueError(msg)

        try:
            raise_error()
        except ValueError as error:
            redirect_error(error, pattern, self._CustomError)

    class _CustomError(ValueError):
        ...

    def test_generic_with_no_unique_arg(self) -> None:
        def raise_error() -> NoReturn:
            raise ValueError(0, 1)

        with raises(NoUniqueArgError):
            try:
                raise_error()
            except ValueError as error:
                redirect_error(error, "error", RuntimeError)
