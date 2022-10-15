from typing import NoReturn

from pytest import raises

from utilities.errors import NoUniqueArg
from utilities.errors import redirect_error


class TestRedirectError:
    def test_redirected(self) -> None:
        def raises_generic() -> NoReturn:
            raise ValueError("error")

        class CustomError(ValueError):
            ...

        with raises(CustomError):
            try:
                raises_generic()
            except ValueError as error:
                redirect_error(error, "error", CustomError)

    def test_not_redirected(self) -> None:
        def raises_generic() -> NoReturn:
            raise ValueError("error")

        class CustomError(ValueError):
            ...

        with raises(ValueError):
            try:
                raises_generic()
            except ValueError as error:
                redirect_error(error, "something else", CustomError)

    def test_no_unique_arg(self) -> None:
        def raises_generic() -> NoReturn:
            raise ValueError(0, 1)

        with raises(NoUniqueArg):
            try:
                raises_generic()
            except ValueError as error:
                redirect_error(error, "error", RuntimeError)
