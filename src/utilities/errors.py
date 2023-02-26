from re import search
from typing import NoReturn
from typing import Union

from beartype import beartype

from utilities.text import ensure_str


class DirectoryExistsError(Exception):
    """Raised when a directory already exists."""


@beartype
def redirect_error(
    old: Exception,
    pattern: str,
    new: Union[Exception, type[Exception]],
    /,
) -> NoReturn:
    """Redirect an error if a matching string is found."""
    args = old.args
    try:
        (msg,) = args
    except ValueError:
        raise NoUniqueArgError(args) from None
    else:
        if search(pattern, ensure_str(msg)):
            raise new from None
        raise old


class NoUniqueArgError(ValueError):
    """Raised when no unique argument can be found."""
