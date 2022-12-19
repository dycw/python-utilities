from re import search
from typing import NoReturn
from typing import Union

from beartype import beartype

from utilities.text import ensure_str


@beartype
def redirect_error(
    old: Exception, pattern: str, new: Union[Exception, type[Exception]], /
) -> NoReturn:
    """Redirect an error if the error contains a string, and if the string
    matches the required pattern.
    """

    args = old.args
    try:
        (msg,) = args
    except ValueError:
        raise NoUniqueArg(args) from None
    else:
        if search(pattern, ensure_str(msg)):
            raise new from None
        else:
            raise old


class NoUniqueArg(ValueError):
    ...
