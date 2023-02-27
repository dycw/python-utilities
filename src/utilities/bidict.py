from beartype import beartype
from bidict import ValueDuplicationError
from bidict import bidict

from utilities.beartype import IterableStrs
from utilities.iterables import check_duplicates
from utilities.text import snake_case


@beartype
def snake_case_mappings(text: IterableStrs, /) -> bidict[str, str]:
    """Map a set of text into their snake cases."""
    text = list(text)
    check_duplicates(text)
    try:
        return bidict({t: snake_case(t) for t in text})
    except ValueDuplicationError:
        msg = f"{text=}"
        raise SnakeCaseContainsDuplicatesError(msg) from None


class SnakeCaseContainsDuplicatesError(ValueError):
    """Raised when the snake case values contain duplicates."""
