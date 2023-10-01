from __future__ import annotations

from bidict import ValueDuplicationError
from bidict import bidict

from utilities.iterables import check_duplicates
from utilities.text import snake_case
from utilities.typing import IterableStrs


def snake_case_mappings(text: IterableStrs, /) -> bidict[str, str]:
    """Map a set of text into their snake cases."""
    as_list = list(text)
    check_duplicates(as_list)
    try:
        return bidict({t: snake_case(t) for t in as_list})
    except ValueDuplicationError:
        msg = f"{text=}"
        raise SnakeCaseContainsDuplicatesError(msg) from None


class SnakeCaseContainsDuplicatesError(ValueError):
    """Raised when the snake case values contain duplicates."""
