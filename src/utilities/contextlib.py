from __future__ import annotations

import re
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


_SUPER_OBJECT_HAS_NO_ATTRIBUTE = re.compile(r"'super' object has no attribute '\w+'")


@contextmanager
def suppress_super_object_attribute_error() -> Iterator[None]:
    """Suppress the super() attribute error, for mix-ins."""
    try:
        yield
    except AttributeError as error:
        if not _SUPER_OBJECT_HAS_NO_ATTRIBUTE.search(error.args[0]):
            raise


__all__ = ["suppress_super_object_attribute_error"]
