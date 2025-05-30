from __future__ import annotations

import re
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType


class NoOpContextManager:
    """Context-manager for no-op."""

    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        _ = (exc_type, exc_val, traceback)
        return False


##


_SUPER_OBJECT_HAS_NO_ATTRIBUTE = re.compile(r"'super' object has no attribute '\w+'")


@contextmanager
def suppress_super_object_attribute_error() -> Iterator[None]:
    """Suppress the super() attribute error, for mix-ins."""
    try:
        yield
    except AttributeError as error:
        if not _SUPER_OBJECT_HAS_NO_ATTRIBUTE.search(error.args[0]):
            raise


__all__ = ["NoOpContextManager", "suppress_super_object_attribute_error"]
