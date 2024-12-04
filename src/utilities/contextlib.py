from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType


class NoOpContextManager:
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


__all__ = ["NoOpContextManager"]
