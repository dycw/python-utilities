from __future__ import annotations

from typing import Any


def check_ipython_class(cls: type[Any], /) -> bool:
    """Check if the `get_ipython` class is a subclass of `cls`."""
    try:
        func = get_ipython  # type: ignore[]
    except NameError:
        return False
    return issubclass(func().__class__, cls)  # pragma: no cover


def is_ipython() -> bool:
    """Check if `ipython` is running."""
    try:
        from IPython.terminal.interactiveshell import (  # type: ignore[]
            TerminalInteractiveShell,
        )
    except ImportError:
        return False
    return check_ipython_class(TerminalInteractiveShell)  # pragma: no cover


__all__ = ["check_ipython_class", "is_ipython"]
