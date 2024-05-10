from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from rich.console import Console

if TYPE_CHECKING:
    from collections.abc import Callable


def get_printed_exception(
    *,
    color_system: Literal["auto", "standard", "256", "truecolor", "windows"]
    | None = "auto",
    width: int = 100,
    before: Callable[[Console], None] | None = None,
    extra_lines: int = 3,
    show_locals: bool = False,
    max_frames: int = 100,
    after: Callable[[Console], None] | None = None,
) -> str:
    """Get the printed exception as per `rich`."""
    console = Console(color_system=color_system, width=width)
    with console.capture() as capture:
        if before is not None:
            before(console)
        console.print_exception(
            width=width,
            extra_lines=extra_lines,
            show_locals=show_locals,
            max_frames=max_frames,
        )
        if after is not None:
            after(console)
    return capture.get()


__all__ = ["get_printed_exception"]
