from re import sub

from beartype import beartype

from utilities.platform import SYSTEM, System
from utilities.typing import never


@beartype
def maybe_sub_pct_y(text: str, /) -> str:
    """Substitute the `%Y' token with '%4Y' if necessary."""
    if SYSTEM is System.windows:  # pragma: os-ne-windows
        return text
    if SYSTEM is System.mac_os:  # pragma: os-ne-macos
        return text
    if SYSTEM is System.linux:  # pragma: os-ne-linux
        return sub("%Y", "%4Y", text)
    return never(SYSTEM)  # pragma: no cover
