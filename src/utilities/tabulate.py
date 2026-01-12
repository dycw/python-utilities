from __future__ import annotations

from textwrap import indent
from typing import TYPE_CHECKING, Any

from tabulate import tabulate

from utilities.functions import get_func_name
from utilities.text import split_f_str_equals, strip_and_dedent

if TYPE_CHECKING:
    from collections.abc import Callable


def func_and_params_str(
    func: Callable[..., Any], version: str, /, *variables: str
) -> str:
    """Generate a string describing a function call."""
    name = get_func_name(func)
    table = tabulate(
        list(map(split_f_str_equals, variables)), tablefmt="rounded_outline"
    )
    indented = indent(table, "  ")
    return strip_and_dedent(f"""
        Running {name!r} (version {version}) with:
        {indented}
    """)


def params_table(*variables: str) -> str:
    """Generate a table of parameter names and values."""
    return tabulate(
        list(map(split_f_str_equals, variables)), tablefmt="rounded_outline"
    )


__all__ = ["func_and_params_str", "params_table"]
