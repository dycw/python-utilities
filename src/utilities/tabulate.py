from __future__ import annotations

from textwrap import indent
from typing import TYPE_CHECKING, Any

from tabulate import tabulate

from utilities.core import get_func_name
from utilities.text import split_f_str_equals

if TYPE_CHECKING:
    from collections.abc import Callable


def func_param_desc(func: Callable[..., Any], version: str, /, *variables: str) -> str:
    """Generate a string describing a function & its parameters."""
    name = get_func_name(func)
    table = indent(params_table(*variables), "  ")
    return f"""\
Running {name!r} (version {version}) with:
{table}"""


def params_table(*variables: str) -> str:
    """Generate a table of parameter names and values."""
    return tabulate(
        list(map(split_f_str_equals, variables)), tablefmt="rounded_outline"
    )


__all__ = ["func_param_desc", "params_table"]
