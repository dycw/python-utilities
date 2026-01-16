from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Any, assert_never, override

if TYPE_CHECKING:
    from utilities.types import PathLike, StrDict, StrMapping


def substitute(
    path_or_text: PathLike,
    /,
    *,
    environ: bool = False,
    mapping: StrMapping | None = None,
    safe: bool = False,
    **kwargs: Any,
) -> str:
    """Substitute from a Path or string."""
    match path_or_text:
        case Path() as path:
            return substitute(
                path.read_text(), environ=environ, mapping=mapping, safe=safe, **kwargs
            )
        case str() as text:
            template = Template(text)
            mapping_use: StrMapping = {} if mapping is None else mapping
            kwargs_use: StrDict = (os.environ if environ else {}) | kwargs
            if safe:
                return template.safe_substitute(mapping_use, **kwargs_use)
            try:
                return template.substitute(mapping_use, **kwargs_use)
            except KeyError as error:
                raise SubstituteError(key=error.args[0]) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class SubstituteError(Exception):
    key: str

    @override
    def __str__(self) -> str:
        return f"Missing key: {self.key!r}"


__all__ = ["SubstituteError", "substitute"]
