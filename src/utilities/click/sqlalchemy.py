from __future__ import annotations

from typing import Any

import sqlalchemy
from click import Context, Parameter, ParamType

from utilities.sqlalchemy import ParseEngineError, ensure_engine


class Engine(ParamType):
    """An engine-valued parameter."""

    name = "engine"

    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> sqlalchemy.Engine:
        """Convert a value into the `Engine` type."""
        try:
            return ensure_engine(value)
        except ParseEngineError:
            self.fail(f"Unable to parse {value}", param, ctx)
