from typing import Any, Optional

import sqlalchemy
from beartype import beartype
from click import Context, Parameter, ParamType

from utilities.sqlalchemy import ParseEngineError, ensure_engine


class Engine(ParamType):
    """An engine-valued parameter."""

    name = "engine"

    @beartype
    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> sqlalchemy.Engine:
        """Convert a value into the `Engine` type."""
        try:
            return ensure_engine(value)
        except ParseEngineError:
            self.fail(f"Unable to parse {value}", param, ctx)
